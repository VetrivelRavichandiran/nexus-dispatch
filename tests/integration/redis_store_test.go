package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
	"github.com/nexus-dispatch/nexus-dispatch/internal/reservation"
)

// newTestStore spins up an in-memory Redis (miniredis) and a Store.
func newTestStore(t *testing.T) (*redis.Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return redis.NewStore(rdb), mr
}

// TestConcurrentReservationSingleWinner is THE invariant test:
//
//	successful_reservations(driver_id) <= 1
//
// 1000 goroutines race to reserve the same driver for 1000 different rides.
// Exactly one must win.
func TestConcurrentReservationSingleWinner(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	const driver = "driver-1"
	const n = 1000

	if err := store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946); err != nil {
		t.Fatal(err)
	}

	var wins atomic.Int64
	var conflicts atomic.Int64
	var otherErrs atomic.Int64
	var mu sync.Mutex
	var winners []string
	var winnerRes *reservation.Reservation

	mgr := reservation.NewManager(store, 30*time.Second, time.Hour, nil)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		rideID := fmt.Sprintf("ride-%d", i)
		go func() {
			defer wg.Done()
			<-start
			r, err := mgr.Reserve(ctx, driver, rideID)
			if err == nil {
				wins.Add(1)
				mu.Lock()
				winners = append(winners, rideID)
				winnerRes = r // winner HOLDS the lease until the test ends
				mu.Unlock()
				return
			}
			switch err {
			case reservation.ErrConflict:
				conflicts.Add(1)
			default:
				otherErrs.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	// Invariant: while the winner holds the lease, at most one ride was ever
	// able to reserve the driver. (Releasing the lease and re-reserving is
	// covered by TestReservationReleaseReacquire.)
	if wins.Load() != 1 {
		t.Fatalf("expected exactly 1 winner, got %d (winners=%v)", wins.Load(), winners)
	}
	if conflicts.Load() != n-1 {
		t.Errorf("expected %d conflicts, got %d", n-1, conflicts.Load())
	}
	if otherErrs.Load() != 0 {
		t.Errorf("expected 0 other errors, got %d", otherErrs.Load())
	}
	// Driver must be RESERVED (held by the winner) right now.
	if state, _ := store.GetState(ctx, driver); state != "RESERVED" {
		t.Errorf("driver state while held = %q, want RESERVED", state)
	}
	// Release; driver must return to AVAILABLE.
	if err := winnerRes.Release(ctx, mgr); err != nil {
		t.Errorf("release: %v", err)
	}
	if state, _ := store.GetState(ctx, driver); state != "AVAILABLE" {
		t.Errorf("driver state after release = %q, want AVAILABLE", state)
	}
}

// TestReservationReleaseReacquire verifies the full cycle: reserve → release →
// re-reserve by a different ride.
func TestReservationReleaseReacquire(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	const driver = "driver-2"
	_ = store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946)
	mgr := reservation.NewManager(store, 30*time.Second, time.Hour, nil)

	r1, err := mgr.Reserve(ctx, driver, "ride-A")
	if err != nil {
		t.Fatal(err)
	}
	// Second ride must conflict while r1 is held.
	if _, err := mgr.Reserve(ctx, driver, "ride-B"); err != reservation.ErrConflict {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
	if err := r1.Release(ctx, mgr); err != nil {
		t.Fatal(err)
	}
	r2, err := mgr.Reserve(ctx, driver, "ride-B")
	if err != nil {
		t.Fatalf("re-reserve after release: %v", err)
	}
	_ = r2.Release(ctx, mgr)
}

// TestStaleReleaseCannotDeleteNewerLease is the crash-recovery scenario:
//
//  1. ride A reserves (version 1), then CRASHES (no release).
//  2. lease TTL expires.
//  3. ride B reserves (version 2).
//  4. ride A's deferred release fires with its OLD lease value.
//  5. B's lease must SURVIVE — compare-and-delete rejects the stale value.
func TestStaleReleaseCannotDeleteNewerLease(t *testing.T) {
	store, mr := newTestStore(t)
	ctx := context.Background()
	const driver = "driver-3"
	_ = store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946)
	// Short TTL so the test is fast.
	mgr := reservation.NewManager(store, 2*time.Second, time.Hour, nil)

	// A reserves, then "crashes" — cancel the renewal but do NOT release.
	ra, err := mgr.Reserve(ctx, driver, "ride-A")
	if err != nil {
		t.Fatal(err)
	}
	staleLease := ra.Lease
	// Simulate crash: stop renewal without releasing. Fast-forward
	// miniredis's clock past the TTL so A's lease expires deterministically
	// (a real Redis would expire it on wall clock).
	mr.FastForward(2 * time.Second)

	// B reserves successfully (A's lease is gone).
	rb, err := mgr.Reserve(ctx, driver, "ride-B")
	if err != nil {
		t.Fatalf("B should reserve after A's lease expired, got %v", err)
	}

	// A's stale release fires now, with its old lease value.
	ok, err := store.Release(ctx, driver, staleLease, "AVAILABLE")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("stale release succeeded — it must NOT delete B's newer lease")
	}
	// B's lease must still be intact.
	lv, err := store.GetLease(ctx, driver)
	if err != nil {
		t.Fatal(err)
	}
	if lv == nil || lv.RideID != "ride-B" {
		t.Fatalf("B's lease was destroyed: got %+v", lv)
	}
	_ = rb.Release(ctx, mgr)
}

// TestStaleReservedStateSelfHeals verifies that a driver left in RESERVED
// state with no lease (its holder crashed and the lease expired) can be
// re-reserved — the reserve script self-heals the stale state.
func TestStaleReservedStateSelfHeals(t *testing.T) {
store, mr := newTestStore(t)
ctx := context.Background()
const driver = "driver-7"
_ = store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946)
mgr := reservation.NewManager(store, 2*time.Second, time.Hour, nil)

ra, err := mgr.Reserve(ctx, driver, "ride-X")
if err != nil {
t.Fatal(err)
}
// Crash: lease expires without release.
mr.FastForward(2 * time.Second)
state, _ := store.GetState(ctx, driver)
if state != "RESERVED" {
t.Fatalf("expected stale RESERVED state, got %q", state)
}
// A new ride must be able to reserve the stale-RESERVED driver.
rb, err := mgr.Reserve(ctx, driver, "ride-Y")
if err != nil {
t.Fatalf("stale-RESERVED driver must be re-reservable, got %v", err)
}
_ = ra.Lease // A's reservation object is dead; ignore.
if err := rb.Release(ctx, mgr); err != nil {
t.Fatal(err)
}
}

// TestLocationSequenceGuard verifies out-of-order and duplicate location
// updates are rejected, and newer ones applied.
func TestLocationSequenceGuard(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	const driver = "driver-4"
	_ = store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946)

	applied, err := store.ApplyLocation(ctx, driver, 12.9720, 77.5950, 5, time.Now().UnixMilli())
	if err != nil || !applied {
		t.Fatalf("seq 5 should apply: applied=%v err=%v", applied, err)
	}
	// Older update must be rejected.
	if applied, _ = store.ApplyLocation(ctx, driver, 12.9700, 77.5900, 3, time.Now().UnixMilli()); applied {
		t.Fatal("seq 3 (older) must be rejected")
	}
	// Duplicate must be rejected.
	if applied, _ = store.ApplyLocation(ctx, driver, 12.9700, 77.5900, 5, time.Now().UnixMilli()); applied {
		t.Fatal("seq 5 (duplicate) must be rejected")
	}
	// Newer update applies.
	if applied, _ = store.ApplyLocation(ctx, driver, 12.9730, 77.5960, 6, time.Now().UnixMilli()); !applied {
		t.Fatal("seq 6 (newer) must apply")
	}
	pos, _ := store.GetLocation(ctx, driver)
	if pos.Lat != 12.9730 || pos.Lng != 77.5960 {
		t.Errorf("position = (%f, %f), want (12.9730, 77.5960)", pos.Lat, pos.Lng)
	}
}

// TestConcurrentLocationSequenceGuard hammers the sequence guard with
// concurrent out-of-order updates; the final applied sequence must be the max.
func TestConcurrentLocationSequenceGuard(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	const driver = "driver-5"
	_ = store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946)

	const n = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			<-start
			_, _ = store.ApplyLocation(ctx, driver, 12.9716+float64(seq)*0.0001, 77.5946, seq, time.Now().UnixMilli())
		}(int64(i))
	}
	close(start)
	wg.Wait()

	pos, _ := store.GetLocation(ctx, driver)
	if pos.Seq != n {
		t.Fatalf("final seq = %d, want %d (max must win)", pos.Seq, n)
	}
}

// TestCellMembershipMigration verifies a driver moving cells is removed from
// the old cell set and added to the new one.
func TestCellMembershipMigration(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	const driver = "driver-6"
	_ = store.InitDriver(ctx, driver, "D", "AVAILABLE", 4.8, 12.9716, 77.5946)
	cell1, _ := store.GetLocation(ctx, driver)

	// Move ~3.5 km north — definitely a different res-7 cell (crosses at ~2 km).
	_, err := store.ApplyLocation(ctx, driver, 12.9716+0.031, 77.5946, 1, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	pos, _ := store.GetLocation(ctx, driver)
	if pos.Cell == cell1.Cell {
		t.Fatal("test setup: expected a different cell after moving 3.5 km")
	}
	rdb := store.RDB()
	// Old cell must not contain the driver (key may be gone entirely).
	old, err := rdb.SMembers(ctx, redis.CellKey(cell1.Cell)).Result()
	if err != nil && err != goredis.Nil {
		t.Fatal(err)
	}
	for _, m := range old {
		if m == driver {
			t.Fatalf("driver still in old cell set %s", cell1.Cell)
		}
	}
	// New cell must contain the driver.
	newSet, err := rdb.SMembers(ctx, redis.CellKey(pos.Cell)).Result()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range newSet {
		if m == driver {
			found = true
		}
	}
	if !found {
		t.Fatalf("driver not in new cell set %s", pos.Cell)
	}
}

// TestSearchNearbyFindsDrivers verifies the H3 search returns nearby
// AVAILABLE drivers within radius and excludes out-of-radius / non-available.
func TestSearchNearbyFindsDrivers(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	baseLat, baseLng := 12.9716, 77.5946

	// 50 available drivers scattered within ~700 m (0.001° ≈ 111 m).
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("d-avail-%d", i)
		lat := baseLat + 0.001*float64(i%7)
		lng := baseLng + 0.001*float64(i%5)
		_ = store.InitDriver(ctx, id, "D", "AVAILABLE", 4.5, lat, lng)
	}
	// 20 drivers ON_TRIP nearby — must be excluded.
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("d-busy-%d", i)
		_ = store.InitDriver(ctx, id, "D", "ON_TRIP", 4.5, baseLat, baseLng)
	}
	// 10 available drivers ~3 km away — must be excluded by radius.
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("d-far-%d", i)
		_ = store.InitDriver(ctx, id, "D", "AVAILABLE", 4.5, baseLat+0.03, baseLng+0.03)
	}

	res, cells, err := store.SearchNearby(ctx, baseLat, baseLng, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if cells == 0 {
		t.Fatal("no cells searched")
	}
	for _, d := range res {
		if d.State != "AVAILABLE" {
			t.Errorf("returned non-available driver %s", d.ID)
		}
		if d.DistM > 1000 {
			t.Errorf("returned out-of-radius driver %s at %.0f m", d.ID, d.DistM)
		}
	}
	// All 50 nearby available drivers must be found (boundary safety).
	if len(res) != 50 {
		t.Errorf("found %d nearby available drivers, want 50", len(res))
	}
	// Sorted by distance ascending.
	for i := 1; i < len(res); i++ {
		if res[i].DistM < res[i-1].DistM {
			t.Fatal("results not sorted by distance")
		}
	}
}

// TestTokenBucket verifies the distributed rate limiter.
func TestTokenBucket(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	// capacity 10, refill 10/s.
	for i := 0; i < 10; i++ {
		ok, err := store.RateAllow(ctx, "user", "u1", 10, 10, 1)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("request %d should be allowed (bucket full)", i+1)
		}
	}
	if ok, _ := store.RateAllow(ctx, "user", "u1", 10, 10, 1); ok {
		t.Fatal("11th request should be rate-limited")
	}
	// Different scope is independent.
	if ok, _ := store.RateAllow(ctx, "ip", "u1", 10, 10, 1); !ok {
		t.Fatal("different scope should have its own bucket")
	}
	// Refill: wait 300 ms → ~3 tokens.
	time.Sleep(300 * time.Millisecond)
	allowed := 0
	for i := 0; i < 5; i++ {
		if ok, _ := store.RateAllow(ctx, "user", "u1", 10, 10, 1); ok {
			allowed++
		}
	}
	if allowed < 2 || allowed > 4 {
		t.Errorf("after 300ms refill expected 2-4 tokens, got %d", allowed)
	}
}