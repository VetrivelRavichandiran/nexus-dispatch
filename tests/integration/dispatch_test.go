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

	"github.com/nexus-dispatch/nexus-dispatch/internal/dispatch"
	"github.com/nexus-dispatch/nexus-dispatch/internal/events"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
	"github.com/nexus-dispatch/nexus-dispatch/internal/reservation"
)

func newEnv(t *testing.T) (*dispatch.Engine, *redis.Store) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	store := redis.NewStore(rdb)
	resv := reservation.NewManager(store, 30*time.Second, time.Hour, nil)
	return dispatch.NewEngine(store, resv, nil, dispatch.DefaultConfig(), nil), store
}

func seedDrivers(t *testing.T, store *redis.Store, n int, baseLat, baseLng float64) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("drv-%d", i)
		lat := baseLat + 0.001*float64(i%10)
		lng := baseLng + 0.001*float64(i%8)
		if err := store.InitDriver(ctx, id, "D", "AVAILABLE", 4.5, lat, lng); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDispatchAssignsNearestDriver verifies the full pipeline picks a nearby
// available driver and holds its lease.
func TestDispatchAssignsNearestDriver(t *testing.T) {
	eng, store := newEnv(t)
	ctx := context.Background()
	seedDrivers(t, store, 30, 12.9716, 77.5946)

	// Put one driver very close to the pickup.
	closeID := "drv-close"
	if err := store.InitDriver(ctx, closeID, "D", "AVAILABLE", 5.0, 12.9717, 77.5947); err != nil {
		t.Fatal(err)
	}

	ev := &events.RideRequested{
		Envelope:  events.Envelope{EventID: "e1", Type: "ride.requested", Timestamp: time.Now()},
		RideID:    "ride-1",
		UserID:    "user-1",
		PickupLat: 12.9716,
		PickupLng: 77.5946,
		RadiusM:   1000,
	}
	res, err := eng.DispatchForRide(ctx, ev)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success, got %s", res.ResultCode)
	}
	if res.DriverID != closeID {
		t.Errorf("expected nearest driver %s, got %s", closeID, res.DriverID)
	}
	// Driver must be RESERVED now.
	if state, _ := store.GetState(ctx, closeID); state != "RESERVED" {
		t.Errorf("driver state = %q, want RESERVED", state)
	}
	// Release it.
	if err := eng.ReleaseDriver(ctx, res.ReserveResult); err != nil {
		t.Fatal(err)
	}
}

// TestDispatchNoCandidates verifies empty-area behavior.
func TestDispatchNoCandidates(t *testing.T) {
	eng, _ := newEnv(t)
	ctx := context.Background()
	ev := &events.RideRequested{
		Envelope:  events.Envelope{EventID: "e2", Type: "ride.requested", Timestamp: time.Now()},
		RideID:    "ride-2",
		UserID:    "user-1",
		PickupLat: -33.8688, // Sydney — no drivers seeded here
		PickupLng: 151.2093,
		RadiusM:   1000,
	}
	res, err := eng.DispatchForRide(ctx, ev)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if res.Success || res.ResultCode != "no_candidates" {
		t.Errorf("expected no_candidates, got success=%v code=%s", res.Success, res.ResultCode)
	}
}

// TestDispatchConcurrentRidesSameDriver is the end-to-end invariant: many
// concurrent ride requests in the same area, limited drivers. Each driver is
// reserved by AT MOST one ride at a time.
func TestDispatchConcurrentRidesSameDriver(t *testing.T) {
	eng, store := newEnv(t)
	ctx := context.Background()
	// Only 5 drivers in the area.
	seedDrivers(t, store, 5, 12.9716, 77.5946)

	const nRides = 50
	var wg sync.WaitGroup
	var successes atomic.Int64
	var mu sync.Mutex
	assigned := map[string]string{} // driver -> ride
	var conflicts atomic.Int64

	for i := 0; i < nRides; i++ {
		wg.Add(1)
		rideID := fmt.Sprintf("ride-%d", i)
		go func() {
			defer wg.Done()
			ev := &events.RideRequested{
				Envelope:  events.Envelope{EventID: rideID, Type: "ride.requested", Timestamp: time.Now()},
				RideID:    rideID,
				UserID:    "user-1",
				PickupLat: 12.9716,
				PickupLng: 77.5946,
				RadiusM:   1000,
			}
			res, err := eng.DispatchForRide(ctx, ev)
			if err != nil {
				conflicts.Add(1)
				return
			}
			if res.Success {
				successes.Add(1)
				mu.Lock()
				if prev, ok := assigned[res.DriverID]; ok {
					t.Errorf("driver %s assigned to both %s and %s — DOUBLE BOOKING", res.DriverID, prev, rideID)
				}
				assigned[res.DriverID] = rideID
				mu.Unlock()
				// Keep the lease held (do not release) so the invariant is
				// "at most one active ride per driver".
				return
			}
			conflicts.Add(1)
		}()
	}
	wg.Wait()

	// At most 5 rides can succeed (only 5 drivers).
	if successes.Load() > 5 {
		t.Fatalf("expected <= 5 successes (5 drivers), got %d", successes.Load())
	}
	if successes.Load() == 0 {
		t.Fatal("expected some successes")
	}
	// Invariant: no driver assigned to two rides.
	for d, r := range assigned {
		if state, _ := store.GetState(ctx, d); state != "RESERVED" {
			t.Errorf("driver %s (ride %s) state = %q, want RESERVED", d, r, state)
		}
	}
}

// TestDispatchStaleDriverNotAssigned verifies a driver that went OFFLINE is
// not assigned.
func TestDispatchStaleDriverNotAssigned(t *testing.T) {
	eng, store := newEnv(t)
	ctx := context.Background()
	_ = store.InitDriver(ctx, "drv-off", "D", "OFFLINE", 4.5, 12.9717, 77.5947)
	_ = store.InitDriver(ctx, "drv-on", "D", "AVAILABLE", 4.5, 12.9718, 77.5948)

	ev := &events.RideRequested{
		Envelope:  events.Envelope{EventID: "e3", Type: "ride.requested", Timestamp: time.Now()},
		RideID:    "ride-3",
		UserID:    "user-1",
		PickupLat: 12.9716,
		PickupLng: 77.5946,
		RadiusM:   1000,
	}
	res, err := eng.DispatchForRide(ctx, ev)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Success {
		t.Fatalf("expected success via drv-on, got %s", res.ResultCode)
	}
	if res.DriverID == "drv-off" {
		t.Fatal("assigned an OFFLINE driver")
	}
}