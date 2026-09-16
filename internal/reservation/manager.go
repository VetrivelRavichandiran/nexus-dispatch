// Package reservation implements the no-double-booking primitive:
// an atomic, TTL-bounded, re-entrant driver lease with background renewal
// and compare-and-delete release.
//
// Correctness argument (see docs/decisions/ADR-006):
//   - Acquisition is a single atomic Redis Lua script: two racers serialize,
//     exactly one wins.
//   - The lease value is rideID:token:version. Renewal is re-entrant (only the
//     winner's token can extend). Release is compare-and-delete (a stale
//     holder with an old version cannot delete a newer lease).
//   - TTL bounds the "winner crashed" window: if the holder dies, the lease
//     expires and the driver becomes reservable again.
//   - A durable Postgres unique partial index on active dispatches is the
//     backstop (enforced in the persistence layer).
package reservation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
)

// Manager coordinates driver reservations.
type Manager struct {
	store *redis.Store
	ttl   time.Duration
	renew time.Duration
	log   *slog.Logger
	// version is a process-wide monotonic counter for lease versions.
	version atomic.Uint64
}

// NewManager creates a reservation manager.
func NewManager(store *redis.Store, ttl, renew time.Duration, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{store: store, ttl: ttl, renew: renew, log: log}
}

// Reservation is a held lease on a driver.
type Reservation struct {
	DriverID string
	RideID   string
	Lease    redis.LeaseValue
	cancel   context.CancelFunc
	done     chan struct{}
}

// Release returns the driver to the pool (compare-and-delete). Safe to call
// multiple times; only the first call releases.
func (r *Reservation) Release(ctx context.Context, m *Manager) error {
	if r.cancel != nil {
		r.cancel()
		<-r.done
	}
	ok, err := m.store.Release(ctx, r.DriverID, r.Lease, "AVAILABLE")
	if err != nil {
		return err
	}
	if !ok {
		// Lease already gone (expired or released by a newer holder). Not an
		// error: the driver is already free.
		return nil
	}
	return nil
}

// Reserve atomically reserves driverID for rideID. On success it starts a
// background renewal goroutine that extends the lease every m.renew until
// Release is called or ctx is cancelled.
//
// Returns (nil, ErrConflict) if another ride holds the driver, and
// (nil, ErrNotAvailable) if the driver is not in a reservable state.
func (m *Manager) Reserve(ctx context.Context, driverID, rideID string) (*Reservation, error) {
	token, err := randomToken()
	if err != nil {
		return nil, fmt.Errorf("reservation: token: %w", err)
	}
	lv := redis.LeaseValue{
		RideID:  rideID,
		Token:   token,
		Version: m.version.Add(1),
	}
	res, err := m.store.Reserve(ctx, driverID, rideID, lv, m.ttl)
	if err != nil {
		return nil, fmt.Errorf("reservation: %w", err)
	}
	switch res {
	case redis.Reserved:
		r := &Reservation{DriverID: driverID, RideID: rideID, Lease: lv, done: make(chan struct{})}
		rCtx, cancel := context.WithCancel(ctx)
		r.cancel = cancel
		go m.renewLoop(rCtx, r)
		m.log.Info("driver reserved",
			"driver_id", driverID, "ride_id", rideID, "version", lv.Version)
		return r, nil
	case redis.Conflict:
		return nil, ErrConflict
	default:
		return nil, ErrNotAvailable
	}
}

// renewLoop extends the lease until ctx is done. Renewal is re-entrant: if
// the lease was lost (expired + re-taken by another ride), renewal fails
// silently and the loop exits — the caller's Release will then be a no-op.
func (m *Manager) renewLoop(ctx context.Context, r *Reservation) {
	defer close(r.done)
	t := time.NewTicker(m.renew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Re-entrant renewal: same lease value extends its own TTL.
			if _, err := m.store.Reserve(ctx, r.DriverID, r.RideID, r.Lease, m.ttl); err != nil {
				m.log.Warn("lease renewal failed", "driver_id", r.DriverID, "err", err)
				return
			}
		}
	}
}

func randomToken() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}