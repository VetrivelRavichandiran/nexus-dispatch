// Package dispatch implements the dispatch pipeline:
//
//	ride request → H3 cell → k-ring search → candidate filter →
//	score & rank → atomic reservation → dispatch record → driver notify
//
// The engine is stateless: all coordination is in Redis (leases) and the
// durable record lands in Postgres via the Event Processor. Multiple engine
// instances can run in parallel; the reservation lease guarantees at most one
// wins per driver.
package dispatch

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/nexus-dispatch/nexus-dispatch/internal/events"
	"github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
	"github.com/nexus-dispatch/nexus-dispatch/internal/reservation"
)

// Config tunes the dispatch engine.
type Config struct {
	// DefaultRadiusM is used when a ride request doesn't specify one.
	DefaultRadiusM float64
	// MaxCandidates caps how many ranked candidates we attempt to reserve
	// before giving up on this dispatch attempt.
	MaxCandidates int
	// Score weights. Distance is in meters; requestAge in seconds.
	WDistance   float64
	WRequestAge float64
	WRating     float64
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		DefaultRadiusM: 1000,
		MaxCandidates:  10,
		// Weights: 1 m of distance ≈ 1 unit; 1 s of request age ≈ 5 units
		// (a 20 s-old request "costs" like 100 m); rating 5.0→0 penalty.
		WDistance:   1.0,
		WRequestAge: 5.0,
		WRating:     50.0,
	}
}

// Candidate is a scored dispatch candidate.
type Candidate struct {
	DriverID  string
	Lat       float64
	Lng       float64
	DistanceM float64 // geodesic distance (NOT road distance)
	Score     float64
	Rating    float64
	Cell      string
}

// Result is the outcome of a dispatch attempt.
type Result struct {
	Success         bool
	DriverID        string
	DispatchID      string
	ReserveResult   *reservation.Reservation
	ResultCode      string // "ok" | "no_candidates" | "all_conflicts" | "redis_unavailable"
	CellsSearched   int
	CandidatesTotal int
	TookNs          int64
}

// Engine runs the dispatch pipeline.
type Engine struct {
	store  *redis.Store
	resv   *reservation.Manager
	prod   *kafka.Producer // may be nil (tests)
	cfg    Config
	log    *slog.Logger
	dispatchID func() string // id generator (injectable for tests)
}

// NewEngine creates a dispatch engine.
func NewEngine(store *redis.Store, resv *reservation.Manager, prod *kafka.Producer, cfg Config, log *slog.Logger) *Engine {
	if log == nil {
		log = slog.Default()
	}
	if cfg.MaxCandidates <= 0 {
		cfg = DefaultConfig()
	}
	return &Engine{
		store:      store,
		resv:       resv,
		prod:       prod,
		cfg:        cfg,
		log:        log,
		dispatchID: newID("disp"),
	}
}

// DispatchForRide runs the full pipeline for a ride request event.
func (e *Engine) DispatchForRide(ctx context.Context, ev *events.RideRequested) (*Result, error) {
	start := time.Now()
	radius := float64(ev.RadiusM)
	if radius <= 0 {
		radius = e.cfg.DefaultRadiusM
	}
	ageSec := time.Since(ev.Timestamp).Seconds()
	if ageSec < 0 {
		ageSec = 0
	}

	// 1. H3 k-ring search for AVAILABLE drivers within radius.
	cands, cells, err := e.store.SearchNearby(ctx, ev.PickupLat, ev.PickupLng, radius)
	if err != nil {
		// Redis unavailable: fail-closed. The ride stays REQUESTED and is
		// retried (or expires). We never guess.
		e.log.Error("dispatch: redis unavailable, failing closed",
			"ride_id", ev.RideID, "err", err)
		return &Result{ResultCode: "redis_unavailable", TookNs: time.Since(start).Nanoseconds()}, nil
	}
	if len(cands) == 0 {
		e.log.Info("dispatch: no candidates", "ride_id", ev.RideID, "cells", cells)
		return &Result{
			ResultCode:      "no_candidates",
			CellsSearched:   cells,
			TookNs:          time.Since(start).Nanoseconds(),
		}, nil
	}

	// 2. Score & rank.
	scored := e.score(cands, ageSec)
	sort.Slice(scored, func(i, j int) bool { return scored[i].Score < scored[j].Score })
	if len(scored) > e.cfg.MaxCandidates {
		scored = scored[:e.cfg.MaxCandidates]
	}

	// 3. Try to reserve candidates in rank order until one wins.
	var lastErr error
	for _, c := range scored {
		res, err := e.resv.Reserve(ctx, c.DriverID, ev.RideID)
		if err == nil {
			dispatchID := e.dispatchID()
			// 4. Persist + notify (best-effort; the lease is already held).
			e.publishDispatchCreated(ctx, ev, c, dispatchID)
			e.log.Info("dispatch: driver assigned",
				"ride_id", ev.RideID, "driver_id", c.DriverID,
				"distance_m", c.DistanceM, "score", c.Score,
				"candidates", len(cands), "cells", cells,
				"took_ms", time.Since(start).Milliseconds())
			return &Result{
				Success:         true,
				DriverID:        c.DriverID,
				DispatchID:      dispatchID,
				ReserveResult:   res,
				ResultCode:      "ok",
				CellsSearched:   cells,
				CandidatesTotal: len(cands),
				TookNs:          time.Since(start).Nanoseconds(),
			}, nil
		}
		lastErr = err
		if err != reservation.ErrConflict {
			// Not a conflict (e.g. not-available): stop trying others.
			break
		}
	}
	return &Result{
		ResultCode:      "all_conflicts",
		CellsSearched:   cells,
		CandidatesTotal: len(cands),
		TookNs:          time.Since(start).Nanoseconds(),
	}, fmt.Errorf("dispatch: all candidates conflicted: %w", lastErr)
}

// score computes the dispatch score for each candidate.
//
//	score = WDistance·distanceM + WRequestAge·requestAgeSec + WRating·(5 - rating)
//
// Lower is better. Distance is GEODESIC (straight-line), not road distance —
// a routing engine can replace this term later without touching the pipeline.
func (e *Engine) score(cands []*redis.DriverPos, ageSec float64) []Candidate {
	out := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		rating := c.Rating
		if rating <= 0 {
			rating = 5.0 // unknown rating → neutral
		}
		s := e.cfg.WDistance*c.DistM +
			e.cfg.WRequestAge*ageSec +
			e.cfg.WRating*(5.0-rating)
		out = append(out, Candidate{
			DriverID:  c.ID,
			Lat:       c.Lat,
			Lng:       c.Lng,
			DistanceM: c.DistM,
			Score:     s,
			Rating:    rating,
			Cell:      c.Cell,
		})
	}
	return out
}

// publishDispatchCreated emits the dispatch-created event (keyed by ride_id).
func (e *Engine) publishDispatchCreated(ctx context.Context, ev *events.RideRequested, c Candidate, dispatchID string) {
	if e.prod == nil {
		return
	}
	d := events.DispatchCreated{
		Envelope:   events.Envelope{EventID: dispatchID, Type: "dispatch.created", Timestamp: time.Now()},
		DispatchID: dispatchID,
		RideID:     ev.RideID,
		DriverID:   c.DriverID,
		DistanceM:  c.DistanceM,
		Score:      c.Score,
	}
	b, err := events.Encode(d)
	if err != nil {
		e.log.Error("dispatch: encode event", "err", err)
		return
	}
	if err := e.prod.Publish(ctx, kafka.TopicDispatchCreated, ev.RideID, b); err != nil {
		e.log.Error("dispatch: publish dispatch-created", "err", err)
	}
	// Also emit driver-reserved (keyed by driver_id) for the location
	// processor / persistence.
	dr := events.DriverReserved{
		Envelope:     events.Envelope{EventID: dispatchID + "-res", Type: "driver.reserved", Timestamp: time.Now()},
		DriverID:     c.DriverID,
		RideID:       ev.RideID,
		LeaseVersion: 0,
	}
	if b2, err := events.Encode(dr); err == nil {
		if err := e.prod.Publish(ctx, kafka.TopicDriverReserved, c.DriverID, b2); err != nil {
			e.log.Error("dispatch: publish driver-reserved", "err", err)
		}
	}
}

// newID returns a simple unique id generator (prefix + timestamp + counter).
func newID(prefix string) func() string {
	var n uint64
	return func() string {
		n++
		return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), n)
	}
}

// FindCandidates performs a search without reserving (for /drivers/nearby).
func (e *Engine) FindCandidates(ctx context.Context, lat, lng, radiusM float64, max int) ([]Candidate, int, int64, error) {
	start := time.Now()
	cands, cells, err := e.store.SearchNearby(ctx, lat, lng, radiusM)
	if err != nil {
		return nil, 0, 0, err
	}
	scored := e.score(cands, 0)
	sort.Slice(scored, func(i, j int) bool { return scored[i].Score < scored[j].Score })
	if max > 0 && len(scored) > max {
		scored = scored[:max]
	}
	return scored, cells, time.Since(start).Nanoseconds(), nil
}

// ReleaseDriver releases a reservation (ride cancelled / driver rejected).
func (e *Engine) ReleaseDriver(ctx context.Context, res *reservation.Reservation) error {
	return res.Release(ctx, e.resv)
}

// DispatchByRideID dispatches a ride given only its ID (gRPC path): it reads
// the ride's hot state for pickup coordinates.
func (e *Engine) DispatchByRideID(ctx context.Context, rideID string) (*Result, error) {
	m, err := e.store.GetRide(ctx, rideID)
	if err != nil {
		return nil, fmt.Errorf("dispatch: get ride %s: %w", rideID, err)
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("dispatch: ride %s not found in hot state", rideID)
	}
	var lat, lng, radius float64
	fmt.Sscanf(m["pickup_lat"], "%f", &lat)
	fmt.Sscanf(m["pickup_lng"], "%f", &lng)
	fmt.Sscanf(m["radius_m"], "%f", &radius)
	if radius <= 0 {
		radius = e.cfg.DefaultRadiusM
	}
	var createdMs int64
	fmt.Sscanf(m["created_at"], "%d", &createdMs)
	ev := &events.RideRequested{
		Envelope:  events.Envelope{EventID: "grpc-" + rideID, Type: "ride.requested", Timestamp: time.UnixMilli(createdMs)},
		RideID:    rideID,
		UserID:    m["user_id"],
		PickupLat: lat,
		PickupLng: lng,
		RadiusM:   int32(radius),
	}
	return e.DispatchForRide(ctx, ev)
}