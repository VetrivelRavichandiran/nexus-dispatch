package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nexus-dispatch/nexus-dispatch/internal/geospatial"
)

// Store is the hot-state Redis client. All multi-key mutations go through the
// atomic Lua scripts in scripts.go.
type Store struct {
	rdb redis.UniversalClient
}

// NewStore wraps a redis client.
func NewStore(rdb redis.UniversalClient) *Store { return &Store{rdb: rdb} }

// RDB exposes the underlying client (for health checks / ops).
func (s *Store) RDB() redis.UniversalClient { return s.rdb }

// Ping checks connectivity.
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// DriverPos is a driver's last-known position with metadata.
type DriverPos struct {
	ID       string
	Lat      float64
	Lng      float64
	Cell     string
	Seq      int64
	Ts       int64
	State    string
	Rating   float64
	DistM    float64 // set by SearchNearby
}

// InitDriver creates the driver profile hash and registers it in the geo
// index. Idempotent: re-initializing an existing driver updates the state.
func (s *Store) InitDriver(ctx context.Context, id, name, state string, rating float64, lat, lng float64) error {
	cell, err := geospatial.CellKey(lat, lng, geospatial.DefaultResolution)
	if err != nil {
		return err
	}
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, DriverKey(id), map[string]any{
		"id": id, "name": name, "state": state, "rating": rating,
	})
	pipe.HSet(ctx, DriverLocKey(id), map[string]any{
		"lat": lat, "lng": lng, "cell": cell, "seq": 0, "ts": time.Now().UnixMilli(),
	})
	pipe.Expire(ctx, DriverLocKey(id), 120*time.Second)
	pipe.SAdd(ctx, CellKey(cell), id)
	pipe.GeoAdd(ctx, GeoKey, &redis.GeoLocation{Name: id, Longitude: lng, Latitude: lat})
	_, err = pipe.Exec(ctx)
	return err
}

// SetState updates a driver's availability state (non-lease path, e.g.
// driver goes OFFLINE/PAUSED).
func (s *Store) SetState(ctx context.Context, id, state string) error {
	return s.rdb.HSet(ctx, DriverKey(id), "state", state).Err()
}

// GetState returns a driver's current state, or "" if unknown.
func (s *Store) GetState(ctx context.Context, id string) (string, error) {
	v, err := s.rdb.HGet(ctx, DriverKey(id), "state").Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

// ApplyLocation atomically applies a location update with the sequence guard.
// Returns (applied, err). applied=false means the update was rejected as
// stale or duplicate (not an error).
func (s *Store) ApplyLocation(ctx context.Context, driverID string, lat, lng float64, seq, tsMs int64) (bool, error) {
	oldCell, _ := s.rdb.HGet(ctx, DriverLocKey(driverID), "cell").Result()
	if oldCell == "" {
		// First update: use the new cell for both (no migration needed).
		newCell, err := geospatial.CellKey(lat, lng, geospatial.DefaultResolution)
		if err != nil {
			return false, err
		}
		oldCell = newCell
	}
	newCell, err := geospatial.CellKey(lat, lng, geospatial.DefaultResolution)
	if err != nil {
		return false, err
	}
	res, err := s.rdb.Eval(ctx, applyLocationScript, []string{
		DriverSeqKey(driverID),
		DriverLocKey(driverID),
		CellKey(oldCell),
		CellKey(newCell),
		GeoKey,
	}, seq, lat, lng, newCell, tsMs, 120, driverID).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// GetLocation returns a driver's last-known position.
func (s *Store) GetLocation(ctx context.Context, driverID string) (*DriverPos, error) {
	m, err := s.rdb.HGetAll(ctx, DriverLocKey(driverID)).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	p := &DriverPos{ID: driverID, Cell: m["cell"]}
	p.Lat, _ = strconv.ParseFloat(m["lat"], 64)
	p.Lng, _ = strconv.ParseFloat(m["lng"], 64)
	p.Seq, _ = strconv.ParseInt(m["seq"], 10, 64)
	p.Ts, _ = strconv.ParseInt(m["ts"], 10, 64)
	if d, err := s.rdb.HGet(ctx, DriverKey(driverID), "state").Result(); err == nil {
		p.State = d
	}
	if r, err := s.rdb.HGet(ctx, DriverKey(driverID), "rating").Result(); err == nil {
		p.Rating, _ = strconv.ParseFloat(r, 64)
	}
	return p, nil
}

// SearchNearby finds AVAILABLE drivers within radiusM of (lat, lng) using the
// H3 cell index. It returns candidates sorted by geodesic distance (ascending)
// with DistM populated.
//
// Complexity: O(cells × set-size) where cells = 3k(k+1)+1 for the ring that
// covers radiusM — independent of total fleet size.
func (s *Store) SearchNearby(ctx context.Context, lat, lng, radiusM float64) ([]*DriverPos, int, error) {
	plan, err := geospatial.PlanSearch(lat, lng, radiusM, geospatial.DefaultResolution)
	if err != nil {
		return nil, 0, err
	}
	seen := make(map[string]bool)
	var out []*DriverPos
	for _, cell := range plan.Cells {
		ids, err := s.rdb.SMembers(ctx, CellKey(cell)).Result()
		if err != nil {
			return nil, 0, err
		}
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			m, err := s.rdb.HMGet(ctx, DriverLocKey(id), "lat", "lng", "cell").Result()
			if err != nil {
				return nil, 0, err
			}
			if len(m) < 3 {
				continue // no position yet
			}
			state, _ := s.rdb.HGet(ctx, DriverKey(id), "state").Result()
			if state != "AVAILABLE" {
				continue
			}
			dLat, _ := strconv.ParseFloat(m[0].(string), 64)
			dLng, _ := strconv.ParseFloat(m[1].(string), 64)
			d := geospatial.HaversineMeters(lat, lng, dLat, dLng)
			if d > radiusM {
				continue
			}
			rating, _ := s.rdb.HGet(ctx, DriverKey(id), "rating").Result()
			rf, _ := strconv.ParseFloat(rating, 64)
			out = append(out, &DriverPos{
				ID: id, Lat: dLat, Lng: dLng, Cell: m[2].(string),
				State: state, Rating: rf, DistM: d,
			})
		}
	}
	// sort by distance ascending
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].DistM < out[j-1].DistM; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, len(plan.Cells), nil
}

// ReserveResult encodes the outcome of an atomic reservation attempt.
type ReserveResult int

const (
	// Reserved means the caller won the reservation (or renewed its own lease).
	Reserved ReserveResult = iota
	// Conflict means another lease owns the driver.
	Conflict
	// NotAvailable means the driver is not in a reservable state.
	NotAvailable
)

func (r ReserveResult) String() string {
	switch r {
	case Reserved:
		return "reserved"
	case Conflict:
		return "conflict"
	case NotAvailable:
		return "not_available"
	default:
		return "unknown"
	}
}

// Reserve atomically reserves driverID for rideID. The lease value
// (rideID:token:version) makes renewal re-entrant and release
// compare-and-delete. ttl bounds the "winner crashed" window.
func (s *Store) Reserve(ctx context.Context, driverID, rideID string, lv LeaseValue, ttl time.Duration) (ReserveResult, error) {
	res, err := s.rdb.Eval(ctx, reserveScript, []string{
		DriverLeaseKey(driverID),
		DriverKey(driverID),
	}, rideID, lv.String(), int(ttl.Seconds())).Int()
	if err != nil {
		return NotAvailable, err
	}
	switch res {
	case 1:
		return Reserved, nil
	case 0:
		return Conflict, nil
	default:
		return NotAvailable, nil
	}
}

// Release compare-and-deletes the lease. Only the holder whose lease value
// matches can release; a stale holder (old version) cannot delete a newer
// lease. targetState is the driver state to restore (e.g. AVAILABLE).
func (s *Store) Release(ctx context.Context, driverID string, lv LeaseValue, targetState string) (bool, error) {
	res, err := s.rdb.Eval(ctx, releaseScript, []string{
		DriverLeaseKey(driverID),
		DriverKey(driverID),
	}, lv.String(), targetState).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// GetLease returns the current lease holder for a driver, if any.
func (s *Store) GetLease(ctx context.Context, driverID string) (*LeaseValue, error) {
	v, err := s.rdb.Get(ctx, DriverLeaseKey(driverID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	lv, err := ParseLeaseValue(v)
	if err != nil {
		return nil, err
	}
	return &lv, nil
}

// RateAllow attempts to consume `requested` tokens from the bucket for
// (scope, id). Returns (allowed, err).
func (s *Store) RateAllow(ctx context.Context, scope, id string, capacity int, refillPerSec float64, requested int) (bool, error) {
	now := time.Now().UnixMilli()
	res, err := s.rdb.Eval(ctx, tokenBucketScript, []string{
		RateLimitKey(scope, id),
	}, capacity, refillPerSec, now, requested, 2*60*1000).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}

// SetRide stores hot ride state (for dispatch decisions + WS fan-out).
func (s *Store) SetRide(ctx context.Context, id string, fields map[string]any, ttl time.Duration) error {
	pipe := s.rdb.TxPipeline()
	pipe.HSet(ctx, RideKey(id), fields)
	pipe.Expire(ctx, RideKey(id), ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// GetRide returns hot ride state.
func (s *Store) GetRide(ctx context.Context, id string) (map[string]string, error) {
	return s.rdb.HGetAll(ctx, RideKey(id)).Result()
}

// DriverCount returns (total, available) driver counts from Redis.
// O(drivers) — used by metrics scrapers, not the hot path.
func (s *Store) DriverCount(ctx context.Context) (total, available int64, err error) {
	var cur uint64
	for {
		var keys []string
		keys, cur, err = s.rdb.Scan(ctx, cur, DriverKeyPrefix+"*", 1000).Result()
		if err != nil {
			return 0, 0, err
		}
		for _, k := range keys {
			if len(k) <= len(DriverKeyPrefix) || k[len(DriverKeyPrefix):len(DriverKeyPrefix)+1] == ":" {
				continue // skip :loc / :lease keys
			}
			total++
			if st, e := s.rdb.HGet(ctx, k, "state").Result(); e == nil && st == "AVAILABLE" {
				available++
			}
		}
		if cur == 0 {
			break
		}
	}
	return total, available, nil
}

// RemoveDriver deletes all hot state for a driver (used in tests / teardown).
func (s *Store) RemoveDriver(ctx context.Context, id string) error {
	cell, _ := s.rdb.HGet(ctx, DriverLocKey(id), "cell").Result()
	pipe := s.rdb.TxPipeline()
	pipe.Del(ctx, DriverKey(id), DriverLocKey(id), DriverLeaseKey(id), DriverSeqKey(id))
	if cell != "" {
		pipe.SRem(ctx, CellKey(cell), id)
	}
	// A GEO key is a plain zset under the hood; ZRem removes the member.
	pipe.ZRem(ctx, GeoKey, id)
	_, err := pipe.Exec(ctx)
	return err
}

// RebuildFromDrivers re-seeds the geo index + cell sets from a list of
// (driverID, lat, lng, state) — used on startup to recover hot state from
// Postgres (see ADR-008).
func (s *Store) RebuildFromDrivers(ctx context.Context, drivers []struct {
	ID, State string
	Lat, Lng  float64
}) error {
	for _, d := range drivers {
		if err := s.InitDriver(ctx, d.ID, "", d.State, 0, d.Lat, d.Lng); err != nil {
			return fmt.Errorf("rebuild driver %s: %w", d.ID, err)
		}
	}
	return nil
}