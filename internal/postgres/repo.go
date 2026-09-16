package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// DriverRow is a durable driver record.
type DriverRow struct {
	ID             string
	Name           string
	Email          string
	State          string
	Rating         float64
	TripsCompleted int64
}

// RideRow is a durable ride record.
type RideRow struct {
	ID        string
	UserID    string
	DriverID  string
	State     string
	PickupLat float64
	PickupLng float64
	DropLat   float64
	DropLng   float64
	CreatedAt time.Time
}

// InsertDriver persists a new driver (and its vehicle). Idempotent via
// ON CONFLICT DO NOTHING.
func (p *Pool) InsertDriver(ctx context.Context, id, name, email, state string, lat, lng float64, make, model, plate string, seats int) (bool, error) {
	tag, err := p.p.Exec(ctx, `
		INSERT INTO drivers (id, name, email, state, last_lat, last_lng, last_location_at, location)
		VALUES ($1, $2, $3, $4, $5, $6, now(), ST_SetSRID(ST_MakePoint($6, $5), 4326)::geography)
		ON CONFLICT (id) DO NOTHING`, id, name, email, state, lat, lng)
	if err != nil {
		return false, fmt.Errorf("postgres: insert driver: %w", err)
	}
	if plate != "" {
		_, _ = p.p.Exec(ctx, `
			INSERT INTO vehicles (driver_id, make, model, plate, seats)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (plate) DO NOTHING`, id, make, model, plate, seats)
	}
	return tag.RowsAffected() == 1, nil
}

// UpdateDriverState persists a driver state transition.
func (p *Pool) UpdateDriverState(ctx context.Context, id, state string) error {
	_, err := p.p.Exec(ctx, `
		UPDATE drivers SET state = $2, updated_at = now() WHERE id = $1`, id, state)
	return err
}

// GetDriver fetches a durable driver record.
func (p *Pool) GetDriver(ctx context.Context, id string) (*DriverRow, error) {
	row := p.p.QueryRow(ctx, `
		SELECT id::text, name, email, state, rating, trips_completed
		FROM drivers WHERE id = $1`, id)
	var d DriverRow
	err := row.Scan(&d.ID, &d.Name, &d.Email, &d.State, &d.Rating, &d.TripsCompleted)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get driver: %w", err)
	}
	return &d, nil
}

// InsertRide persists a new ride. Idempotent via ON CONFLICT DO NOTHING.
func (p *Pool) InsertRide(ctx context.Context, id, userID string, pickupLat, pickupLng, dropLat, dropLng float64, radiusM int) (bool, error) {
	tag, err := p.p.Exec(ctx, `
		INSERT INTO rides (id, user_id, state, pickup, dropoff, radius_m)
		VALUES ($1, $2, 'REQUESTED',
		        ST_SetSRID(ST_MakePoint($4, $3), 4326)::geography,
		        ST_SetSRID(ST_MakePoint($6, $5), 4326)::geography,
		        $7)
		ON CONFLICT (id) DO NOTHING`, id, userID, pickupLat, pickupLng, dropLat, dropLng, radiusM)
	if err != nil {
		return false, fmt.Errorf("postgres: insert ride: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// UpdateRideState persists a ride state transition.
func (p *Pool) UpdateRideState(ctx context.Context, id, state, driverID, reason string) error {
	_, err := p.p.Exec(ctx, `
		UPDATE rides SET
			state = $2,
			driver_id = COALESCE($3::uuid, driver_id),
			cancel_reason = COALESCE($4, cancel_reason),
			matched_at = CASE WHEN $2 IN ('MATCHED','DRIVER_EN_ROUTE') AND matched_at IS NULL THEN now() ELSE matched_at END,
			completed_at = CASE WHEN $2 = 'COMPLETED' THEN now() ELSE completed_at END,
			updated_at = now()
		WHERE id = $1`, id, state, driverID, reason)
	return err
}

// GetRide fetches a durable ride record.
func (p *Pool) GetRide(ctx context.Context, id string) (*RideRow, error) {
	row := p.p.QueryRow(ctx, `
		SELECT r.id::text, r.user_id::text, COALESCE(r.driver_id::text,''), r.state,
		       ST_Y(r.pickup::geometry), ST_X(r.pickup::geometry),
		       ST_Y(r.dropoff::geometry), ST_X(r.dropoff::geometry),
		       r.created_at
		FROM rides r WHERE r.id = $1`, id)
	var r RideRow
	err := row.Scan(&r.ID, &r.UserID, &r.DriverID, &r.State,
		&r.PickupLat, &r.PickupLng, &r.DropLat, &r.DropLng, &r.CreatedAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("postgres: get ride: %w", err)
	}
	return &r, nil
}

// InsertRideEvent appends a ride lifecycle event. Idempotent: the
// UNIQUE(ride_id, event_id) constraint makes duplicates no-ops.
func (p *Pool) InsertRideEvent(ctx context.Context, rideID, state, driverID, reason, eventID string) (bool, error) {
	tag, err := p.p.Exec(ctx, `
		INSERT INTO ride_events (ride_id, state, driver_id, reason, event_id)
		VALUES ($1, $2, NULLIF($3,'')::uuid, $4, $5)
		ON CONFLICT (ride_id, event_id) DO NOTHING`, rideID, state, driverID, reason, eventID)
	if err != nil {
		return false, fmt.Errorf("postgres: insert ride event: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// InsertDispatch persists a dispatch record. The uq_active_dispatch unique
// partial index is the durable backstop: a second ACTIVE dispatch for the
// same driver fails with a unique violation.
func (p *Pool) InsertDispatch(ctx context.Context, id, rideID, driverID string, distanceM, score float64, candidates, cells int, latencyMs float64) error {
	_, err := p.p.Exec(ctx, `
		INSERT INTO dispatches (id, ride_id, driver_id, state, distance_m, score,
			candidates_considered, cells_searched, dispatch_latency_ms)
		VALUES ($1, $2, $3, 'RESERVED', $4, $5, $6, $7, $8)
		ON CONFLICT (ride_id, driver_id) DO NOTHING`,
		id, rideID, driverID, distanceM, score, candidates, cells, latencyMs)
	if err != nil {
		return fmt.Errorf("postgres: insert dispatch: %w", err)
	}
	return nil
}

// UpdateDispatchState persists a dispatch state transition.
func (p *Pool) UpdateDispatchState(ctx context.Context, id, state string) error {
	_, err := p.p.Exec(ctx, `
		UPDATE dispatches SET state = $2, updated_at = now() WHERE id = $1`, id, state)
	return err
}

// InsertDriverHistory appends a downsampled GPS point. Idempotent via the
// (driver_id, ts) primary key.
func (p *Pool) InsertDriverHistory(ctx context.Context, driverID string, ts time.Time, lat, lng float64, seq int64) error {
	_, err := p.p.Exec(ctx, `
		INSERT INTO driver_history (driver_id, ts, location, seq)
		VALUES ($1, $2, ST_SetSRID(ST_MakePoint($4, $3), 4326)::geography, $5)
		ON CONFLICT (driver_id, ts) DO NOTHING`, driverID, ts, lat, lng, seq)
	if err != nil {
		return fmt.Errorf("postgres: insert driver history: %w", err)
	}
	return nil
}

// ActiveDispatchCount returns how many active dispatches a driver has
// (should always be <= 1; used by invariant checks).
func (p *Pool) ActiveDispatchCount(ctx context.Context, driverID string) (int, error) {
	var n int
	err := p.p.QueryRow(ctx, `
		SELECT count(*) FROM dispatches
		WHERE driver_id = $1 AND state IN ('RESERVED','ASSIGNED','ON_TRIP')`, driverID).Scan(&n)
	return n, err
}