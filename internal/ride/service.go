// Package ride implements the Ride Service: ride lifecycle, queries, and
// cancellation. Rides are created in REQUESTED state and published to Kafka;
// the Dispatch Engine picks them up.
package ride

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nexus-dispatch/nexus-dispatch/internal/events"
	"github.com/nexus-dispatch/nexus-dispatch/internal/geospatial"
	"github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/postgres"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
)

// State is a ride lifecycle state.
type State string

const (
	StateRequested    State = "REQUESTED"
	StateMatched      State = "MATCHED"
	StateDriverEnRoute State = "DRIVER_EN_ROUTE"
	StateArrived      State = "ARRIVED"
	StateInProgress   State = "IN_PROGRESS"
	StateCompleted    State = "COMPLETED"
	StateCancelled    State = "CANCELLED"
	StateExpired      State = "EXPIRED"
)

// validRideTransitions encodes the ride state machine.
var validRideTransitions = map[State]map[State]bool{
	StateRequested:     {StateMatched: true, StateCancelled: true, StateExpired: true},
	StateMatched:       {StateDriverEnRoute: true, StateCancelled: true, StateExpired: true},
	StateDriverEnRoute: {StateArrived: true, StateCancelled: true},
	StateArrived:       {StateInProgress: true, StateCancelled: true},
	StateInProgress:    {StateCompleted: true},
	// Terminal states have no outgoing edges.
}

// CanTransition validates a ride state transition.
func CanTransition(from, to State) bool {
	if from == to {
		return true
	}
	return validRideTransitions[from][to]
}

// Service implements nexusv1.RideServiceServer.
type Service struct {
	nexusv1.UnimplementedRideServiceServer

	store *redis.Store
	db    *postgres.Pool
	prod  *kafka.Producer
	log   *slog.Logger
}

// NewService constructs a Ride Service.
func NewService(store *redis.Store, db *postgres.Pool, prod *kafka.Producer, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, db: db, prod: prod, log: log}
}

// CreateRide creates a ride in REQUESTED state and publishes ride-requested.
func (s *Service) CreateRide(ctx context.Context, req *nexusv1.CreateRideRequest) (*nexusv1.Ride, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if err := geospatial.ValidateLatLng(req.PickupLat, req.PickupLng); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid pickup: %v", err)
	}
	if req.DropoffLat != 0 || req.DropoffLng != 0 {
		if err := geospatial.ValidateLatLng(req.DropoffLat, req.DropoffLng); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid dropoff: %v", err)
		}
	}
	radius := int(req.RadiusM)
	if radius <= 0 {
		radius = 1000
	}
	rideID := fmt.Sprintf("ride-%d", time.Now().UnixNano())

	// Durable (best-effort).
	if s.db != nil {
		if _, err := s.db.InsertRide(ctx, rideID, req.UserId, req.PickupLat, req.PickupLng, req.DropoffLat, req.DropoffLng, radius); err != nil {
			s.log.Warn("ride: durable insert failed", "ride_id", rideID, "err", err)
		}
	}

	// Hot state (for dispatch + WS fan-out).
	_ = s.store.SetRide(ctx, rideID, map[string]any{
		"id": rideID, "user_id": req.UserId, "state": string(StateRequested),
		"pickup_lat": req.PickupLat, "pickup_lng": req.PickupLng,
		"dropoff_lat": req.DropoffLat, "dropoff_lng": req.DropoffLng,
		"radius_m": radius, "created_at": time.Now().UnixMilli(),
	}, 30*time.Minute)

	// Publish ride-requested (keyed by ride_id).
	if s.prod != nil {
		ev := &events.RideRequested{
			Envelope:   events.Envelope{EventID: fmt.Sprintf("req-%s", rideID), Type: "ride.requested", Timestamp: time.Now()},
			RideID:     rideID,
			UserID:     req.UserId,
			PickupLat:  req.PickupLat,
			PickupLng:  req.PickupLng,
			DropoffLat: req.DropoffLat,
			DropoffLng: req.DropoffLng,
			RadiusM:    int32(radius),
		}
		if b, err := events.Encode(ev); err == nil {
			if err := s.prod.Publish(ctx, kafka.TopicRideRequested, rideID, b); err != nil {
				s.log.Error("ride: publish ride-requested failed", "ride_id", rideID, "err", err)
			}
		}
	}

	s.log.Info("ride created", "ride_id", rideID, "user_id", req.UserId)
	return &nexusv1.Ride{
		Id: rideID, UserId: req.UserId, State: nexusv1.RideState_REQUESTED,
		PickupLat: req.PickupLat, PickupLng: req.PickupLng,
		DropoffLat: req.DropoffLat, DropoffLng: req.DropoffLng,
		CreatedAtMs: time.Now().UnixMilli(),
	}, nil
}

// GetRide fetches a ride (hot state preferred, Postgres fallback).
func (s *Service) GetRide(ctx context.Context, req *nexusv1.GetRideRequest) (*nexusv1.Ride, error) {
	m, err := s.store.GetRide(ctx, req.RideId)
	if err == nil && len(m) > 0 {
		return rideFromHot(m), nil
	}
	if s.db != nil {
		row, err := s.db.GetRide(ctx, req.RideId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "ride: get: %v", err)
		}
		if row == nil {
			return nil, status.Errorf(codes.NotFound, "ride %s not found", req.RideId)
		}
		return &nexusv1.Ride{
			Id: row.ID, UserId: row.UserID, DriverId: row.DriverID,
			State: stateToProto(row.State),
			PickupLat: row.PickupLat, PickupLng: row.PickupLng,
			DropoffLat: row.DropLat, DropoffLng: row.DropLng,
		}, nil
	}
	return nil, status.Errorf(codes.NotFound, "ride %s not found", req.RideId)
}

// CancelRide cancels a ride (only from non-terminal states).
func (s *Service) CancelRide(ctx context.Context, req *nexusv1.CancelRideRequest) (*nexusv1.Ride, error) {
	m, err := s.store.GetRide(ctx, req.RideId)
	if err != nil || len(m) == 0 {
		return nil, status.Errorf(codes.NotFound, "ride %s not found", req.RideId)
	}
	from := State(m["state"])
	if !CanTransition(from, StateCancelled) {
		return nil, status.Errorf(codes.FailedPrecondition, "ride %s in state %s cannot be cancelled", req.RideId, from)
	}
	_ = s.store.RDB().HSet(ctx, redis.RideKey(req.RideId), "state", string(StateCancelled), "cancel_reason", req.Reason).Err()
	if s.db != nil {
		_ = s.db.UpdateRideState(ctx, req.RideId, string(StateCancelled), "", req.Reason)
		_, _ = s.db.InsertRideEvent(ctx, req.RideId, string(StateCancelled), "", req.Reason, fmt.Sprintf("cancel-%s", req.RideId))
	}
	s.log.Info("ride cancelled", "ride_id", req.RideId, "reason", req.Reason)
	return rideFromHot(map[string]string{"id": req.RideId, "state": string(StateCancelled), "cancel_reason": req.Reason}), nil
}

// ListRides lists a user's rides (Postgres-backed; hot state has no index).
func (s *Service) ListRides(ctx context.Context, req *nexusv1.ListRidesRequest) (*nexusv1.ListRidesResponse, error) {
	if s.db == nil {
		return &nexusv1.ListRidesResponse{}, nil
	}
	limit := int(req.Limit)
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.Raw().Query(ctx, `
		SELECT id::text, user_id::text, COALESCE(driver_id::text,''), state,
		       ST_Y(pickup::geometry), ST_X(pickup::geometry),
		       ST_Y(dropoff::geometry), ST_X(dropoff::geometry), created_at
		FROM rides
		WHERE user_id = $1 AND created_at < to_timestamp($2/1000.0)
		ORDER BY created_at DESC LIMIT $3`,
		req.UserId, req.CursorCreatedMs, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ride: list: %v", err)
	}
	defer rows.Close()
	var rides []*nexusv1.Ride
	var nextCursor int64
	for rows.Next() {
		var (
			id, userID, driverID, state string
			pickupLat, pickupLng, dropLat, dropLng float64
			created time.Time
		)
		if err := rows.Scan(&id, &userID, &driverID, &state,
			&pickupLat, &pickupLng, &dropLat, &dropLng, &created); err != nil {
			return nil, status.Errorf(codes.Internal, "ride: list scan: %v", err)
		}
		rides = append(rides, &nexusv1.Ride{
			Id: id, UserId: userID, DriverId: driverID,
			State: stateToProto(state),
			PickupLat: pickupLat, PickupLng: pickupLng,
			DropoffLat: dropLat, DropoffLng: dropLng,
			CreatedAtMs: created.UnixMilli(),
		})
		nextCursor = created.UnixMilli()
	}
	hasMore := len(rides) == limit
	if hasMore && len(rides) > 0 {
		nextCursor = rides[len(rides)-1].CreatedAtMs
	}
	return &nexusv1.ListRidesResponse{Rides: rides, NextCursorCreatedMs: nextCursor, HasMore: hasMore}, nil
}

func rideFromHot(m map[string]string) *nexusv1.Ride {
	r := &nexusv1.Ride{Id: m["id"], UserId: m["user_id"], DriverId: m["driver_id"], State: stateToProto(m["state"])}
	if v, ok := m["pickup_lat"]; ok {
		fmt.Sscanf(v, "%f", &r.PickupLat)
	}
	if v, ok := m["pickup_lng"]; ok {
		fmt.Sscanf(v, "%f", &r.PickupLng)
	}
	if v, ok := m["created_at"]; ok {
		fmt.Sscanf(v, "%d", &r.CreatedAtMs)
	}
	return r
}

func stateToProto(s string) nexusv1.RideState {
	switch State(s) {
	case StateRequested:
		return nexusv1.RideState_REQUESTED
	case StateMatched:
		return nexusv1.RideState_MATCHED
	case StateDriverEnRoute:
		return nexusv1.RideState_DRIVER_EN_ROUTE
	case StateArrived:
		return nexusv1.RideState_ARRIVED
	case StateInProgress:
		return nexusv1.RideState_IN_PROGRESS
	case StateCompleted:
		return nexusv1.RideState_COMPLETED
	case StateCancelled:
		return nexusv1.RideState_CANCELLED
	case StateExpired:
		return nexusv1.RideState_EXPIRED
	default:
		return nexusv1.RideState_RIDE_STATE_UNSPECIFIED
	}
}