package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/postgres"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
)

// Service implements nexusv1.DriverServiceServer.
type Service struct {
	nexusv1.UnimplementedDriverServiceServer

	store *redis.Store
	db    *postgres.Pool // may be nil (hot-only mode)
	prod  *kafka.Producer
	log   *slog.Logger
	// in-memory fallback registry when Postgres is unavailable (dev/demo).
	mu      sync.RWMutex
	drivers map[string]*nexusv1.Driver
}

// NewService constructs a Driver Service.
func NewService(store *redis.Store, db *postgres.Pool, prod *kafka.Producer, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		store:   store,
		db:      db,
		prod:    prod,
		log:     log,
		drivers: make(map[string]*nexusv1.Driver),
	}
}

// RegisterDriver persists a new driver (Postgres) and seeds Redis hot state.
func (s *Service) RegisterDriver(ctx context.Context, req *nexusv1.RegisterDriverRequest) (*nexusv1.RegisterDriverResponse, error) {
	if req.Name == "" || req.Email == "" {
		return nil, status.Error(codes.InvalidArgument, "name and email are required")
	}
	id := "drv-" + req.Email // stable id derived from email (demo)
	state := StateAvailable
	if req.InitialLat == 0 && req.InitialLng == 0 {
		state = StateOffline
	}

	// Durable (best-effort: if Postgres is down we still register in hot state).
	if s.db != nil {
		if _, err := s.db.InsertDriver(ctx, id, req.Name, req.Email, string(state),
			req.InitialLat, req.InitialLng,
			req.Vehicle.Make, req.Vehicle.Model, req.Vehicle.Plate, int(req.Vehicle.Seats)); err != nil {
			s.log.Warn("driver: durable insert failed (hot-only)", "driver_id", id, "err", err)
		}
	}

	// Hot state.
	if err := s.store.InitDriver(ctx, id, req.Name, string(state), 4.8, req.InitialLat, req.InitialLng); err != nil {
		return nil, status.Errorf(codes.Internal, "driver: init hot state: %v", err)
	}

	s.mu.Lock()
	s.drivers[id] = &nexusv1.Driver{
		Id: id, Name: req.Name, Email: req.Email, Vehicle: req.Vehicle,
		State: nexusv1.DriverState_AVAILABLE,
		Rating: 4.8, CreatedAtMs: time.Now().UnixMilli(), UpdatedAtMs: time.Now().UnixMilli(),
	}
	s.mu.Unlock()

	s.log.Info("driver registered", "driver_id", id, "state", state)
	s.emitDriverStatus(ctx, id, string(state), "registered")
	return &nexusv1.RegisterDriverResponse{
		Driver: &nexusv1.Driver{
			Id: id, Name: req.Name, Email: req.Email, Vehicle: req.Vehicle,
			State: nexusv1.DriverState_AVAILABLE,
			Rating: 4.8, CreatedAtMs: time.Now().UnixMilli(),
		},
	}, nil
}

// GetDriver fetches a driver (hot state preferred, Postgres fallback).
func (s *Service) GetDriver(ctx context.Context, req *nexusv1.GetDriverRequest) (*nexusv1.Driver, error) {
	s.mu.RLock()
	if d, ok := s.drivers[req.DriverId]; ok {
		s.mu.RUnlock()
		return d, nil
	}
	s.mu.RUnlock()

	state, err := s.store.GetState(ctx, req.DriverId)
	if err != nil || state == "" {
		if s.db != nil {
			row, err := s.db.GetDriver(ctx, req.DriverId)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "driver: get: %v", err)
			}
			if row == nil {
				return nil, status.Errorf(codes.NotFound, "driver %s not found", req.DriverId)
			}
			return &nexusv1.Driver{Id: row.ID, Name: row.Name, Email: row.Email,
				State: stateToProto(row.State), Rating: row.Rating, TripsCompleted: row.TripsCompleted}, nil
		}
		return nil, status.Errorf(codes.NotFound, "driver %s not found", req.DriverId)
	}
	return &nexusv1.Driver{Id: req.DriverId, State: stateToProto(state)}, nil
}

// UpdateDriverStatus enforces the state machine and updates hot + durable state.
func (s *Service) UpdateDriverStatus(ctx context.Context, req *nexusv1.UpdateDriverStatusRequest) (*nexusv1.UpdateDriverStatusResponse, error) {
	to := State(protoToState(req.State))
	if err := ValidateState(to); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	from, err := s.store.GetState(ctx, req.DriverId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "driver: read state: %v", err)
	}
	if from == "" {
		return nil, status.Errorf(codes.NotFound, "driver %s not found", req.DriverId)
	}
	fromState := State(from)
	if err := AssertTransition(fromState, to); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if err := s.store.SetState(ctx, req.DriverId, string(to)); err != nil {
		return nil, status.Errorf(codes.Internal, "driver: set state: %v", err)
	}
	if s.db != nil {
		_ = s.db.UpdateDriverState(ctx, req.DriverId, string(to))
	}
	s.mu.Lock()
	if d, ok := s.drivers[req.DriverId]; ok {
		d.State = req.State
		d.UpdatedAtMs = time.Now().UnixMilli()
	}
	s.mu.Unlock()

	s.log.Info("driver state changed", "driver_id", req.DriverId, "from", from, "to", to)
	s.emitDriverStatus(ctx, req.DriverId, string(to), "manual")
	return &nexusv1.UpdateDriverStatusResponse{
		Previous: stateToProto(from),
		Current:  stateToProto(string(to)),
	}, nil
}

// AssignDriver is called by the Dispatch Engine after winning a reservation.
// It transitions RESERVED → ON_TRIP is NOT done here (that happens when the
// driver accepts); here we just confirm the assignment and record the ride.
func (s *Service) AssignDriver(ctx context.Context, req *nexusv1.AssignDriverRequest) (*nexusv1.AssignDriverResponse, error) {
	state, err := s.store.GetState(ctx, req.DriverId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "driver: read state: %v", err)
	}
	if state != string(StateReserved) {
		return nil, status.Errorf(codes.FailedPrecondition, "driver %s not reserved (state=%s)", req.DriverId, state)
	}
	// Confirm: driver is now committed to this ride. Keep RESERVED until the
	// driver accepts (→ ON_TRIP). Record the ride association in hot state.
	_ = s.store.RDB().HSet(ctx, redis.DriverKey(req.DriverId), "assigned_ride", req.RideId).Err()
	s.log.Info("driver assigned to ride", "driver_id", req.DriverId, "ride_id", req.RideId)
	return &nexusv1.AssignDriverResponse{State: stateToProto(state)}, nil
}

// ReleaseDriver returns a driver to AVAILABLE (reservation cancelled/expired).
func (s *Service) ReleaseDriver(ctx context.Context, req *nexusv1.ReleaseDriverRequest) (*nexusv1.ReleaseDriverResponse, error) {
	state, _ := s.store.GetState(ctx, req.DriverId)
	if state != string(StateReserved) && state != string(StateOnTrip) {
		return nil, status.Errorf(codes.FailedPrecondition, "driver %s not reservable (state=%s)", req.DriverId, state)
	}
	if err := s.store.SetState(ctx, req.DriverId, string(StateAvailable)); err != nil {
		return nil, status.Errorf(codes.Internal, "driver: set state: %v", err)
	}
	if s.db != nil {
		_ = s.db.UpdateDriverState(ctx, req.DriverId, string(StateAvailable))
	}
	s.log.Info("driver released", "driver_id", req.DriverId, "reason", req.Reason)
	s.emitDriverStatus(ctx, req.DriverId, string(StateAvailable), req.Reason)
	return &nexusv1.ReleaseDriverResponse{State: stateToProto(string(StateAvailable))}, nil
}

// emitDriverStatus publishes a driver-status event (best-effort).
func (s *Service) emitDriverStatus(ctx context.Context, driverID, state, reason string) {
	if s.prod == nil {
		return
	}
	ev := &struct {
		EventID   string    `json:"event_id"`
		Type      string    `json:"type"`
		Timestamp time.Time `json:"timestamp"`
		DriverID  string    `json:"driver_id"`
		State     string    `json:"state"`
		Reason    string    `json:"reason"`
		}{fmt.Sprintf("dst-%d", time.Now().UnixNano()), "driver.status", time.Now(), driverID, state, reason}
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	if err := s.prod.Publish(ctx, kafka.TopicDriverStatus, driverID, b); err != nil {
		s.log.Warn("driver: emit status failed", "err", err)
	}
}

func stateToProto(s string) nexusv1.DriverState {
	switch State(s) {
	case StateAvailable:
		return nexusv1.DriverState_AVAILABLE
	case StateReserved:
		return nexusv1.DriverState_RESERVED
	case StateOnTrip:
		return nexusv1.DriverState_ON_TRIP
	case StatePaused:
		return nexusv1.DriverState_PAUSED
	default:
		return nexusv1.DriverState_OFFLINE
	}
}

func protoToState(s nexusv1.DriverState) State {
	switch s {
	case nexusv1.DriverState_AVAILABLE:
		return StateAvailable
	case nexusv1.DriverState_RESERVED:
		return StateReserved
	case nexusv1.DriverState_ON_TRIP:
		return StateOnTrip
	case nexusv1.DriverState_PAUSED:
		return StatePaused
	default:
		return StateOffline
	}
}

// ensure pgx import retained for error type
var _ = pgx.ErrNoRows