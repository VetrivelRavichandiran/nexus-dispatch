// Package location implements the Location Service: GPS ingestion,
// validation, sequence stamping, H3 cell computation, Kafka publish, and
// Redis hot-state write.
package location

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nexus-dispatch/nexus-dispatch/internal/events"
	"github.com/nexus-dispatch/nexus-dispatch/internal/geospatial"
	"github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
)

// Service implements nexusv1.LocationServiceServer.
type Service struct {
	nexusv1.UnimplementedLocationServiceServer

	store *redis.Store
	prod  *kafka.Producer
	log   *slog.Logger
	// per-driver sequence counters (server-assigned when client sends 0).
	seqs atomic.Uint64
}

// NewService constructs a Location Service.
func NewService(store *redis.Store, prod *kafka.Producer, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{store: store, prod: prod, log: log}
}

// UpdateLocation ingests a single GPS update.
func (s *Service) UpdateLocation(ctx context.Context, req *nexusv1.UpdateLocationRequest) (*nexusv1.UpdateLocationResponse, error) {
	resp, _ := s.apply(ctx, req, true)
	return resp, nil
}

// BatchUpdateLocation ingests many updates (simulator path).
func (s *Service) BatchUpdateLocation(ctx context.Context, req *nexusv1.BatchUpdateLocationRequest) (*nexusv1.BatchUpdateLocationResponse, error) {
	accepted, rejected := 0, 0
	for _, u := range req.Updates {
		resp, err := s.apply(ctx, u, false)
		if err != nil {
			rejected++
			continue
		}
		if resp.Accepted {
			accepted++
		} else {
			rejected++
		}
	}
	return &nexusv1.BatchUpdateLocationResponse{Accepted: int32(accepted), Rejected: int32(rejected)}, nil
}

// apply validates, stamps, writes Redis (atomic seq guard), and publishes.
func (s *Service) apply(ctx context.Context, req *nexusv1.UpdateLocationRequest, single bool) (*nexusv1.UpdateLocationResponse, error) {
	if req.DriverId == "" {
		return nil, status.Error(codes.InvalidArgument, "driver_id is required")
	}
	if err := geospatial.ValidateLatLng(req.Lat, req.Lng); err != nil {
		return &nexusv1.UpdateLocationResponse{Accepted: false, RejectReason: "invalid_coords"}, nil
	}

	// Server-assigned sequence when the client didn't provide one.
	seq := req.Sequence
	if seq <= 0 {
		seq = int64(s.seqs.Add(1))
	}

	// Atomic Redis write with the out-of-order/duplicate guard.
	applied, err := s.store.ApplyLocation(ctx, req.DriverId, req.Lat, req.Lng, seq, time.Now().UnixMilli())
	if err != nil {
		if single {
			return nil, status.Errorf(codes.Internal, "location: redis: %v", err)
		}
		return &nexusv1.UpdateLocationResponse{Accepted: false, RejectReason: "redis_error"}, nil
	}
	if !applied {
		return &nexusv1.UpdateLocationResponse{Accepted: false, RejectReason: "stale_sequence", ServerSequence: seq}, nil
	}

	// Compute the H3 cell (for the event + observability).
	cell, _ := geospatial.CellKey(req.Lat, req.Lng, geospatial.DefaultResolution)

	// Publish to Kafka (best-effort; the Redis write is the source of truth
	// for hot state, Kafka feeds the persistence pipeline).
	if s.prod != nil {
		ev := &events.LocationUpdate{
			Envelope:  events.Envelope{EventID: fmt.Sprintf("loc-%d", time.Now().UnixNano()), Type: "location.update", Timestamp: time.Now()},
			DriverID:  req.DriverId,
			Latitude:  req.Lat,
			Longitude: req.Lng,
			Sequence:  seq,
			H3Cell:    cell,
		}
		if b, err := events.Encode(ev); err == nil {
			if err := s.prod.Publish(ctx, kafka.TopicDriverLocation, req.DriverId, b); err != nil {
				s.log.Warn("location: kafka publish failed", "driver_id", req.DriverId, "err", err)
			}
		}
	}

	return &nexusv1.UpdateLocationResponse{
		Accepted: true, ServerSequence: seq, H3Cell: cell,
	}, nil
}