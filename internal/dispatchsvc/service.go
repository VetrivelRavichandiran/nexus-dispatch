// Package dispatchsvc wires the dispatch Engine to gRPC and the Kafka
// ride-requested consumer.
package dispatchsvc

import (
	"context"
	"log/slog"
	"time"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/segmentio/kafka-go"

	"github.com/nexus-dispatch/nexus-dispatch/internal/dispatch"
	"github.com/nexus-dispatch/nexus-dispatch/internal/events"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
)

// Service implements nexusv1.DispatchServiceServer.
type Service struct {
	nexusv1.UnimplementedDispatchServiceServer
	eng   *dispatch.Engine
	m     *metrics.Dispatch
	log   *slog.Logger
}

// NewService wraps the engine.
func NewService(eng *dispatch.Engine, m *metrics.Dispatch, log *slog.Logger) *Service {
	return &Service{eng: eng, m: m, log: log}
}

// FindCandidates performs a non-reserving search (for /drivers/nearby).
func (s *Service) FindCandidates(ctx context.Context, req *nexusv1.FindCandidatesRequest) (*nexusv1.FindCandidatesResponse, error) {
	radius := float64(req.RadiusM)
	if radius <= 0 {
		radius = 1000
	}
	max := int(req.MaxResults)
	if max <= 0 {
		max = 20
	}
	cands, cells, tookNs, err := s.eng.FindCandidates(ctx, req.Lat, req.Lng, radius, max)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dispatch: search: %v", err)
	}
	out := &nexusv1.FindCandidatesResponse{
		CellsSearched:        int32(cells),
		CandidatesConsidered: int32(len(cands)),
		TookNs:               tookNs,
	}
	for _, c := range cands {
		out.Candidates = append(out.Candidates, &nexusv1.Candidate{
			DriverId: c.DriverID, Lat: c.Lat, Lng: c.Lng,
			DistanceM: c.DistanceM, Score: c.Score, Rating: c.Rating, H3Cell: c.Cell,
		})
	}
	return out, nil
}

// DispatchForRide runs the pipeline for a ride (also triggered via Kafka).
func (s *Service) DispatchForRide(ctx context.Context, req *nexusv1.DispatchForRideRequest) (*nexusv1.DispatchForRideResponse, error) {
	// Look up the ride's hot state to get pickup coords.
	// (The Kafka path has the coords in the event; this gRPC path fetches.)
	return s.runDispatch(ctx, req.RideId, req.RequestId)
}

func (s *Service) runDispatch(ctx context.Context, rideID, requestID string) (*nexusv1.DispatchForRideResponse, error) {
	// Build a minimal event from hot state.
	res, err := s.eng.DispatchByRideID(ctx, rideID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dispatch: %v", err)
	}
	if s.m != nil {
		s.m.DispatchLatency.WithLabelValues(res.ResultCode).Observe(float64(res.TookNs) / 1e9)
		if res.Success {
			s.m.SuccessTotal.Inc()
		} else {
			s.m.FailureTotal.WithLabelValues(res.ResultCode).Inc()
		}
		s.m.Candidates.WithLabelValues().Observe(float64(res.CandidatesTotal))
	}
	return &nexusv1.DispatchForRideResponse{
		DriverId:   res.DriverID,
		DispatchId: res.DispatchID,
		Success:    res.Success,
		ResultCode: res.ResultCode,
	}, nil
}

// HandleRideRequested is the Kafka consumer handler: decode + dispatch.
func (s *Service) HandleRideRequested(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeRideRequested(msg.Value)
	if err != nil {
		// Poison message: log and drop (do not retry forever).
		s.log.Error("dispatch: decode ride-requested", "err", err)
		return nil
	}
	res, err := s.eng.DispatchForRide(ctx, ev)
	if err != nil {
		return err // retryable
	}
	if s.m != nil {
		s.m.DispatchLatency.WithLabelValues(res.ResultCode).Observe(float64(res.TookNs) / 1e9)
		if res.Success {
			s.m.SuccessTotal.Inc()
		} else {
			s.m.FailureTotal.WithLabelValues(res.ResultCode).Inc()
		}
	}
	_ = time.Now()
	return nil
}