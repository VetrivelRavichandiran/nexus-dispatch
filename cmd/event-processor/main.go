// event-processor: the durable persistence consumer. It applies ride-status,
// dispatch-created, and driver-status events to PostgreSQL idempotently, and
// downsamples driver-location events into driver_history.
//
// This is the ONLY writer of high-volume data to Postgres (ADR-008): GPS
// updates are downsampled to 1 point / 5 s and batched.
package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"

	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/connect"
	"github.com/nexus-dispatch/nexus-dispatch/internal/events"
	nkafka "github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/logging"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
	"github.com/nexus-dispatch/nexus-dispatch/internal/postgres"
)

// Processor persists events to Postgres.
type Processor struct {
	db  *postgres.Pool
	log *slog.Logger
	// lastHistoryTs downsamples driver_history to 1 point / 5 s per driver.
	mu            sync.Mutex
	lastHistoryTs map[string]time.Time
}

func main() {
	cfg := config.Load("event-processor")
	log := logging.New(cfg.ServiceName, 0)
	reg := metrics.NewRegistry()
	_ = metrics.NewCommon(reg, cfg.ServiceName)

	ctx := context.Background()
	clients := &connect.Clients{}
	clients.Postgres = connect.Postgres(ctx, cfg, log)
	if clients.Postgres == nil {
		log.Error("fatal: event-processor requires Postgres")
		return
	}
	clients.DLQ = connect.KafkaProducer(ctx, cfg, log)

	p := &Processor{
		db:            clients.Postgres,
		log:           log,
		lastHistoryTs: make(map[string]time.Time),
	}

	// Consume the topics we persist.
	topics := []struct {
		topic   string
		handler func(context.Context, kafka.Message) error
	}{
		{nkafka.TopicDispatchCreated, p.handleDispatchCreated},
		{nkafka.TopicRideStatus, p.handleRideStatus},
		{nkafka.TopicDriverStatus, p.handleDriverStatus},
		{nkafka.TopicDriverLocation, p.handleDriverLocation},
	}
	for _, tc := range topics {
		c, err := nkafka.NewConsumer(nkafka.ConsumerConfig{
			Brokers:    cfg.KafkaBrokers,
			Topic:      tc.topic,
			Group:      "persist",
			ClientID:   "event-processor",
			Handler:    tc.handler,
			Logger:     log,
			DLQ:        clients.DLQ,
			MaxRetries: 3,
		})
		if err != nil {
			log.Error("fatal: consumer", "topic", tc.topic, "err", err)
			return
		}
		go func(c *nkafka.Consumer, topic string) {
			if err := c.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("consumer stopped", "topic", topic, "err", err)
			}
		}(c, tc.topic)
		log.Info("event-processor consuming", "topic", tc.topic)
	}

	log.Info("event-processor running")
	// Block forever (consumers run in goroutines).
	select {}
}

// handleDispatchCreated persists the dispatch record (the durable backstop).
func (p *Processor) handleDispatchCreated(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeDispatchCreated(msg.Value)
	if err != nil {
		p.log.Error("decode dispatch-created", "err", err)
		return nil // poison: drop
	}
	start := time.Now()
	err = p.db.InsertDispatch(ctx, ev.DispatchID, ev.RideID, ev.DriverID,
		ev.DistanceM, ev.Score, 0, 0, 0)
	if err != nil {
		p.log.Error("insert dispatch", "err", err)
		return err // retryable
	}
	p.log.Debug("dispatch persisted", "dispatch_id", ev.DispatchID, "took_ms", time.Since(start).Milliseconds())
	return nil
}

// handleRideStatus persists ride state transitions + lifecycle events.
func (p *Processor) handleRideStatus(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeRideStatus(msg.Value)
	if err != nil {
		p.log.Error("decode ride-status", "err", err)
		return nil
	}
	start := time.Now()
	if err := p.db.UpdateRideState(ctx, ev.RideID, ev.State, ev.DriverID, ev.Reason); err != nil {
		p.log.Error("update ride state", "err", err)
		return err
	}
	eventID := ev.EventID
	if eventID == "" {
		eventID = "rs-" + ev.RideID + "-" + ev.State
	}
	if _, err := p.db.InsertRideEvent(ctx, ev.RideID, ev.State, ev.DriverID, ev.Reason, eventID); err != nil {
		p.log.Error("insert ride event", "err", err)
		return err
	}
	p.log.Debug("ride status persisted", "ride_id", ev.RideID, "state", ev.State, "took_ms", time.Since(start).Milliseconds())
	return nil
}

// handleDriverStatus persists driver state transitions.
func (p *Processor) handleDriverStatus(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeDriverStatus(msg.Value)
	if err != nil {
		p.log.Error("decode driver-status", "err", err)
		return nil
	}
	if err := p.db.UpdateDriverState(ctx, ev.DriverID, ev.State); err != nil {
		p.log.Error("update driver state", "err", err)
		return err
	}
	return nil
}

// handleDriverLocation downsamples GPS points into driver_history (1/5 s).
func (p *Processor) handleDriverLocation(ctx context.Context, msg kafka.Message) error {
	ev, err := events.DecodeLocationUpdate(msg.Value)
	if err != nil {
		p.log.Error("decode location", "err", err)
		return nil
	}
	now := time.Now()
	p.mu.Lock()
	last, ok := p.lastHistoryTs[ev.DriverID]
	if ok && now.Sub(last) < 5*time.Second {
		p.mu.Unlock()
		return nil // downsample: skip
	}
	p.lastHistoryTs[ev.DriverID] = now
	p.mu.Unlock()

	if err := p.db.InsertDriverHistory(ctx, ev.DriverID, now, ev.Latitude, ev.Longitude, ev.Sequence); err != nil {
		p.log.Error("insert driver history", "err", err)
		return err
	}
	return nil
}