// dispatch-engine: H3 candidate search, scoring, and atomic driver
// reservation. Consumes ride-requested from Kafka and exposes a gRPC
// DispatchService for on-demand dispatch.
package main

import (
	"context"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"google.golang.org/grpc"

	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/connect"
	"github.com/nexus-dispatch/nexus-dispatch/internal/dispatch"
	"github.com/nexus-dispatch/nexus-dispatch/internal/dispatchsvc"
	"github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/logging"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
	"github.com/nexus-dispatch/nexus-dispatch/internal/reservation"
	"github.com/nexus-dispatch/nexus-dispatch/internal/server"
)

func main() {
	cfg := config.Load("dispatch-engine")
	log := logging.New(cfg.ServiceName, 0)
	reg := metrics.NewRegistry()
	_ = metrics.NewCommon(reg, cfg.ServiceName)
	dispMetrics := metrics.NewDispatch(reg)

	ctx := context.Background()
	clients := &connect.Clients{}
	store, err := connect.Redis(cfg)
	if err != nil {
		log.Error("fatal: redis", "err", err)
		return
	}
	clients.Redis = store
	clients.Producer = connect.KafkaProducer(ctx, cfg, log)
	clients.DLQ = connect.KafkaProducer(ctx, cfg, log)

	resv := reservation.NewManager(store, cfg.LeaseTTL, cfg.LeaseRenew, log)
	eng := dispatch.NewEngine(store, resv, clients.Producer, dispatch.Config{
		DefaultRadiusM: float64(cfg.DefaultRadiusM),
	}, log)
	svc := dispatchsvc.NewService(eng, dispMetrics, log)

	// Kafka consumer: ride-requested → dispatch.
	var consumer *kafka.Consumer
	if clients.Producer != nil {
		consumer, err = kafka.NewConsumer(kafka.ConsumerConfig{
			Brokers:    cfg.KafkaBrokers,
			Topic:      kafka.TopicRideRequested,
			Group:      "dispatch",
			ClientID:   "dispatch-engine",
			Handler:    svc.HandleRideRequested,
			Logger:     log,
			DLQ:        clients.DLQ,
			MaxRetries: 3,
		})
		if err != nil {
			log.Error("fatal: kafka consumer", "err", err)
			return
		}
	}

	run := func() error {
		return server.Run(server.Deps{
			Config: cfg,
			Log:    log,
			Reg:    reg,
			RegisterGRPC: func(s *grpc.Server) {
				nexusv1.RegisterDispatchServiceServer(s, svc)
			},
			OnShutdown: clients.Close,
		})
	}

	if consumer == nil {
		log.Warn("dispatch-engine running without Kafka consumer (gRPC only)")
		if err := run(); err != nil {
			log.Error("dispatch-engine exited", "err", err)
		}
		return
	}

	// Run the consumer in a goroutine alongside the gRPC server.
	consumeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := consumer.Run(consumeCtx); err != nil && consumeCtx.Err() == nil {
			log.Error("kafka consumer stopped", "err", err)
		}
	}()
	log.Info("dispatch-engine consuming ride-requested")

	if err := run(); err != nil {
		log.Error("dispatch-engine exited", "err", err)
	}
}

// ensure redis import retained
var _ = redis.GeoKey