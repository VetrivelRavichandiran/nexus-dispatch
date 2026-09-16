// location-service: GPS ingestion, validation, sequence stamping, H3 cell
// computation, Kafka publish, and Redis hot-state writes.
package main

import (
	"context"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"google.golang.org/grpc"

	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/connect"
	"github.com/nexus-dispatch/nexus-dispatch/internal/location"
	"github.com/nexus-dispatch/nexus-dispatch/internal/logging"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
	"github.com/nexus-dispatch/nexus-dispatch/internal/server"
)

func main() {
	cfg := config.Load("location-service")
	log := logging.New(cfg.ServiceName, 0)
	reg := metrics.NewRegistry()
	locMetrics := metrics.NewLocation(reg)
	_ = locMetrics

	ctx := context.Background()
	clients := &connect.Clients{}
	store, err := connect.Redis(cfg)
	if err != nil {
		log.Error("fatal: redis", "err", err)
		return
	}
	clients.Redis = store
	clients.Producer = connect.KafkaProducer(ctx, cfg, log)

	svc := location.NewService(store, clients.Producer, log)

	if err := server.Run(server.Deps{
		Config: cfg,
		Log:    log,
		Reg:    reg,
		RegisterGRPC: func(s *grpc.Server) {
			nexusv1.RegisterLocationServiceServer(s, svc)
		},
		OnShutdown: clients.Close,
	}); err != nil {
		log.Error("location-service exited", "err", err)
	}
}