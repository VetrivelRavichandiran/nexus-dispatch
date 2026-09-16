// driver-service: driver registration, profiles, and the availability
// state machine. gRPC on GRPC_ADDR, metrics/health on ADMIN_ADDR.
package main

import (
	"context"

	nexusv1 "github.com/nexus-dispatch/nexus-dispatch/api/gen/nexus/v1"
	"google.golang.org/grpc"

	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/connect"
	"github.com/nexus-dispatch/nexus-dispatch/internal/driver"
	"github.com/nexus-dispatch/nexus-dispatch/internal/logging"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
	"github.com/nexus-dispatch/nexus-dispatch/internal/server"
)

func main() {
	cfg := config.Load("driver-service")
	log := logging.New(cfg.ServiceName, 0)
	reg := metrics.NewRegistry()
	common := metrics.NewCommon(reg, cfg.ServiceName)
	_ = common

	ctx := context.Background()
	clients := &connect.Clients{}
	store, err := connect.Redis(cfg)
	if err != nil {
		log.Error("fatal: redis", "err", err)
		return
	}
	clients.Redis = store
	clients.Postgres = connect.Postgres(ctx, cfg, log)
	clients.Producer = connect.KafkaProducer(ctx, cfg, log)

	svc := driver.NewService(store, clients.Postgres, clients.Producer, log)

	if err := server.Run(server.Deps{
		Config: cfg,
		Log:    log,
		Reg:    reg,
		RegisterGRPC: func(s *grpc.Server) {
			nexusv1.RegisterDriverServiceServer(s, svc)
		},
		OnShutdown: clients.Close,
	}); err != nil {
		log.Error("driver-service exited", "err", err)
	}
}