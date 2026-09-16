// Package server provides shared service bootstrap: config, logging,
// metrics, tracing, gRPC + HTTP servers, and graceful shutdown.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/logging"
	"github.com/nexus-dispatch/nexus-dispatch/internal/metrics"
	"github.com/nexus-dispatch/nexus-dispatch/internal/tracing"
)

// Deps are the wired dependencies a service registers on its servers.
type Deps struct {
	Config  config.Config
	Log     *slog.Logger
	Reg     *metrics.Registry
	Tracer  *tracing.Provider
	// RegisterGRPC registers gRPC services on the server (before serve).
	RegisterGRPC func(s *grpc.Server)
	// ExtraHTTP adds extra HTTP handlers (e.g. WebSocket upgrade).
	ExtraHTTP func(mux *http.ServeMux)
	// OnShutdown runs during graceful shutdown (close producers, etc.).
	OnShutdown func()
}

// Run bootstraps and runs a service until SIGINT/SIGTERM.
func Run(d Deps) error {
	cfg := d.Config
	log := d.Log
	if log == nil {
		log = logging.New(cfg.ServiceName, slog.LevelInfo)
	}
	d.Log = log

	// Tracing.
	_, err := tracing.New(context.Background(), tracing.Config{
		ServiceName:  cfg.ServiceName,
		OTLPEndpoint: cfg.OTLPEndpoint,
		Insecure:     true,
	}, log)
	if err != nil {
		log.Warn("tracing init failed (continuing without)", "err", err)
	}

	// gRPC server.
	grpcServer := grpc.NewServer()
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	if d.RegisterGRPC != nil {
		d.RegisterGRPC(grpcServer)
	}
	healthpb.RegisterHealthServer(grpcServer, healthSrv)

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("server: listen grpc %s: %w", cfg.GRPCAddr, err)
	}

	// Admin HTTP server (metrics + health).
	adminMux := http.NewServeMux()
	if d.Reg != nil {
		adminMux.Handle("/metrics", d.Reg.Handler())
	}
	adminMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	adminMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		healthSrv.Check(r.Context(), &healthpb.HealthCheckRequest{})
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ready")
	})
	adminListener, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		grpcServer.Stop()
		return fmt.Errorf("server: listen admin %s: %w", cfg.AdminAddr, err)
	}
	adminSrv := &http.Server{Handler: adminMux, ReadHeaderTimeout: 5 * time.Second}

	// Public HTTP server (gateway only uses this; others may too).
	var httpSrv *http.Server
	var httpListener net.Listener
	if cfg.HTTPAddr != "" && (d.ExtraHTTP != nil || d.RegisterGRPC != nil) {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "ok")
		})
		if d.Reg != nil {
			mux.Handle("/metrics", d.Reg.Handler())
		}
		if d.ExtraHTTP != nil {
			d.ExtraHTTP(mux)
		}
		httpListener, err = net.Listen("tcp", cfg.HTTPAddr)
		if err != nil {
			grpcServer.Stop()
			return fmt.Errorf("server: listen http %s: %w", cfg.HTTPAddr, err)
		}
		httpSrv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	}

	log.Info("service starting",
		"service", cfg.ServiceName, "grpc", cfg.GRPCAddr,
		"http", cfg.HTTPAddr, "admin", cfg.AdminAddr)

	errCh := make(chan error, 3)
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Warn("grpc server stopped", "err", err)
		}
	}()
	go func() { errCh <- adminSrv.Serve(adminListener) }()
	if httpSrv != nil {
		go func() { errCh <- httpSrv.Serve(httpListener) }()
	}

	// Wait for signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(ctx)
	}
	_ = adminSrv.Shutdown(ctx)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	grpcServer.GracefulStop()
	if d.Tracer != nil {
		_ = d.Tracer.Shutdown(ctx)
	}
	if d.OnShutdown != nil {
		d.OnShutdown()
	}
	log.Info("service stopped", "service", cfg.ServiceName)
	return nil
}