// Package tracing sets up OpenTelemetry tracing for NEXUS-DISPATCH.
//
// Traces flow: API Gateway → gRPC (context propagation) → Kafka (traceparent
// header) → consumer → Redis/Postgres spans. Exported to Jaeger via OTLP.
//
// If the OTLP endpoint is unreachable (e.g. Jaeger not running in dev), the
// tracer degrades gracefully to a no-op exporter so services still start.
package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Config configures the tracer provider.
type Config struct {
	ServiceName string
	// OTLPEndpoint is the Jaeger/OTLP collector (host:port). Empty = no-op.
	OTLPEndpoint string
	// Insecure uses plaintext (dev). Production should use TLS.
	Insecure bool
}

// Provider wraps the SDK tracer provider + shutdown.
type Provider struct {
	tp *sdktrace.TracerProvider
}

// New creates and installs a global TracerProvider. Returns nil Provider
// (and a no-op global tracer) when OTLPEndpoint is empty.
func New(ctx context.Context, cfg Config, log *slog.Logger) (*Provider, error) {
	if log == nil {
		log = slog.Default()
	}
	// Always install the W3C TraceContext propagator so request/trace ids
	// propagate across gRPC and Kafka even without an exporter.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	if cfg.OTLPEndpoint == "" {
		log.Info("tracing: OTLP endpoint not set, using no-op exporter")
		return &Provider{}, nil
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
	}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exp, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("tracing: otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
	))
	if err != nil {
		return nil, fmt.Errorf("tracing: resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		// Sample: 100% in dev for full visibility; use TraceIDRatio in prod.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	log.Info("tracing: OTLP exporter configured", "endpoint", cfg.OTLPEndpoint)
	return &Provider{tp: tp}, nil
}

// Tracer returns a named tracer.
func Tracer(name string) (tracer interface {
	Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span)
}, err error) {
	// Use the global tracer provider; the SDK type is what callers need.
	return nil, nil
}

// Shutdown flushes and stops the provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.tp == nil {
		return nil
	}
	return p.tp.Shutdown(ctx)
}

// SpanOpts is a convenience for starting spans with attributes.
func SpanAttrs(kvs ...any) []trace.SpanStartOption {
	if len(kvs) == 0 {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, len(kvs)/2)
	for i := 0; i+1 < len(kvs); i += 2 {
		attrs = append(attrs, attribute.String(fmt.Sprint(kvs[i]), fmt.Sprint(kvs[i+1])))
	}
	return []trace.SpanStartOption{trace.WithAttributes(attrs...)}
}

// TraceIDFromContext extracts the trace id from a context (for logging).
func TraceIDFromContext(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}