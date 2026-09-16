// Package logging provides structured JSON logging with request/trace
// correlation. Every log line carries service + (when available) request_id
// and trace_id. Secrets are never logged (callers must not pass them).
package logging

import (
	"log/slog"
	"os"
)

// New builds a structured JSON logger for a service.
func New(service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}

// FromContext extracts request_id / trace_id from context values set by
// middleware and returns a child logger.
func FromContext(base *slog.Logger, get func(key string) string) *slog.Logger {
	if get == nil {
		return base
	}
	if rid := get("request_id"); rid != "" {
		base = base.With("request_id", rid)
	}
	if tid := get("trace_id"); tid != "" {
		base = base.With("trace_id", tid)
	}
	return base
}