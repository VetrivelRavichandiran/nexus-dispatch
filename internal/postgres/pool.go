// Package postgres provides the durable-state layer (PostgreSQL + PostGIS).
//
// This is the system of record. It is NOT on the GPS hot path: location
// updates land here only as downsampled driver_history rows via the Event
// Processor (ADR-008).
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Config holds the Postgres connection settings.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DB       string
	SSLMode  string
	MaxConns int32
	// RawDSN, if set, is used verbatim (takes precedence over the fields).
	RawDSN string
}

// DSN builds a connection string.
func (c Config) DSN() string {
	if c.RawDSN != "" {
		return c.RawDSN
	}
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.SSLMode == "" {
		c.SSLMode = "disable"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DB, c.SSLMode)
}

// Pool wraps a pgx connection pool with observability hooks.
type Pool struct {
	p *pgxpool.Pool
}

// NewPool creates a connection pool.
func NewPool(ctx context.Context, cfg Config) (*Pool, error) {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 20
	}
	cc, err := pgx.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	pc := &pgxpool.Config{
		ConnConfig:        cc,
		MaxConns:          cfg.MaxConns,
		MaxConnLifetime:   30 * time.Minute,
		MaxConnIdleTime:   5 * time.Minute,
		HealthCheckPeriod: 1 * time.Minute,
		MinConns:          1,
	}
	p, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: pool: %w", err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Pool{p: p}, nil
}

// Raw exposes the underlying pool (for migrations / ops).
func (p *Pool) Raw() *pgxpool.Pool { return p.p }

// Close shuts down the pool.
func (p *Pool) Close() { p.p.Close() }

// Ping checks connectivity.
func (p *Pool) Ping(ctx context.Context) error { return p.p.Ping(ctx) }