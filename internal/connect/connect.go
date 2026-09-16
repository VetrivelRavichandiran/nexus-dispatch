// Package connect builds the shared infrastructure clients (Redis, Postgres,
// Kafka) from configuration. Services call these in their main and wire the
// results into their service structs.
package connect

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nexus-dispatch/nexus-dispatch/internal/config"
	"github.com/nexus-dispatch/nexus-dispatch/internal/kafka"
	"github.com/nexus-dispatch/nexus-dispatch/internal/postgres"
	"github.com/nexus-dispatch/nexus-dispatch/internal/redis"
)

// Clients bundles the shared infrastructure clients.
type Clients struct {
	Redis    *redis.Store
	Postgres *postgres.Pool // nil if Postgres is unreachable (hot-only mode)
	Producer *kafka.Producer
	DLQ      *kafka.Producer
}

// Close shuts down all clients.
func (c *Clients) Close() {
	if c.Producer != nil {
		_ = c.Producer.Close()
	}
	if c.DLQ != nil {
		_ = c.DLQ.Close()
	}
	if c.Postgres != nil {
		c.Postgres.Close()
	}
	if c.Redis != nil {
		_ = c.Redis.RDB().Close()
	}
}

// Redis connects to Redis (required).
func Redis(cfg config.Config) (*redis.Store, error) {
	rdb := goredis.NewClient(&goredis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("connect: redis %s: %w", cfg.RedisAddr, err)
	}
	return redis.NewStore(rdb), nil
}

// Postgres connects to Postgres (optional: returns nil if unreachable).
func Postgres(ctx context.Context, cfg config.Config, log *slog.Logger) *postgres.Pool {
	if cfg.PostgresDSN == "" {
		return nil
	}
	pool, err := postgres.NewPool(ctx, postgres.Config{RawDSN: cfg.PostgresDSN})
	if err != nil {
		log.Warn("connect: postgres unavailable (hot-only mode)", "err", err)
		return nil
	}
	log.Info("connect: postgres ready")
	return pool
}

// KafkaProducer connects a Kafka producer (optional: nil if unreachable).
func KafkaProducer(ctx context.Context, cfg config.Config, log *slog.Logger) *kafka.Producer {
	prod, err := kafka.NewProducer(kafka.Config{Brokers: cfg.KafkaBrokers, ClientID: cfg.ServiceName})
	if err != nil {
		log.Warn("connect: kafka producer unavailable (events disabled)", "err", err)
		return nil
	}
	log.Info("connect: kafka producer ready", "brokers", cfg.KafkaBrokers)
	return prod
}
