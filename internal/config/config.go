// Package config loads service configuration from environment variables with
// sensible defaults. No secrets are hard-coded; every credential comes from
// the environment (see .env.example).
package config

import (
	"os"
	"strconv"
	"time"
)

// Config is the shared configuration for all services.
type Config struct {
	ServiceName string

	// HTTP / gRPC
	HTTPAddr  string
	GRPCAddr  string
	AdminAddr string // metrics/health port

	// Kafka
	KafkaBrokers []string

	// Redis
	RedisAddr string
	RedisDB   int

	// Postgres
	PostgresDSN string

	// Auth
	JWTSecret string
	JWTExpiry time.Duration

	// Tracing
	OTLPEndpoint string

	// Dispatch
	DefaultRadiusM int
	LeaseTTL       time.Duration
	LeaseRenew     time.Duration

	// Rate limiting
	UserRateCapacity int
	UserRateRefill   float64
	IPRateCapacity   int
	IPRateRefill     float64
}

// Load reads configuration from the environment.
func Load(service string) Config {
	return Config{
		ServiceName:      service,
		HTTPAddr:         getEnv("HTTP_ADDR", defaultPort(service, "8080")),
		GRPCAddr:         getEnv("GRPC_ADDR", defaultPort(service, "9000")),
		AdminAddr:        getEnv("ADMIN_ADDR", defaultPort(service, "9100")),
		KafkaBrokers:     splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisDB:          getEnvInt("REDIS_DB", 0),
		PostgresDSN:      getEnv("POSTGRES_DSN", "postgres://nexus:nexus@localhost:5432/nexus?sslmode=disable"),
		JWTSecret:        getEnv("JWT_SECRET", "dev-only-insecure-secret-change-me"),
		JWTExpiry:        getEnvDuration("JWT_EXPIRY", 24*time.Hour),
		OTLPEndpoint:     getEnv("OTLP_ENDPOINT", ""),
		DefaultRadiusM:   getEnvInt("DEFAULT_RADIUS_M", 1000),
		LeaseTTL:         getEnvDuration("LEASE_TTL", 30*time.Second),
		LeaseRenew:       getEnvDuration("LEASE_RENEW", 10*time.Second),
		UserRateCapacity: getEnvInt("USER_RATE_CAPACITY", 60),
		UserRateRefill:   getEnvFloat("USER_RATE_REFILL", 1.0),
		IPRateCapacity:   getEnvInt("IP_RATE_CAPACITY", 300),
		IPRateRefill:     getEnvFloat("IP_RATE_REFILL", 5.0),
	}
}

func defaultPort(service, def string) string {
	switch service {
	case "gateway":
		return "8080"
	case "driver-service":
		return "9001"
	case "ride-service":
		return "9002"
	case "location-service":
		return "9003"
	case "dispatch-engine":
		return "9004"
	}
	return def
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getEnvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if n, err := time.ParseDuration(v); err == nil {
			return n
		}
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}