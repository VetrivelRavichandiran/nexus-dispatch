# NEXUS-DISPATCH

**Distributed Real-Time Geo-Tracking & Intelligent Dispatch Engine**

A fault-tolerant, high-concurrency geo-dispatch platform for ride-hailing /
delivery: real-time driver tracking, sub-second proximity matching, atomic
driver reservation (no double-booking), event streaming, durable
persistence, real-time client push, and full observability.

Built in **Go** on **gRPC + Kafka + Redis + PostgreSQL/PostGIS + H3**.

---

## The core invariant

> **No driver is ever assigned to two active rides at the same time** — even
> when two users request the same driver from different API instances at the
> same instant.

This is enforced by an atomic Redis Lua lease (SETNX + TTL + re-entrant
renewal + compare-and-delete release) and verified by a 1000-goroutine
concurrency test in `tests/integration`. See
[ADR-006](docs/decisions/ADR-006-reservation-consistency.md).

---

## Architecture

```
                       ┌────────────────────────────────────────────┐
  Users / Drivers  ──▶ │  API Gateway :8080  (REST + WebSocket)     │
  (REST / WS)          │  JWT · rate-limit · request-id · WS hub    │
                       └──────┬───────────┬───────────┬─────────────┘
                              gRPC        gRPC        gRPC
             ┌──────────────────┐  ┌──────┴───────┐  ┌┴─────────────────┐
             │ Driver Service   │  │ Ride Service │  │ Location Service │
             │ :9001            │  │ :9002        │  │ :9003            │
             └────────┬─────────┘  └──────┬───────┘  └┬─────────────────┘
                      │                   │           │
                      ▼                   ▼           ▼
             ┌─────────────────────────────────────────────────────────┐
             │                        Kafka                            │
             │  driver-location · ride-requested · driver-reserved     │
             │  dispatch-created · ride-status · driver-status · DLQ   │
             └─────────────────────────────────────────────────────────┘
                      │                   │           │
        ┌─────────────┘                   │           └──────────────┐
        ▼                                 ▼                          ▼
┌─────────────────┐            ┌────────────────────┐      ┌────────────────────┐
│ Location        │            │ Dispatch Engine    │      │ Event Processor    │
│ Processor       │            │ :9004 (gRPC)       │      │ (persist)          │
│ (loc-proc)      │            │ H3 search + score  │      │ rides/dispatches → │
│ Redis hot state │            │ + atomic reserve   │      │ Postgres           │
└────────┬────────┘            └─────────┬──────────┘      └─────────┬──────────┘
         │                               │ gRPC AssignDriver         │
         ▼                               ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  Redis (hot state: H3 cells, geo zset, driver state, leases, rate limits)   │
│  PostgreSQL/PostGIS (durable: users, drivers, rides, dispatches, history)   │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Service | Port | Role |
|---------|------|------|
| **gateway** | 8080 (HTTP/WS) | Public API: JWT auth, rate limiting, REST→gRPC, WebSocket fan-out |
| **driver-service** | 9001 (gRPC) | Driver registration, profiles, availability state machine |
| **ride-service** | 9002 (gRPC) | Ride lifecycle, queries, cancellation |
| **location-service** | 9003 (gRPC) | GPS validation, sequence stamping, H3 cell, Kafka publish |
| **dispatch-engine** | 9004 (gRPC) | H3 candidate search, scoring, **atomic reservation**, driver notify |
| **location-processor** | — | Kafka consumer (`loc-proc`): Redis hot-state backstop, stale-driver detection |
| **event-processor** | — | Kafka consumer (`persist`): idempotent Postgres persistence |

Every service also exposes an admin HTTP port (metrics + `/healthz` +
`/readyz`): gateway 9180, driver 9101, ride 9102, location 9103, dispatch 9104,
event-processor 9105, location-processor 9106.

Full design rationale: [docs/architecture.md](docs/architecture.md) and the
[ADRs](docs/decisions/).

---

## Quick start (Docker — full stack)

Prereq: Docker with the Compose plugin.

```bash
docker compose up --build -d
docker compose logs -f gateway        # watch it come up
```

This starts Kafka (KRaft), Redis, PostgreSQL/PostGIS (migrations applied
automatically), and all seven services. The gateway is on `http://localhost:8080`.

### Demo: register a driver, request a ride

```bash
# 1. Log in as a driver and register them near Bengaluru (12.9716, 77.5946).
DTOKEN=$(curl -s -X POST localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"driver1@example.com","role":"driver"}' | jq -r .token)

curl -s -X POST localhost:8080/api/v1/drivers \
  -H "Authorization: Bearer $DTOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"Demo Driver","email":"driver1@example.com",
       "lat":12.9716,"lng":77.5946,
       "vehicle":{"make":"Toyota","model":"Corolla","plate":"KA-01-AB-1234","seats":4}}'

# 2. Log in as a user and request a ride ~50 m away.
UTOKEN=$(curl -s -X POST localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"user1@example.com","role":"user"}' | jq -r .token)

curl -s -X POST localhost:8080/api/v1/rides \
  -H "Authorization: Bearer $UTOKEN" -H 'Content-Type: application/json' \
  -d '{"pickup_lat":12.9720,"pickup_lng":77.5950,
       "dropoff_lat":12.9800,"dropoff_lng":77.6100,"radius_m":1500}'

# 3. Watch the dispatch happen (Kafka → dispatch-engine → driver notify → WS).
curl -s localhost:8080/api/v1/rides/<ride_id> -H "Authorization: Bearer $UTOKEN"
```

Or just run `make demo` after `make up`.

### WebSocket

```bash
# Connect (token from /auth/login). The gateway pushes:
#   ride.driver_assigned · ride.status · driver.status · driver.reserved
websocat "ws://localhost:8080/api/v1/ws?token=$UTOKEN"
```

---

## Local development (no Docker for services)

Prereqs: Go ≥ 1.25, Redis, Kafka, PostgreSQL 16 + PostGIS (or run only
Redis — every service degrades gracefully: Postgres/Kafka are optional).

```bash
cp .env.example .env          # adjust as needed
export $(grep -v '^#' .env | xargs)

make build                    # builds all 7 binaries into ./bin
./bin/ride-service &          # start services in any order
./bin/driver-service &
./bin/location-service &
./bin/dispatch-engine &
./bin/location-processor &
./bin/event-processor &
./bin/gateway                 # last: it dials the other services
```

Apply the schema to Postgres first:

```bash
psql "$POSTGRES_DSN" -f migrations/001_init.sql
```

---

## API reference

All endpoints are under `http://<gateway>:8080`. Authenticated routes take
`Authorization: Bearer <token>`.

| Method & path | Roles | Description |
|---|---|---|
| `POST /api/v1/auth/login` | — | Issue a JWT. Body: `{"email","role":"user\|driver\|admin"}` |
| `POST /api/v1/drivers` | driver, admin | Register a driver. Body: name, email, lat, lng, vehicle{make,model,plate,seats} |
| `PATCH /api/v1/drivers/{id}/status` | driver, admin | Transition state. Body: `{"state":"OFFLINE\|AVAILABLE\|PAUSED\|ON_TRIP"}` |
| `POST /api/v1/drivers/{id}/location` | driver, admin | Ingest a GPS update. Body: `{"lat","lng","seq"}` |
| `POST /api/v1/rides` | user, admin | Create a ride (triggers dispatch). Body: pickup/dropoff lat,lng, radius_m |
| `GET /api/v1/rides/{id}` | user, admin | Ride status |
| `POST /api/v1/rides/{id}/cancel` | user, admin | Cancel a ride. Body: `{"reason"}` |
| `GET /api/v1/drivers/nearby?lat&lng&radius_m&limit` | user, admin | H3 candidate search (no reservation) |
| `GET /api/v1/ws?token=…` | user, driver, admin | WebSocket event stream |
| `GET /health` · `GET /ready` · `GET /metrics` | — | Liveness / readiness / Prometheus |

State machines:

```
Driver:  OFFLINE ⇄ AVAILABLE ⇄ RESERVED ⇄ ON_TRIP
         (AVAILABLE/RESERVED/ON_TRIP → PAUSED → AVAILABLE)
Ride:    REQUESTED → MATCHED → DRIVER_EN_ROUTE → ARRIVED → IN_PROGRESS → COMPLETED
         (REQUESTED/MATCHED/DRIVER_EN_ROUTE/ARRIVED → CANCELLED; REQUESTED → EXPIRED)
```

---

## Testing

```bash
make test        # unit + integration (in-memory Redis via miniredis)
make test-race   # same, with the race detector
make vet         # go vet
```

The integration suite includes the **core invariant test**: 1000 goroutines
race to reserve the same driver for 1000 different rides; exactly one wins.
It also covers H3 cell-boundary correctness (drivers placed at every cell
edge are never missed) and the full dispatch pipeline (search → score →
reserve → notify).

```bash
go test ./tests/integration/ -run TestConcurrentReservationSingleWinner -v
```

---

## Configuration

All configuration is via environment variables (see [.env.example](.env.example)).
Key knobs:

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `REDIS_ADDR` | `localhost:6379` | Hot-state Redis |
| `POSTGRES_DSN` | `postgres://nexus:nexus@localhost:5432/nexus` | Durable store (optional) |
| `JWT_SECRET` / `JWT_EXPIRY` | dev secret / 24h | Auth |
| `DEFAULT_RADIUS_M` | `1000` | Dispatch search radius |
| `LEASE_TTL` / `LEASE_RENEW` | 30s / 10s | Reservation lease lifetime / renewal |
| `STALE_DRIVER_AFTER` | 90s | Quiet-driver → OFFLINE threshold |
| `USER_RATE_CAPACITY` / `IP_RATE_CAPACITY` | 60 / 300 | Token-bucket rate limits |
| `OTLP_ENDPOINT` | *(empty)* | Tracing collector (empty = disabled) |
| `*_SERVICE_GRPC_ADDR` | `localhost:90xx` | Backend gRPC addresses (gateway, dispatch) |

---

## Degraded modes (by design)

| Failure | Behavior |
|---|---|
| Postgres down | Services run **hot-only** (Redis is the source of truth); durable writes are best-effort and logged |
| Kafka down | Dispatch still works via gRPC; events are disabled (logged); WS bridge off |
| Redis down | Dispatch **fails closed** (never guesses); location ingestion returns errors |
| Driver-service down during dispatch | Reservation still wins (lease is authoritative); the gRPC notify is skipped and logged |

---

## Project layout

```
api/proto/        gRPC contracts (nexus.v1)
api/gen/          generated Go protobuf/gRPC code
cmd/              one main per service (7 binaries)
internal/
  auth/           JWT issue/verify, roles, context helpers
  config/         env-based configuration
  connect/        shared Redis/Postgres/Kafka client bootstrap
  dispatch/       dispatch pipeline (search → score → reserve → notify)
  dispatchsvc/    dispatch gRPC service + Kafka handler
  driver/         driver state machine service
  events/         domain event types + codecs
  gateway/        REST server, auth middleware, WS hub, gRPC clients
  geospatial/     H3 cells, k-ring search plans, haversine
  kafka/          topics, producer, consumer (retries + DLQ)
  location/       GPS ingestion service
  metrics/        Prometheus registry + per-service metrics
  postgres/       pgx pool + repositories
  redis/          hot-state store + atomic Lua scripts
  reservation/    lease manager (the no-double-booking primitive)
  ride/           ride lifecycle service
  server/         shared bootstrap (gRPC/HTTP/admin, graceful shutdown)
  tracing/        OpenTelemetry setup
migrations/       Postgres schema (001_init.sql)
tests/integration/ concurrency + boundary + pipeline tests
docs/             architecture.md + 10 ADRs
```