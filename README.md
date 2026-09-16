# 🚀 NEXUS-DISPATCH

### Distributed Real-Time Geo-Tracking & Intelligent Dispatch Engine

> **A fault-tolerant, high-concurrency dispatch platform engineered for real-time ride-hailing and delivery workloads.**

NEXUS-DISPATCH is a **production-oriented distributed dispatch system** designed to solve one of the hardest problems in real-time mobility platforms:

> **How do you reliably find, score, reserve, and notify the right driver — at scale — without ever double-booking the same driver?**

The platform combines **real-time GPS tracking, H3 geospatial indexing, intelligent candidate scoring, atomic driver reservation, Kafka event streaming, Redis hot state, PostgreSQL/PostGIS persistence, WebSockets, observability, and fault-tolerant service design** into a cohesive distributed architecture.

Built with:

**Go · gRPC · Apache Kafka · Redis · PostgreSQL/PostGIS · H3 · WebSockets · Prometheus · OpenTelemetry**

---

## ✨ Why NEXUS-DISPATCH?

Traditional CRUD-based ride-hailing prototypes often break down when concurrency, stale GPS data, distributed services, and infrastructure failures enter the picture.

NEXUS-DISPATCH is designed around those problems from the beginning.

### 🎯 Core Engineering Goals

| Challenge                        | NEXUS-DISPATCH Approach                        |
| -------------------------------- | ---------------------------------------------- |
| Driver double-booking            | Atomic Redis Lua reservation lease             |
| Fast nearby-driver discovery     | H3 spatial indexing + k-ring search            |
| Stale GPS updates                | Per-driver monotonic sequence validation       |
| High-throughput event processing | Kafka event-driven architecture                |
| Durable business state           | PostgreSQL/PostGIS                             |
| Ultra-fast operational state     | Redis                                          |
| Real-time client updates         | WebSocket event hub                            |
| Service failures                 | Explicit degraded-mode behavior                |
| Concurrent race conditions       | 1000-goroutine integration test                |
| Production observability         | Prometheus + OpenTelemetry                     |
| API protection                   | JWT + role-based authorization + rate limiting |

---

# 🧠 The Core Invariant

NEXUS-DISPATCH is built around a critical correctness guarantee:

> ## 🔒 No driver can ever be assigned to two active rides at the same time.

Even if:

* multiple users request rides simultaneously,
* multiple gateway instances process requests,
* multiple dispatch workers race for the same driver,
* requests arrive concurrently,
* or the same driver is targeted by hundreds of requests,

the reservation layer allows **at most one successful reservation**.

The guarantee is implemented using an **atomic Redis Lua lease** with:

* `SETNX`
* TTL expiration
* re-entrant renewal
* compare-and-delete release

The invariant is continuously validated by a **1000-goroutine concurrency test**, where 1000 rides compete for the same driver and exactly one reservation succeeds.

---

# ⚡ Key Features

### 🔐 Atomic Driver Reservation

Distributed-safe reservation prevents double-booking across multiple API/service instances.

```text
Ride Request
     │
     ▼
Candidate Search
     │
     ▼
Candidate Scoring
     │
     ▼
Atomic Redis Lease
     │
 ┌───┴────┐
 │        │
WIN      LOSS
 │        │
 ▼        ▼
Assign   Try Next
```

The lease also supports TTL-based expiry and controlled renewal/release.

---

### 🌍 H3-Powered Geospatial Dispatch

Drivers are indexed using **H3 resolution 9 cells**.

Instead of scanning every driver globally, the dispatch engine:

1. Converts the pickup location into an H3 cell.
2. Generates a surrounding k-ring.
3. Retrieves nearby candidates.
4. Filters unsuitable drivers.
5. Calculates candidate scores.
6. Attempts atomic reservation.
7. Notifies the winning driver.

Candidate ranking considers:

**Distance + Request Age + Driver Rating**

This provides a scalable spatial-search strategy instead of relying on expensive full-dataset distance scans.

---

### 📍 Out-of-Order GPS Protection

Real-world mobile networks can deliver location updates late or out of order.

NEXUS-DISPATCH protects driver state using **per-driver monotonic sequence numbers**.

```text
seq=101  ──► ACCEPT
seq=102  ──► ACCEPT
seq=103  ──► ACCEPT
seq=101  ──► REJECT ❌
seq=102  ──► REJECT ❌
seq=104  ──► ACCEPT
```

A Redis atomic guard prevents stale or duplicate location updates from overwriting newer state.

---

### 📨 Event-Driven Architecture

Kafka acts as the event backbone connecting the distributed services.

Representative topics include:

```text
driver-location
ride-requested
driver-reserved
dispatch-created
ride-status
driver-status
DLQ
```

The event pipeline supports:

* asynchronous processing
* retries
* durable event flow
* dead-letter handling
* persistence consumers
* service decoupling

---

### ⚡ Redis Hot State + PostgreSQL Durable State

The system separates **fast operational state** from **durable business state**.

```text
                    NEXUS-DISPATCH
                           │
             ┌─────────────┴─────────────┐
             │                           │
          REDIS                       POSTGRES
        Hot State                   Durable State
             │                           │
     ┌───────┼────────┐          ┌───────┼────────┐
     │       │        │          │       │        │
    H3    Driver    Leases      Users   Rides  History
   Cells   State              Dispatches
```

Redis handles hot operational data such as:

* H3 cells
* driver state
* geo indexes
* reservation leases
* rate limits

PostgreSQL/PostGIS stores:

* users
* drivers
* rides
* dispatches
* downsampled driver history

The services are intentionally designed to degrade gracefully when infrastructure components become unavailable.

---

### 📡 Real-Time WebSocket Updates

Connected users and drivers receive real-time events through the gateway WebSocket hub.

Supported events include:

```text
ride.driver_assigned
ride.status
driver.status
driver.reserved
```

This enables clients to react to dispatch and ride-state changes without continuously polling the REST API.

---

# 🏗️ System Architecture

```text
                           ┌───────────────────────────────┐
                           │        USERS / DRIVERS        │
                           │       REST API / WebSocket    │
                           └───────────────┬───────────────┘
                                           │
                                           ▼
                    ┌─────────────────────────────────────────┐
                    │             API GATEWAY :8080           │
                    │                                         │
                    │ JWT • RBAC • Rate Limit • Request ID   │
                    │ REST → gRPC • WebSocket Event Hub      │
                    └───────────────┬─────────────────────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
               gRPC               gRPC               gRPC
                 │                  │                  │
        ┌────────▼───────┐ ┌────────▼───────┐ ┌──────▼────────────┐
        │ DRIVER SERVICE │ │ RIDE SERVICE   │ │ LOCATION SERVICE  │
        │     :9001      │ │     :9002      │ │      :9003        │
        └────────┬───────┘ └────────┬───────┘ └──────┬────────────┘
                 │                  │                │
                 └──────────────────┼────────────────┘
                                    │
                                    ▼
                     ┌──────────────────────────────┐
                     │            KAFKA             │
                     │                              │
                     │ Location • Ride • Reservation│
                     │ Dispatch • Status • DLQ      │
                     └──────────────┬───────────────┘
                                    │
                 ┌──────────────────┼──────────────────┐
                 │                  │                  │
                 ▼                  ▼                  ▼
       ┌─────────────────┐ ┌──────────────────┐ ┌─────────────────┐
       │ LOCATION        │ │ DISPATCH ENGINE  │ │ EVENT PROCESSOR │
       │ PROCESSOR       │ │      :9004       │ │                 │
       │                 │ │                  │ │ Kafka → DB      │
       │ Redis hot state │ │ H3 + Score       │ │ Idempotent      │
       │ Stale detection │ │ Atomic Reserve   │ │ Persistence     │
       └────────┬────────┘ └────────┬─────────┘ └────────┬────────┘
                │                   │                  │
                └───────────────────┼──────────────────┘
                                    │
                    ┌───────────────┴────────────────┐
                    │                                │
                    ▼                                ▼
          ┌──────────────────┐             ┌────────────────────┐
          │      REDIS       │             │ POSTGRESQL/POSTGIS │
          │                  │             │                    │
          │ Hot State        │             │ Durable State      │
          │ H3 Index         │             │ Users              │
          │ Geo ZSets        │             │ Drivers            │
          │ Driver State     │             │ Rides              │
          │ Leases           │             │ Dispatches          │
          │ Rate Limits      │             │ Driver History      │
          └──────────────────┘             └────────────────────┘
```

---

# 🧩 Microservices

| Service                |   Port | Responsibility                                                     |
| ---------------------- | -----: | ------------------------------------------------------------------ |
| **Gateway**            | `8080` | REST API, JWT, RBAC, rate limiting, WebSockets                     |
| **Driver Service**     | `9001` | Driver registration and availability state machine                 |
| **Ride Service**       | `9002` | Ride lifecycle, queries, cancellation                              |
| **Location Service**   | `9003` | GPS validation, sequencing, H3 indexing, Kafka publishing          |
| **Dispatch Engine**    | `9004` | Candidate search, scoring, reservation, notification               |
| **Location Processor** |      — | Kafka location consumer, Redis hot-state backstop, stale detection |
| **Event Processor**    |      — | Kafka consumer and idempotent PostgreSQL persistence               |

Every service exposes an administrative HTTP endpoint for:

```text
/healthz
/readyz
/metrics
```

Admin ports:

```text
Gateway             9180
Driver Service      9101
Ride Service        9102
Location Service    9103
Dispatch Engine     9104
Event Processor     9105
Location Processor  9106
```

---

# 🔄 End-to-End Dispatch Flow

```text
1. Driver sends GPS update
             │
             ▼
2. Location Service validates sequence
             │
             ▼
3. H3 cell calculated
             │
             ▼
4. Location event published to Kafka
             │
             ▼
5. Redis hot state updated
             │
             ▼
6. User requests ride
             │
             ▼
7. Ride Service creates REQUESTED ride
             │
             ▼
8. Dispatch Engine searches H3 k-ring
             │
             ▼
9. Candidates are scored
             │
             ▼
10. Atomic Redis reservation attempted
             │
       ┌─────┴─────┐
       │            │
     SUCCESS       FAIL
       │            │
       ▼            ▼
  Driver reserved  Next candidate
       │
       ▼
11. Driver notification
       │
       ▼
12. Dispatch event published
       │
       ▼
13. WebSocket event pushed
       │
       ▼
14. Event Processor persists state
```

---

# 🧭 State Machines

### Driver Lifecycle

```text
                  ┌───────────────┐
                  │    OFFLINE    │
                  └───────┬───────┘
                          │
                          ▼
                  ┌───────────────┐
            ┌────►│   AVAILABLE   │◄────┐
            │     └───────┬───────┘     │
            │             │              │
            │             ▼              │
            │     ┌───────────────┐      │
            │     │   RESERVED    │      │
            │     └───────┬───────┘      │
            │             │              │
            │             ▼              │
            │     ┌───────────────┐      │
            │     │    ON_TRIP     │──────┘
            │     └───────────────┘
            │
            ▼
       ┌───────────┐
       │  PAUSED   │
       └─────┬─────┘
             │
             └──────────► AVAILABLE
```

### Ride Lifecycle

```text
REQUESTED
    │
    ▼
 MATCHED
    │
    ▼
DRIVER_EN_ROUTE
    │
    ▼
 ARRIVED
    │
    ▼
IN_PROGRESS
    │
    ▼
 COMPLETED
```

Cancellation is supported from appropriate intermediate states, while requested rides can also expire.

---

# 🛡️ Reliability & Failure Handling

NEXUS-DISPATCH does not assume that infrastructure will always be healthy.

Instead, failure behavior is explicitly defined.

| Failure                        | System Behavior                                                                   |
| ------------------------------ | --------------------------------------------------------------------------------- |
| **PostgreSQL unavailable**     | Services continue in hot-only mode using Redis; durable writes become best-effort |
| **Kafka unavailable**          | gRPC dispatch continues; event streaming and WS bridge are disabled/logged        |
| **Redis unavailable**          | Dispatch fails closed rather than making unsafe reservation decisions             |
| **Driver Service unavailable** | Reservation remains authoritative; driver notification is skipped and logged      |

This makes failure behavior **predictable rather than accidental**.

---

# 🧪 Concurrency & Correctness Testing

The test suite is designed to validate the system's most important correctness properties.

### 🔥 1000-Way Reservation Race

```text
                    SAME DRIVER
                         │
          ┌──────────────┼──────────────┐
          │              │              │
        Ride 1         Ride 2         Ride 3
          │              │              │
          ├──────────────┼──────────────┤
          │              │              │
          ▼              ▼              ▼
                1000 CONCURRENT
                  GOROUTINES
                       │
                       ▼
              ATOMIC REDIS LEASE
                       │
                ┌──────┴──────┐
                │             │
              ONE WIN       999 LOSE
```

The integration suite verifies:

* concurrent reservation correctness
* H3 cell-boundary correctness
* full dispatch pipeline
* candidate search
* candidate scoring
* reservation
* driver notification

Run the core invariant test:

```bash
go test ./tests/integration/ \
  -run TestConcurrentReservationSingleWinner -v
```

---

# 📊 Observability

The platform includes production-oriented observability primitives:

* Prometheus metrics
* OpenTelemetry tracing hooks
* Health endpoints
* Readiness endpoints
* Request IDs
* Structured service architecture
* Graceful shutdown
* Retry and DLQ mechanisms

This makes the system easier to monitor, diagnose, and operate in distributed environments.

---

# 🔐 Security

The gateway provides:

* JWT authentication
* Role-based authorization
* Redis-backed token-bucket rate limiting
* Request correlation IDs
* Secure HTTP headers

Supported roles:

```text
user
driver
admin
```

---

# 🚀 Quick Start

## Prerequisites

Recommended:

```text
Docker
Docker Compose
```

For local service development:

```text
Go ≥ 1.25
Redis
Kafka
PostgreSQL 16
PostGIS
```

---

## 🐳 Run the Complete Stack

The recommended approach is Docker Compose:

```bash
docker compose up --build -d
```

Watch the gateway:

```bash
docker compose logs -f gateway
```

The stack launches:

```text
Kafka (KRaft)
Redis
PostgreSQL/PostGIS
Database migrations
Gateway
Driver Service
Ride Service
Location Service
Dispatch Engine
Location Processor
Event Processor
```

Gateway:

```text
http://localhost:8080
```

---

# 🪟 Windows / PowerShell

Use:

```powershell
docker compose up --build -d
```

For HTTP requests, prefer `curl.exe` instead of the PowerShell `curl` alias.

### 1. Login as Driver

```powershell
$token = (curl.exe -s -X POST localhost:8080/api/v1/auth/login `
  -H 'Content-Type: application/json' `
  -d '{"email":"driver1@example.com","role":"driver"}' |
  ConvertFrom-Json).token
```

### 2. Register Driver

```powershell
curl.exe -s -X POST localhost:8080/api/v1/drivers `
  -H "Authorization: Bearer $token" `
  -H 'Content-Type: application/json' `
  -d '{"name":"Demo Driver","email":"driver1@example.com",
  "lat":12.9716,"lng":77.5946,
  "vehicle":{"make":"Toyota","model":"Corolla",
  "plate":"KA-01-AB-1234","seats":4}}'
```

### 3. Login as User

```powershell
$utoken = (curl.exe -s -X POST localhost:8080/api/v1/auth/login `
  -H 'Content-Type: application/json' `
  -d '{"email":"user1@example.com","role":"user"}' |
  ConvertFrom-Json).token
```

### 4. Request a Ride

```powershell
curl.exe -s -X POST localhost:8080/api/v1/rides `
  -H "Authorization: Bearer $utoken" `
  -H 'Content-Type: application/json' `
  -d '{"pickup_lat":12.9720,"pickup_lng":77.5950,
  "dropoff_lat":12.9800,"dropoff_lng":77.6100,
  "radius_m":1500}'
```

### 5. Inspect the Ride

```powershell
curl.exe -s localhost:8080/api/v1/rides/<ride_id> `
  -H "Authorization: Bearer $utoken"
```

---

# 📡 WebSocket Streaming

Connect using the authenticated token:

```bash
websocat "ws://localhost:8080/api/v1/ws?token=$UTOKEN"
```

The gateway can stream:

```text
ride.driver_assigned
ride.status
driver.status
driver.reserved
```

---

# 💻 Local Development

Copy the environment template:

```bash
cp .env.example .env
```

Build all services:

```bash
make build
```

The binaries are generated under:

```text
./bin/
```

Start the services:

```bash
./bin/ride-service &
./bin/driver-service &
./bin/location-service &
./bin/dispatch-engine &
./bin/location-processor &
./bin/event-processor &
./bin/gateway
```

Apply the database schema:

```bash
psql "$POSTGRES_DSN" -f migrations/001_init.sql
```

PostgreSQL and Kafka are optional during certain degraded local-development scenarios because the services are designed to degrade gracefully; Redis remains important for the hot-state and reservation paths.

---

# 🔌 API Reference

All APIs are exposed through:

```text
http://localhost:8080
```

Authenticated requests use:

```http
Authorization: Bearer <token>
```

| Method  | Endpoint                        | Roles               | Purpose                 |
| ------- | ------------------------------- | ------------------- | ----------------------- |
| `POST`  | `/api/v1/auth/login`            | —                   | Issue JWT               |
| `POST`  | `/api/v1/drivers`               | driver, admin       | Register driver         |
| `PATCH` | `/api/v1/drivers/{id}/status`   | driver, admin       | Update driver state     |
| `POST`  | `/api/v1/drivers/{id}/location` | driver, admin       | Submit GPS update       |
| `POST`  | `/api/v1/rides`                 | user, admin         | Create ride             |
| `GET`   | `/api/v1/rides/{id}`            | user, admin         | Retrieve ride           |
| `POST`  | `/api/v1/rides/{id}/cancel`     | user, admin         | Cancel ride             |
| `GET`   | `/api/v1/drivers/nearby`        | user, admin         | H3 nearby-driver search |
| `GET`   | `/api/v1/ws`                    | user, driver, admin | WebSocket event stream  |
| `GET`   | `/health`                       | —                   | Health check            |
| `GET`   | `/ready`                        | —                   | Readiness check         |
| `GET`   | `/metrics`                      | —                   | Prometheus metrics      |

---

# ⚙️ Configuration

Configuration is environment-variable driven.

| Variable              | Default                                       | Purpose                      |
| --------------------- | --------------------------------------------- | ---------------------------- |
| `KAFKA_BROKERS`       | `localhost:9092`                              | Kafka brokers                |
| `REDIS_ADDR`          | `localhost:6379`                              | Redis hot-state store        |
| `POSTGRES_DSN`        | `postgres://nexus:nexus@localhost:5432/nexus` | Durable database             |
| `JWT_SECRET`          | dev secret                                    | Authentication               |
| `JWT_EXPIRY`          | `24h`                                         | JWT lifetime                 |
| `DEFAULT_RADIUS_M`    | `1000`                                        | Dispatch radius              |
| `LEASE_TTL`           | `30s`                                         | Reservation lease lifetime   |
| `LEASE_RENEW`         | `10s`                                         | Reservation renewal interval |
| `STALE_DRIVER_AFTER`  | `90s`                                         | Stale-driver threshold       |
| `USER_RATE_CAPACITY`  | `60`                                          | User rate-limit capacity     |
| `IP_RATE_CAPACITY`    | `300`                                         | IP rate-limit capacity       |
| `OTLP_ENDPOINT`       | empty                                         | OpenTelemetry collector      |
| `*_SERVICE_GRPC_ADDR` | `localhost:90xx`                              | Internal service addresses   |

See `.env.example` for the complete configuration surface.

---

# 🗂️ Project Structure

```text
NEXUS-DISPATCH/
│
├── api/
│   ├── proto/              # gRPC contracts
│   └── gen/                # Generated protobuf/gRPC code
│
├── cmd/                    # Service entry points
│
├── internal/
│   ├── auth/               # JWT + roles
│   ├── config/             # Environment configuration
│   ├── connect/            # Redis/Postgres/Kafka bootstrap
│   ├── dispatch/           # Dispatch pipeline
│   ├── dispatchsvc/        # Dispatch gRPC service
│   ├── driver/             # Driver state machine
│   ├── events/             # Domain events
│   ├── gateway/            # REST + WebSocket gateway
│   ├── geospatial/         # H3 + geospatial algorithms
│   ├── kafka/              # Kafka producer/consumer
│   ├── location/           # GPS ingestion
│   ├── metrics/             # Prometheus metrics
│   ├── postgres/            # PostgreSQL repositories
│   ├── redis/               # Redis state + Lua scripts
│   ├── reservation/         # Atomic lease manager
│   ├── ride/                # Ride lifecycle
│   ├── server/              # HTTP/gRPC bootstrap
│   └── tracing/             # OpenTelemetry
│
├── migrations/
│   └── 001_init.sql
│
├── tests/
│   └── integration/         # Concurrency + pipeline tests
│
└── docs/
    ├── architecture.md
    └── decisions/           # Architecture Decision Records
```

The repository contains dedicated modules for dispatch, geospatial indexing, Kafka, Redis, reservation management, ride lifecycle, observability, persistence, and integration testing.

---

# 🧪 Testing & Quality

Run the complete test suite:

```bash
make test
```

Run with Go's race detector:

```bash
make test-race
```

Run static analysis:

```bash
make vet
```

Or run the critical concurrency test directly:

```bash
go test ./tests/integration/ \
  -run TestConcurrentReservationSingleWinner -v
```

---

# 🏛️ Architecture Decisions

Major architectural decisions are documented as ADRs.

```text
docs/
└── decisions/
    ├── ADR-001-...
    ├── ADR-002-...
    ├── ...
    └── ADR-006-reservation-consistency.md
```

The repository currently includes **10 architecture decision records**, including the reservation-consistency design.

---

# 📈 Engineering Highlights

### ⚡ High Concurrency

Designed for concurrent ride requests and driver reservations across distributed service instances.

### 🌐 Geospatial Intelligence

H3-based spatial indexing avoids naïve global driver scans and enables localized candidate discovery.

### 🔒 Strong Consistency Where It Matters

The reservation primitive prioritizes correctness:

> **If the system cannot safely determine reservation ownership, it fails closed rather than guessing.**

### 📨 Event-Driven Scalability

Kafka separates high-throughput asynchronous workflows from synchronous request handling.

### 💾 Dual-Layer State Model

Redis provides low-latency operational state while PostgreSQL/PostGIS provides durable persistence.

### 📡 Real-Time UX

WebSockets allow clients to receive dispatch and ride-state events immediately.

### 🔭 Production Observability

Metrics, tracing hooks, health checks, readiness checks, request IDs, retries, and DLQ support are integrated into the architecture.

---

# 🧱 Technology Stack

| Layer               | Technology                  |
| ------------------- | --------------------------- |
| Language            | **Go**                      |
| API Gateway         | **REST + WebSocket**        |
| Internal RPC        | **gRPC**                    |
| Event Streaming     | **Apache Kafka / KRaft**    |
| Hot State           | **Redis**                   |
| Database            | **PostgreSQL 16 + PostGIS** |
| Geospatial Index    | **H3**                      |
| Authentication      | **JWT**                     |
| Authorization       | **RBAC**                    |
| Rate Limiting       | **Redis Token Bucket**      |
| Metrics             | **Prometheus**              |
| Distributed Tracing | **OpenTelemetry**           |
| Deployment          | **Docker Compose**          |

---

# 🎯 What This Project Demonstrates

NEXUS-DISPATCH is more than a ride-booking API.

It demonstrates practical distributed-systems engineering concepts including:

```text
Distributed Systems
        │
        ├── Concurrency Control
        ├── Atomic Operations
        ├── Event-Driven Architecture
        ├── Fault Tolerance
        ├── Service Decomposition
        ├── State Machines
        ├── Distributed Caching
        ├── Geospatial Indexing
        ├── Idempotent Persistence
        ├── Real-Time Streaming
        ├── Observability
        └── Graceful Degradation
```

---

# 🏁 Project Status

### Production-Oriented Distributed Dispatch Prototype

The current implementation includes:

* ✅ Microservice architecture
* ✅ REST + gRPC APIs
* ✅ Real-time GPS ingestion
* ✅ H3 geospatial search
* ✅ Candidate scoring
* ✅ Atomic driver reservation
* ✅ Kafka event streaming
* ✅ Redis hot state
* ✅ PostgreSQL/PostGIS persistence
* ✅ WebSocket notifications
* ✅ JWT authentication
* ✅ Role-based access
* ✅ Rate limiting
* ✅ Prometheus metrics
* ✅ OpenTelemetry hooks
* ✅ Health/readiness endpoints
* ✅ Graceful shutdown
* ✅ Retry + DLQ architecture
* ✅ Degraded infrastructure modes
* ✅ Concurrency testing
* ✅ Architecture Decision Records

---

# 📚 Documentation

For deeper technical details:

```text
docs/architecture.md
docs/decisions/
```

The architecture documentation explains the system design, while the ADR collection records important architectural decisions and their rationale.

---

# ⭐ The NEXUS-DISPATCH Principle

> ### **Fast when the system is healthy.**
>
> ### **Correct when the system is busy.**
>
> ### **Safe when the system is failing.**

NEXUS-DISPATCH is designed around a simple distributed-systems philosophy:

**Optimize for speed — without sacrificing correctness.**

---

<p align="center">

### 🚀 NEXUS-DISPATCH

**Real-Time Geo-Tracking • Intelligent Dispatch • Atomic Reservations • Event-Driven Architecture**

Built with **Go + Kafka + Redis + PostgreSQL/PostGIS + H3**

</p>

