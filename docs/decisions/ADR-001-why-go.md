# ADR-001: Why Go?

**Status:** Accepted

**Context:** We need a backend that sustains very high connection counts (GPS
ingest, WebSockets), low per-request memory, easy concurrency for fan-out work
(dispatch search, WS bridges), and a single static binary per service for
containerization.

**Decision:** Go 1.23.

**Alternatives:**
- *Java/Kotlin + Spring* — strong ecosystem, but higher memory floor and slower
  iteration for a portfolio project; gRPC/streams equally available in Go.
- *Node/TypeScript* — good for WS, but CPU-bound geo math (haversine × thousands of
  candidates) and high-throughput Kafka consumers are weaker in a single-threaded
  runtime; worker threads add complexity.
- *Rust* — best raw performance, but longer development time for the breadth of
  features required; the performance gap vs Go is not the bottleneck here (Redis
  round-trips are).

**Reasoning:** goroutines + channels map directly onto the workload (per-connection
WS state, per-partition consumer loops, bounded worker pools for dispatch); the
standard library covers HTTP, TLS, JSON, context; `go test -race` gives real
concurrency-safety verification; static binaries make images small and
reproducible.

**Consequences:** one language across services (simpler monorepo); we accept Go's
GC pauses (measured < 1 ms p99 in benchmarks) over Rust's zero-cost guarantees.