# ADR-002: Why Kafka?

**Status:** Accepted

**Context:** Location updates (up to 50k/s), ride requests, dispatch decisions, and
status changes must be decoupled: producers should not block on consumers,
consumers must scale independently, and a durable replayable log is needed for the
persistence pipeline and for debugging.

**Decision:** Apache Kafka (KRaft mode, single broker in dev compose) as the
backbone event bus.

**Alternatives:**
- *Direct gRPC calls only* — couples location ingest to Redis write latency and
  makes the persistence pipeline a push-based fan-out; no replay; a slow consumer
  stalls the producer.
- *NATS / Redis Streams* — lighter weight; NATS has no built-in durable log with
  the same retention/partition semantics; Redis Streams is tied to the Redis we
  already use for coordination (mixing roles increases blast radius).
- *RabbitMQ* — work-queue semantics, not a log; poor fit for "many consumers each
  replaying the stream" (location processor, persistence, WS bridge).

**Reasoning:** Kafka gives (1) per-partition ordering — exactly what per-driver
sequence ordering needs, (2) consumer groups — horizontal scaling of processors,
(3) retention/replay — the Event Processor can catch up after an outage, (4)
proven throughput far beyond our target. The operational cost (a broker) is
acceptable: compose ships KRaft single-node; production would be 3-broker.

**Consequences:** one more stateful component to operate; we mitigate with
at-least-once + idempotent consumers (so broker restarts are safe), DLQ topic, and
producer buffering with bounded memory.