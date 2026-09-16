# ADR-007: Kafka partitioning strategy

**Status:** Accepted

**Context:** We need per-driver ordering for location updates (sequence-based
dedupe) and per-ride ordering for ride lifecycle events, while maximizing consumer
parallelism.

**Decision:**
- `driver-location`, `driver-reserved`, `driver-status`: **key = driver_id**, 12 partitions.
- `ride-requested`, `dispatch-created`, `ride-status`: **key = ride_id**, 8 partitions.
- `nexus-dead-letter`: key = original key, 3 partitions.

**Reasoning:**
- Kafka orders **within a partition**. Keying location events by `driver_id` puts
  one driver's entire stream in one partition → the Location Processor sees that
  driver's updates in produce order, so the `sequence` field is a reliable
  out-of-order guard (older seq → reject).
- Keying by ride_id keeps a ride's lifecycle (requested → reserved → arrived →
  completed) ordered for the persistence consumer, which applies state transitions
  in order.
- Partition counts are chosen for parallelism headroom (12 location consumers can
  run in dev; more in prod) without over-sharding (each partition needs a consumer
  to add throughput; 12 is a sane default for a single-city deployment).

**Alternatives:**
- *Round-robin / no key* — breaks per-driver ordering; sequence dedupe would need
  wall-clock timestamps, which are unreliable across devices. **Rejected.**
- *One partition per topic* — maximum ordering, zero parallelism. **Rejected.**
- *Key = H3 cell* — would order per-cell, not per-driver; a driver moving between
  cells would split its stream. **Rejected.**

**Consequences:** a single hot driver (e.g. a simulator bug pinning one driver at
50k updates/s) can only fill one partition; mitigated by per-driver rate limiting
in the Location Service (10 updates/s/driver, documented) and producer-side
backpressure.