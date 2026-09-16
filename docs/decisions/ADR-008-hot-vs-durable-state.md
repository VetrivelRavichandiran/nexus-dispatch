# ADR-008: Hot-state vs durable-state separation

**Status:** Accepted

**Context:** GPS updates arrive at up to 50k/s. Business records (rides,
dispatches, users) change at ~100/s. Putting both in one store forces a
compromise: either the fast store is too weak for invariants, or the durable store
becomes the hot path.

**Decision:** **Two-tier state.**
- **Hot tier (Redis):** last-known position, availability, H3 cell membership,
  reservation leases, rate-limit buckets, hot ride state. Loss-tolerant,
  TTL-bounded, sub-ms reads.
- **Durable tier (Postgres/PostGIS):** users, drivers, vehicles, rides,
  ride_events, dispatches, driver_history (downsampled). Transactional, indexed,
  the system of record.

**The contract between tiers:**
1. Redis is **always rebuildable**: on startup, the Location Processor re-reads
   driver positions from `driver_history` (latest per driver) and rebuilds cell
   sets + geo index; availability re-derives from `drivers.state`.
2. Kafka is the **bridge**: every state change is an event; the Event Processor
   applies them to Postgres idempotently. If Redis is lost, replaying Kafka
   (1 h retention) recovers recent hot state.
3. **No hot-path write to Postgres**: GPS updates never touch the DB in the
   request path. `driver_history` receives downsampled (1/5 s) batched inserts.

**Why this is correct:** the only *strong* invariant (no double-booking) is
enforced in the hot tier (atomic lease) AND backstopped in the durable tier
(unique partial index) — so neither tier alone can violate it. Everything else is
eventually consistent by design and documented as such.

**Consequences:** we operate two stateful stores; the rebuild path is a tested
feature (`make rebuild-hot-state`), not an afterthought.