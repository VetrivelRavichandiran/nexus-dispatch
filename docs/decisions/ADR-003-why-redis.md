# ADR-003: Why Redis?

**Status:** Accepted

**Context:** Two needs: (a) **hot real-time state** — last known position and
availability of every driver, readable at dispatch time in < 5 ms; (b)
**distributed coordination** — atomic driver reservation across dispatch-engine
instances, plus token-bucket rate limiting shared by gateway instances.

**Decision:** Redis 7 (single node in dev; Sentinel/Cluster documented as the
scale path).

**Alternatives:**
- *Postgres for hot state* — a 50k updates/s write rate into Postgres is possible
  but turns the durable DB into the hot path: WAL pressure, index churn on the
  location table, and dispatch queries doing geospatial scans on a table being
  rewritten constantly. We explicitly do NOT do this (ADR-008).
- *In-process map* — breaks the moment we run > 1 dispatch instance (the whole
  point of horizontal scaling) and loses state on restart.
- *etcd / ZooKeeper* — consensus stores; overkill and slower for high-frequency
  location writes; their locking primitives are not designed for 30 s TTL leases at
  dispatch rates.

**Reasoning:** Redis gives atomic multi-key operations via **Lua scripts** (the
reservation primitive), native **GEO commands** (geohash zset) as a fallback index,
sub-millisecond latency, TTLs for self-healing leases, and a single shared instance
that all services can reach. Its data is **loss-tolerant by design** here — every
key is rebuildable from Postgres + Kafka replay — so a Redis restart degrades
rather than corrupts.

**Consequences:** memory is bounded by driver count (≈ 1 KB/driver hot state →
100k drivers ≈ 100 MB, fine); we document maxmemory `allkeys-lru` + TTLs to prevent
unbounded growth; a Redis outage fails reservations closed (503) — an intentional
availability-vs-correctness trade-off (ADR-006).