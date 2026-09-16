# ADR-010: Scaling strategy

**Status:** Accepted

**Context:** The system must scale horizontally from a single-city demo (10k
drivers) toward multi-city (1M+ drivers) without redesign.

**Decision — scaling axes, in order:**

1. **Stateless services scale by replication.** Gateway, Driver, Ride, Location,
   Dispatch are all stateless; add instances behind the load balancer. The only
   shared state is in Kafka/Redis/Postgres.

2. **Kafka partitions are the consumer parallelism ceiling.** Location processing
   scales with `driver-location` partitions (12 → 48 → …). Rebalance is automatic
   via consumer groups. Producers scale freely (idempotent producer, key-based
   routing).

3. **Redis scales by cluster mode.** Cell keys (`h3:{cell}:drivers`) hash
   uniformly; the reservation lease key hashes by driver — no hot key at city
   scale. The one shared global key (`geo:drivers`) is the first thing to shard:
   in cluster mode we'd split it by H3 res-5 super-region (`geo:{region}:drivers`)
   so a city's data stays on one shard (a documented change, not a rewrite).

4. **Postgres scales by read replicas + partitioning.** `driver_history` and
   `ride_events` are partitioned by month; analytics read from replicas. The
   write path is already low-volume by design (downsampling + batching).

5. **Geographic sharding (1M+ drivers).** H3 is hierarchical: res-5 cells tile a
   country. Sharding by res-5 region gives natural data locality — a region's
   drivers, requests, and Redis keys all live together. Cross-region dispatch is
   explicitly out of scope (a ride doesn't cross regions mid-match).

**What we do NOT do:** no multi-active global coordination, no vector clocks, no
CRDTs — the domain's invariants are per-driver/per-ride, and per-key sharding
keeps each invariant local.

**Consequences:** the res-5 sharding seam is designed now (key prefixes, region
field in driver records) so it's a config change later, not a migration.