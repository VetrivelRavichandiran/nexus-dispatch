# ADR-005: Why PostgreSQL + PostGIS?

**Status:** Accepted

**Context:** Durable business state — users, drivers, vehicles, rides, ride events,
dispatches, driver history — needs transactions, foreign keys, and rich querying
("all rides in area X last week"). GPS updates must NOT be the primary workload of
this database.

**Decision:** PostgreSQL 16 with the PostGIS extension for durable state.

**Alternatives:**
- *Postgres without PostGIS* — we'd store lat/lon as floats and compute distance in
  SQL or app code; fine for a demo, but we lose `ST_DWithin`/GiST indexes for the
  analytics queries the platform will inevitably need ("drivers who were in this
  zone during this window").
- *MongoDB* — document model doesn't fit relational invariants (a ride references
  driver + user + vehicle; we want FKs and a unique partial index on active
  dispatches).
- *Cassandra* — great write throughput, weak on the transactional invariants this
  domain needs (state machines, unique constraints).

**Reasoning:** Postgres gives us (1) a **unique partial index** as the durable
backstop for the no-double-booking invariant, (2) `ON CONFLICT` for idempotent
event persistence, (3) PostGIS `geography` columns + GiST indexes for historical
spatial queries, (4) connection pooling via `pgxpool` with sane defaults.

**Consequences:** the Event Processor is the only writer of high-volume data
(`driver_history`), and it **downsamples** GPS points to 1/5 s and batches inserts
(100 rows/tx) so the DB sees ~20 tx/s even at 50k updates/s. GPS hot state stays in
Redis (ADR-008).