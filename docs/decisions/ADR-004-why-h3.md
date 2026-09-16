# ADR-004: Why H3 (and why H3 + Redis GEO, not H3 alone)?

**Status:** Accepted

**Context:** We need to answer "available drivers within R meters of (lat, lon)"
without scanning the fleet. The design must handle cell boundaries correctly and
scale to 100k+ drivers.

**Decision:** **H3 resolution 7** for hierarchical spatial partitioning, with the
cell → drivers mapping stored as Redis sets (`h3:{cell}:drivers`), and a Redis
**GEO zset** (`geo:drivers`) maintained in parallel as (a) the authoritative
position store for precise distance, and (b) a fallback/verification index.

**Alternatives:**
- *Redis GEO alone (GEORADIUS)* — simplest: one command returns nearby drivers with
  distance. Rejected as the *primary* design because: (1) it is a flat geohash
  index with no hierarchy — we cannot cheaply answer "how many drivers per region"
  or do region-level load balancing; (2) the spec and the interview story both
  benefit from demonstrating explicit spatial partitioning; (3) GEORADIUS returns
  by distance order which is fine, but cell sets give us per-cell driver counts for
  capacity planning and for the "search only k cells" complexity argument.
- *PostGIS `ST_DWithin`* — correct but lives in the durable DB; running dispatch
  queries there couples the hot path to Postgres availability and latency.
- *Quadtree / custom grid* — H3 is a well-studied hierarchical hexagonal tiling
  with uniform cell size, well-defined neighbor rings (`gridDisk`), and stable cell
  IDs; reinventing a grid buys nothing.

**Reasoning:** H3 res-7 cells (~1.22 km², circumradius ≈ 400 m) mean a 1 km search
radius touches at most a k=2 ring (19 cells). Candidate count ≈ fleet density ×
19 cells — independent of total fleet size. The k-ring expansion is the explicit
answer to "what if the driver is just across the boundary?": the request's cell
ring always contains every cell within `k × cellRadius` of the request point.

**Consequences:** we maintain two indexes (cell sets + geo zset) — a deliberate
redundancy; the geo zset is also the source of truth for *precise* distance (cell
membership only says "probably near"). Both are updated in one Lua script per
location update so they cannot diverge. Cell membership changes (driver moves to a
new cell) are handled atomically (SREM old + SADD new in the same script).

**Implementation note (H3 library):** the official `uber/h3-go` binding requires
cgo (a C toolchain in every build image). We use `github.com/dimchansky/h3-go`
(v0.4.0), a pure-Go reimplementation of the H3 core (no cgo, no unsafe). Its
correctness is pinned by `internal/geospatial/parity_test.go`, which asserts our
cell IDs and centers match the official H3 reference values (e.g.
`latLngToCell(12.9716, 77.5946, 7) == 8760145b4ffffff`, verified against the
official H3 Python/JS libraries). If the library ever drifts, that test fails.