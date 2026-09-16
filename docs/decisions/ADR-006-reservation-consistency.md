# ADR-006: Reservation consistency strategy

**Status:** Accepted

**Context:** Two concurrent ride requests may pick the same top-ranked driver.
Exactly one must win. This is the system's hardest correctness requirement.

**Decision:** **Atomic Redis Lua reservation** (single-script compare-and-set on a
TTL lease, with a versioned lease value) **+ durable Postgres unique partial index
as backstop**.

**Alternatives considered:**

1. *`SET driver:{id}:lock NX EX 30`* — the naive choice. Problems: (a) no check of
   driver state (a RESERVED driver could be "reserved" again by a second ride
   before the first completes the transition); (b) lease renewal is not
   re-entrant — the holder can't extend its own lease without a GET (non-atomic
   check-then-act); (c) release is a blind `DEL`: if holder A crashes, its deferred
   release goroutine can fire *after* TTL expiry and delete holder B's fresh
   lease. **Rejected.**

2. *Postgres `SELECT … FOR UPDATE` on the driver row* — correct and simple, but
   puts the durable DB on the hot path: every dispatch takes a row lock, dispatch
   throughput is bounded by Postgres lock throughput, and a Postgres outage kills
   dispatching entirely. We'd also have to keep Redis availability in sync with the
   DB row (two sources of truth). **Rejected for the hot path; kept as backstop.**

3. *etcd lease + watch* — consensus-grade, but etcd is another stateful service,
   its lock API is not designed for 30 s TTL churn at dispatch rates, and we
   already run Redis. **Rejected.**

**Why the chosen design is safe:**
- The Lua script runs **atomically** in Redis — no interleaving between the
  EXISTS check and the SET. Two racers serialize; one wins.
- The lease **value** is `ride_id:token:version`. Renewal is re-entrant (same
  token). Release is a **compare-and-delete** script keyed on the full value — a
  stale holder with an old version cannot delete a newer lease.
- **TTL 30 s + renewal every 10 s** bounds the "winner crashed" window: worst case
  a driver is unreservable for ≤ 30 s after a dispatch-engine crash, then the
  lease expires and the ride (still REQUESTED) is re-dispatched.
- **Fail-closed:** if Redis is unreachable, reservation returns an error and the
  ride stays REQUESTED (retried) — we never guess.
- **Backstop:** `CREATE UNIQUE INDEX uq_active_dispatch ON dispatches(driver_id)
  WHERE state IN ('RESERVED','ASSIGNED','ON_TRIP')` — even a hypothetical bug that
  bypasses Redis cannot create two active dispatches; the second INSERT fails.

**Crash behavior matrix:**
| Who crashes | After | System state |
|---|---|---|
| Dispatch engine (after win, before notify) | ≤ 30 s | lease expires; ride retried or cancelled by ride timeout |
| Redis | immediately | reservations fail-closed (503); location writes Kafka-only; state rebuilds on recovery |
| Driver Service (notify) | immediately | gRPC timeout → dispatch marks ride unmatched, releases lease (compare-and-delete), ride retried |
| Winner's renew goroutine | ≤ 30 s | same as first row |

**Consequences:** reservation correctness depends on Redis being the single
coordinator (true by deployment: one Redis per environment in dev; Sentinel/Cluster
with the lease key on one node in prod — documented). We accept this coupling.