# ADR-009: WebSocket architecture

**Status:** Accepted

**Context:** Clients (driver app, user app, dashboard) need push updates: driver
positions (batched), ride status changes, dispatch notifications.

**Decision:**
- **Gateway owns all WS connections** (single entry point; the gateway is the only
  service that speaks the client protocol).
- **Per-connection goroutine** with a bounded outbound queue (256 messages); on
  overflow the client is dropped (backpressure = disconnect, documented) — a slow
  client must not stall the hub.
- **Fan-out via Kafka, not in-memory pub/sub**: the gateway runs a WS-bridge
  consumer group (`ws-bridge`) over `dispatch-created`, `ride-status`,
  `driver-reserved`. Every gateway instance consumes, so a client connected to
  instance A receives events produced anywhere. Position updates (high volume) are
  **not** pushed per-update: the gateway batches a per-client "nearby drivers"
  snapshot at 2 Hz from Redis (the client's subscribed area), which is what a real
  ride app does (it does not receive every driver's GPS).
- **Auth**: WS handshake requires `?token=<jwt>`; the gateway verifies before
  upgrade; role determines the event channels allowed.
- **Liveness**: 30 s ping/pong; missed pong → close. Reconnect is client-side
  (exponential backoff in the demo client); the server keeps no per-client state
  beyond the queue, so reconnect is cheap.

**Alternatives:**
- *Redis Pub/Sub for fan-out* — works for 1–2 gateway instances but loses messages
  if a gateway is down at publish time (no persistence); Kafka's replay covers
  reconnects. **Rejected** in favor of Kafka (we already run it).
- *SSE* — unidirectional is enough for most events, but driver apps send
  heartbeats/acks over the same channel; WS is the standard for this domain.
- *Per-service WS endpoints* — clients would need N connections and the gateway
  loses central auth/rate-limiting. **Rejected.**

**Consequences:** the WS bridge adds one more consumer group (cheap); position
push is sampled (2 Hz) — a deliberate latency/bandwidth trade-off documented in
the API docs.