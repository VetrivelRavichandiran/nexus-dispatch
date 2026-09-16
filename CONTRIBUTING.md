# Contributing to NEXUS-DISPATCH

Thanks for your interest in contributing! This guide covers the development
setup, code conventions, and how to submit changes.

## Development setup

Prereqs: **Go ≥ 1.25**, Docker (optional, for the full stack).

```bash
git clone https://github.com/VetrivelRavichandiran/nexus-dispatch.git
cd nexus-dispatch

# Full stack (Kafka + Redis + Postgres + all 7 services):
docker compose up --build -d

# Or, for code changes, run services locally:
cp .env.example .env
make build
make test
```

## Code conventions

- **Formatting:** all code must pass `gofmt` (run `make fmt`) and `go vet`
  (`make vet`).
- **Tests:** new features and bug fixes should include tests. The
  integration suite lives in `tests/integration/` and uses in-memory Redis
  (miniredis) — no external services required.
- **Concurrency:** anything touching shared driver state MUST go through the
  atomic Redis Lua scripts in `internal/redis/`. Never do
  check-then-act on driver availability in Go code — that's how
  double-booking happens. See
  [ADR-006](docs/decisions/ADR-006-reservation-consistency.md).
- **gRPC contracts:** changes to `api/proto/nexus/v1/*.proto` must be
  backward-compatible (additive fields only, no renumbering) and the
  generated code in `api/gen/` must be regenerated and committed.
- **Events:** new Kafka events belong in `internal/events/` with a codec, a
  topic constant in `internal/kafka/`, and a consumer handler.
- **Config:** all configuration is environment-based via
  `internal/config/` — add new knobs there with a sensible default and
  document them in `.env.example`.

## Submitting changes

1. Fork the repo and create a branch: `git checkout -b feat/my-change`
   (or `fix/...`, `docs/...`).
2. Keep commits focused; write clear commit messages
   (`feat: ...`, `fix: ...`, `docs: ...`).
3. Before pushing, make sure everything is green:

   ```bash
   make fmt
   make vet
   make test        # unit + integration
   make test-race   # with the race detector
   ```

4. Open a pull request against `main` using the PR template. Explain the
   **why**, not just the what, and note any config, schema, or proto
   changes.
5. CI must pass (build + vet + tests) before a PR can be merged.

## Good first issues

Look for issues labeled `good first issue`. The geospatial and reservation
packages are well-tested and self-contained — good places to start.

## Questions?

Open a discussion or an issue — there's no bad question.