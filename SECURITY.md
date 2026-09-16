# Security Policy

## Supported versions

| Version | Supported          |
| ------- | ------------------ |
| main    | ✅ Bug fixes and security patches |

## Reporting a vulnerability

**Do not open a public issue for security vulnerabilities.**

Please report suspected vulnerabilities privately:

1. Go to **Security → Advisories → New draft security advisory** on this
   repository (requires repo access), or
2. Email the maintainer with a clear description of the issue, steps to
   reproduce, and any suggested mitigation.

We aim to acknowledge reports within **48 hours** and to ship a fix within a
reasonable timeframe depending on severity.

## Scope

This project is a dispatch engine with real-time driver state. Particular
attention is given to:

- **Authorization bypass** — JWT role checks on gateway routes
  (`internal/gateway/`), especially admin-only endpoints.
- **Reservation races** — the atomic lease in `internal/redis/` must remain
  the single authority for driver availability.
- **Input validation** — GPS coordinate bounds, sequence numbers, and
  request payloads in `internal/location/`.
- **Secrets handling** — no secrets in code, logs, or committed config.

## Security notes for operators

- The default `JWT_SECRET` in `.env.example` is for **local development
  only**. Always set a strong, unique secret in production.
- The default Postgres credentials (`nexus`/`nexus`) are for local Docker
  use. Change them for any shared or internet-facing deployment.
- The gateway's rate limits (`USER_RATE_CAPACITY`, `IP_RATE_CAPACITY`)
  should be tuned for your traffic before production use.