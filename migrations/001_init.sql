-- NEXUS-DISPATCH schema (PostgreSQL 16 + PostGIS)
--
-- Durable business state. GPS hot state lives in Redis (ADR-008); this
-- database is the system of record for users, drivers, rides, dispatches,
-- and downsampled driver history.

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- users
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email       TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    role        TEXT NOT NULL DEFAULT 'USER' CHECK (role IN ('USER','DRIVER','ADMIN')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- drivers
-- ============================================================
CREATE TABLE IF NOT EXISTS drivers (
    id            UUID PRIMARY KEY,
    user_id       UUID REFERENCES users(id) ON DELETE SET NULL,
    name          TEXT NOT NULL,
    email         TEXT NOT NULL UNIQUE,
    state         TEXT NOT NULL DEFAULT 'OFFLINE'
                  CHECK (state IN ('OFFLINE','AVAILABLE','RESERVED','ON_TRIP','PAUSED')),
    rating        NUMERIC(3,2) NOT NULL DEFAULT 5.0 CHECK (rating >= 0 AND rating <= 5),
    trips_completed BIGINT NOT NULL DEFAULT 0,
    -- Last known position (durable copy; hot copy is in Redis).
    last_lat      DOUBLE PRECISION,
    last_lng      DOUBLE PRECISION,
    last_location_at TIMESTAMPTZ,
    location      GEOGRAPHY(POINT, 4326),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Spatial index on last known position (for "drivers in area" analytics).
CREATE INDEX IF NOT EXISTS idx_drivers_location ON drivers USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_drivers_state ON drivers (state);

-- ============================================================
-- vehicles
-- ============================================================
CREATE TABLE IF NOT EXISTS vehicles (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    driver_id   UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    make        TEXT NOT NULL,
    model       TEXT NOT NULL,
    plate       TEXT NOT NULL UNIQUE,
    seats       SMALLINT NOT NULL DEFAULT 4 CHECK (seats BETWEEN 1 AND 12),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- rides
-- ============================================================
CREATE TABLE IF NOT EXISTS rides (
    id            UUID PRIMARY KEY,
    user_id       UUID NOT NULL REFERENCES users(id),
    driver_id     UUID REFERENCES drivers(id),
    state         TEXT NOT NULL DEFAULT 'REQUESTED'
                  CHECK (state IN ('REQUESTED','MATCHED','DRIVER_EN_ROUTE','ARRIVED',
                                   'IN_PROGRESS','COMPLETED','CANCELLED','EXPIRED')),
    pickup        GEOGRAPHY(POINT, 4326) NOT NULL,
    dropoff       GEOGRAPHY(POINT, 4326) NOT NULL,
    radius_m      INTEGER NOT NULL DEFAULT 1000,
    cancel_reason TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    matched_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_rides_user ON rides (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rides_state ON rides (state) WHERE state NOT IN ('COMPLETED','CANCELLED','EXPIRED');
CREATE INDEX IF NOT EXISTS idx_rides_driver ON rides (driver_id) WHERE driver_id IS NOT NULL;

-- ============================================================
-- ride_events (append-only lifecycle log)
-- ============================================================
CREATE TABLE IF NOT EXISTS ride_events (
    id          BIGSERIAL PRIMARY KEY,
    ride_id     UUID NOT NULL REFERENCES rides(id) ON DELETE CASCADE,
    state       TEXT NOT NULL,
    driver_id   UUID,
    reason      TEXT,
    event_id    TEXT NOT NULL,          -- idempotency key
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ride_id, event_id)          -- duplicate events are no-ops
);

CREATE INDEX IF NOT EXISTS idx_ride_events_ride ON ride_events (ride_id, created_at);

-- ============================================================
-- dispatches
-- ============================================================
CREATE TABLE IF NOT EXISTS dispatches (
    id          UUID PRIMARY KEY,
    ride_id     UUID NOT NULL REFERENCES rides(id) ON DELETE CASCADE,
    driver_id   UUID NOT NULL REFERENCES drivers(id),
    state       TEXT NOT NULL DEFAULT 'RESERVED'
                CHECK (state IN ('RESERVED','ASSIGNED','ON_TRIP','COMPLETED','CANCELLED','EXPIRED')),
    distance_m  DOUBLE PRECISION NOT NULL,
    score       DOUBLE PRECISION NOT NULL,
    candidates_considered INTEGER NOT NULL DEFAULT 0,
    cells_searched        INTEGER NOT NULL DEFAULT 0,
    dispatch_latency_ms   DOUBLE PRECISION,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ride_id, driver_id)         -- one dispatch per (ride, driver)
);

-- THE durable backstop for the no-double-booking invariant (ADR-006):
-- a driver can have at most ONE active dispatch.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_dispatch
    ON dispatches (driver_id)
    WHERE state IN ('RESERVED','ASSIGNED','ON_TRIP');

CREATE INDEX IF NOT EXISTS idx_dispatches_ride ON dispatches (ride_id);

-- ============================================================
-- driver_history (downsampled GPS trail, 1 point / 5 s)
-- ============================================================
CREATE TABLE IF NOT EXISTS driver_history (
    driver_id   UUID NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    ts          TIMESTAMPTZ NOT NULL,
    location    GEOGRAPHY(POINT, 4326) NOT NULL,
    seq         BIGINT NOT NULL,
    PRIMARY KEY (driver_id, ts)
);

CREATE INDEX IF NOT EXISTS idx_driver_history_loc ON driver_history USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_driver_history_ts ON driver_history (driver_id, ts DESC);