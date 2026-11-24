-- ============================================================================
-- Phase 1 bootstrap: Postgres is provisioned but not yet on the hot path.
-- This file runs exactly once, on first container initialisation (empty volume).
-- It only reserves the schema namespace so Phase 2 can add migrations without
-- a chicken-and-egg "schema does not exist" problem.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS market;

COMMENT ON SCHEMA market IS 'Market data storage (trades, candles) - Phase 2+';

-- Sanity marker so `docker compose exec postgres psql -U trader -d trading` shows
-- something meaningful during Phase 1 smoke tests.
CREATE TABLE IF NOT EXISTS market.schema_version (
    version     INTEGER      PRIMARY KEY,
    applied_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    description TEXT         NOT NULL
);

INSERT INTO market.schema_version (version, description)
VALUES (1, 'phase-1 infrastructure bootstrap')
ON CONFLICT (version) DO NOTHING;
