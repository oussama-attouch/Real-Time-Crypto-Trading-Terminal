-- ============================================================================
-- V1: the write side of the terminal — persisted orders.
--
-- Flyway owns the schema from here on; Hibernate is configured with ddl-auto=none
-- (see application.yml) so the migration is the single source of truth. Every
-- decimal is NUMERIC, never float: an order price that lost precision is a bug
-- that shows up as a rejected or mispriced order.
-- ============================================================================

CREATE TABLE orders (
    id          UUID            PRIMARY KEY,

    symbol      VARCHAR(32)     NOT NULL,

    -- Stored as text so the DB is readable without an enum catalogue, and so adding
    -- a side/status later is a data change rather than a type migration.
    side        VARCHAR(8)      NOT NULL
        CONSTRAINT orders_side_check CHECK (side IN ('BUY', 'SELL')),

    -- NULL for MARKET orders: there is no limit price to record.
    price       NUMERIC(38, 18),

    quantity    NUMERIC(38, 18)  NOT NULL
        CONSTRAINT orders_quantity_check CHECK (quantity > 0),

    status      VARCHAR(16)     NOT NULL
        CONSTRAINT orders_status_check CHECK (status IN ('NEW', 'FILLED', 'CANCELLED')),

    created_at  TIMESTAMPTZ     NOT NULL
);

-- The read side of the UI asks "my most recent orders for this symbol", so index
-- for that access path rather than for lookups by primary key alone.
CREATE INDEX orders_symbol_created_at_idx ON orders (symbol, created_at DESC);

COMMENT ON TABLE orders IS 'Mock order engine (Phase 8). Persisted intent only: no matching engine yet.';
COMMENT ON COLUMN orders.price IS 'NULL = MARKET order. NUMERIC(38,18) matches Binance spot precision.';
