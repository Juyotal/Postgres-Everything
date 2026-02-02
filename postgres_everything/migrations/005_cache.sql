CREATE TABLE IF NOT EXISTS pg_cache (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial index on expires_at speeds up the cleanup query.
CREATE INDEX IF NOT EXISTS idx_pg_cache_expires
    ON pg_cache (expires_at)
    WHERE expires_at IS NOT NULL;
