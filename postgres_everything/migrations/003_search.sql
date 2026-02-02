CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE IF NOT EXISTS pg_search_documents (
    id             BIGSERIAL   PRIMARY KEY,
    collection     TEXT        NOT NULL DEFAULT 'default',
    title          TEXT        NOT NULL,
    body           TEXT        NOT NULL DEFAULT '',
    metadata       JSONB       NOT NULL DEFAULT '{}',
    -- Generated column keeps tsvector in sync automatically.
    search_vector  TSVECTOR    GENERATED ALWAYS AS (
        setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(body,  '')), 'B')
    ) STORED,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pg_search_vector
    ON pg_search_documents USING GIN (search_vector);

CREATE INDEX IF NOT EXISTS idx_pg_search_title_trgm
    ON pg_search_documents USING GIN (title gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_pg_search_collection
    ON pg_search_documents (collection);
