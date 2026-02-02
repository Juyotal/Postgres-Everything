CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pg_vectors (
    id          BIGSERIAL   PRIMARY KEY,
    collection  TEXT        NOT NULL DEFAULT 'default',
    content     TEXT        NOT NULL,
    -- Dimension is intentionally omitted here; HNSW index is created separately
    -- via VectorStore.create_hnsw_index() once the dimension is known.
    embedding   VECTOR,
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pg_vectors_collection
    ON pg_vectors (collection);

CREATE INDEX IF NOT EXISTS idx_pg_vectors_metadata
    ON pg_vectors USING GIN (metadata);
