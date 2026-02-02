CREATE TABLE IF NOT EXISTS pg_documents (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    collection  TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- GIN index lets @> (contains) queries skip full-table scans even on nested fields.
CREATE INDEX IF NOT EXISTS idx_pg_documents_data
    ON pg_documents USING GIN (data);

CREATE INDEX IF NOT EXISTS idx_pg_documents_collection
    ON pg_documents (collection);
