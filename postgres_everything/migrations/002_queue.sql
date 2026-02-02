CREATE TABLE IF NOT EXISTS pg_jobs (
    id           BIGSERIAL   PRIMARY KEY,
    queue        TEXT        NOT NULL DEFAULT 'default',
    task_name    TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}',
    status       TEXT        NOT NULL DEFAULT 'pending',
    priority     INT         NOT NULL DEFAULT 0,
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL DEFAULT 3,
    run_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_at    TIMESTAMPTZ,
    locked_by    TEXT,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Partial index covers only rows that workers actually compete for.
CREATE INDEX IF NOT EXISTS idx_pg_jobs_fetch
    ON pg_jobs (queue, status, priority DESC, run_at)
    WHERE status = 'pending';
