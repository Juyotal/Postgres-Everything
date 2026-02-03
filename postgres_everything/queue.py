from __future__ import annotations

import logging
import signal
import socket
import time
import uuid
from typing import Any, Callable

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from postgres_everything.base import PostgresModule
from postgres_everything.connection import ConnectionPool
from postgres_everything.exceptions import TaskHandlerError

logger = logging.getLogger("postgres_everything.queue")

# Fetch and atomically lock the highest-priority pending job.
# FOR UPDATE SKIP LOCKED lets multiple workers run concurrently without
# blocking each other or causing deadlocks.
_FETCH_JOB_SQL = """
WITH candidate AS (
    SELECT id
    FROM pg_jobs
    WHERE queue = %s
      AND status = 'pending'
      AND run_at <= NOW()
    ORDER BY priority DESC, run_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE pg_jobs
SET status    = 'running',
    locked_at = NOW(),
    locked_by = %s,
    attempts  = attempts + 1
FROM candidate
WHERE pg_jobs.id = candidate.id
RETURNING pg_jobs.*
"""


class TaskQueue(PostgresModule):
    """RabbitMQ-style task queue backed by PostgreSQL SKIP LOCKED.

    Workers call :meth:`process_one` or :meth:`run_worker` to consume jobs.
    Handlers are registered with the :meth:`register` decorator.

    Failed jobs are retried with exponential backoff (``2 ** attempts``
    seconds) until ``max_attempts`` is exceeded, then marked ``failed``.

    Args:
        pool: Shared connection pool.
        dsn: PostgreSQL DSN (creates a private pool when pool is omitted).
    """

    def __init__(
        self,
        pool: ConnectionPool | None = None,
        dsn: str | None = None,
    ) -> None:
        super().__init__(pool=pool, dsn=dsn)
        self._handlers: dict[str, Callable[[dict], None]] = {}

    # ------------------------------------------------------------------
    # Producer API
    # ------------------------------------------------------------------

    def enqueue(
        self,
        task_name: str,
        payload: dict,
        *,
        queue: str = "default",
        priority: int = 0,
        delay_seconds: int = 0,
        max_attempts: int = 3,
    ) -> int:
        """Add a job to the queue.

        Args:
            task_name: Name of the registered handler to invoke.
            payload: Arbitrary JSON-serialisable dict passed to the handler.
            queue: Logical queue name; workers specify which queue to consume.
            priority: Higher values are processed first.
            delay_seconds: Seconds to wait before the job becomes eligible.
            max_attempts: Maximum number of attempts before marking failed.

        Returns:
            Integer job ID.
        """
        row = self._fetch_one(
            """
            INSERT INTO pg_jobs
                (queue, task_name, payload, priority, max_attempts, run_at)
            VALUES
                (%s, %s, %s, %s, %s, NOW() + %s * interval '1 second')
            RETURNING id
            """,
            (queue, task_name, Jsonb(payload), priority, max_attempts, delay_seconds),
        )
        assert row is not None
        job_id = int(row["id"])
        logger.debug("Enqueued job %d task='%s' queue='%s'", job_id, task_name, queue)
        return job_id

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def register(self, task_name: str) -> Callable:
        """Decorator to register a handler function for ``task_name``.

        Handler signature::

            def my_handler(payload: dict) -> None: ...

        Args:
            task_name: Name used when enqueuing jobs.
        """

        def decorator(func: Callable[[dict], None]) -> Callable[[dict], None]:
            self._handlers[task_name] = func
            logger.debug("Registered handler for task '%s'", task_name)
            return func

        return decorator

    # ------------------------------------------------------------------
    # Worker API
    # ------------------------------------------------------------------

    def process_one(self, queue: str = "default") -> bool:
        """Fetch and process a single job from the queue.

        Args:
            queue: Queue to consume from.

        Returns:
            ``True`` if a job was processed (successfully or not),
            ``False`` if the queue was empty.

        Raises:
            TaskHandlerError: If no handler is registered for the task.
        """
        worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        job = self._fetch_one(_FETCH_JOB_SQL, (queue, worker_id))

        if job is None:
            return False

        job_id: int = int(job["id"])
        task_name: str = job["task_name"]
        payload: dict = job["payload"] if isinstance(job["payload"], dict) else {}
        attempts: int = int(job["attempts"])
        max_attempts: int = int(job["max_attempts"])

        handler = self._handlers.get(task_name)
        if handler is None:
            error_msg = f"No handler registered for task '{task_name}'"
            logger.error("Job %d: %s", job_id, error_msg)
            self._retry_or_fail(job_id, error_msg, attempts, max_attempts)
            raise TaskHandlerError(error_msg)

        try:
            handler(payload)
            self._complete_job(job_id)
            logger.debug("Job %d completed (task='%s')", job_id, task_name)
        except Exception as exc:
            logger.warning("Job %d failed (attempt %d): %s", job_id, attempts, exc)
            self._retry_or_fail(job_id, str(exc), attempts, max_attempts)

        return True

    def run_worker(
        self,
        queue: str = "default",
        poll_interval: float = 1.0,
        worker_id: str | None = None,
    ) -> None:
        """Blocking worker loop.  Handles SIGTERM/SIGINT for graceful shutdown.

        Args:
            queue: Queue to consume from.
            poll_interval: Seconds to sleep when the queue is empty.
            worker_id: Optional identifier for logging (defaults to hostname).
        """
        _wid = worker_id or socket.gethostname()
        running = True

        def _stop(signum: int, frame: Any) -> None:
            nonlocal running
            logger.info("Worker '%s' received signal %d, shutting down…", _wid, signum)
            running = False

        old_sigterm = signal.signal(signal.SIGTERM, _stop)
        old_sigint = signal.signal(signal.SIGINT, _stop)
        logger.info("Worker '%s' started on queue '%s'", _wid, queue)

        try:
            while running:
                processed = self.process_one(queue)
                if not processed:
                    time.sleep(poll_interval)
        finally:
            signal.signal(signal.SIGTERM, old_sigterm)
            signal.signal(signal.SIGINT, old_sigint)
            logger.info("Worker '%s' stopped", _wid)

    def reap_stuck(self, stuck_after_minutes: int = 5) -> int:
        """Reset jobs that have been stuck in ``running`` state too long.

        Intended to run periodically (e.g. every minute via a cron job) to
        recover from crashed workers.

        Args:
            stuck_after_minutes: Jobs locked longer than this are reset.

        Returns:
            Number of jobs reset to ``pending``.
        """
        count = self._execute(
            """
            UPDATE pg_jobs
            SET status    = 'pending',
                locked_at = NULL,
                locked_by = NULL,
                last_error = 'Reaped by reap_stuck after timeout'
            WHERE status = 'running'
              AND locked_at < NOW() - %s * interval '1 minute'
            """,
            (stuck_after_minutes,),
        )
        if count:
            logger.warning("Reaped %d stuck job(s)", count)
        return count

    def stats(self, queue: str | None = None) -> dict:
        """Return job counts grouped by status.

        Args:
            queue: Filter to a single queue.  When ``None`` aggregates all.

        Returns:
            Dict mapping status strings to integer counts, e.g.
            ``{"pending": 5, "running": 1, "completed": 42, "failed": 0}``.
        """
        if queue:
            rows = self._fetch_all(
                "SELECT status, COUNT(*) AS cnt FROM pg_jobs WHERE queue = %s GROUP BY status",
                (queue,),
            )
        else:
            rows = self._fetch_all(
                "SELECT status, COUNT(*) AS cnt FROM pg_jobs GROUP BY status"
            )
        return {row["status"]: int(row["cnt"]) for row in rows}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _complete_job(self, job_id: int) -> None:
        self._execute(
            """
            UPDATE pg_jobs
            SET status       = 'completed',
                completed_at = NOW(),
                locked_at    = NULL,
                locked_by    = NULL
            WHERE id = %s
            """,
            (job_id,),
        )

    def _retry_or_fail(
        self, job_id: int, error: str, attempts: int, max_attempts: int
    ) -> None:
        if attempts < max_attempts:
            # Exponential backoff: 2, 4, 8 … seconds.
            backoff = 2**attempts
            self._execute(
                """
                UPDATE pg_jobs
                SET status    = 'pending',
                    run_at    = NOW() + %s * interval '1 second',
                    last_error = %s,
                    locked_at = NULL,
                    locked_by = NULL
                WHERE id = %s
                """,
                (backoff, error, job_id),
            )
            logger.debug(
                "Job %d scheduled for retry in %ds (attempt %d/%d)",
                job_id,
                backoff,
                attempts,
                max_attempts,
            )
        else:
            self._execute(
                """
                UPDATE pg_jobs
                SET status    = 'failed',
                    last_error = %s,
                    locked_at = NULL,
                    locked_by = NULL
                WHERE id = %s
                """,
                (error, job_id),
            )
            logger.warning("Job %d permanently failed after %d attempts", job_id, attempts)
