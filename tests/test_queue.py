from __future__ import annotations

import threading
import time


def test_enqueue_and_process_one(pg):
    results = []

    @pg.queue.register("echo")
    def echo(payload):
        results.append(payload["msg"])

    job_id = pg.queue.enqueue("echo", {"msg": "hello"})
    assert isinstance(job_id, int)

    processed = pg.queue.process_one()
    assert processed is True
    assert results == ["hello"]

    stats = pg.queue.stats()
    assert stats.get("completed", 0) == 1


def test_empty_queue_returns_false(pg):
    assert pg.queue.process_one() is False


def test_priority_ordering(pg):
    order = []

    @pg.queue.register("prio")
    def prio_handler(payload):
        order.append(payload["n"])

    pg.queue.enqueue("prio", {"n": 1}, priority=1)
    pg.queue.enqueue("prio", {"n": 10}, priority=10)
    pg.queue.enqueue("prio", {"n": 5}, priority=5)

    pg.queue.process_one()
    pg.queue.process_one()
    pg.queue.process_one()

    assert order == [10, 5, 1]


def test_failed_job_retries_with_backoff(pg):
    attempt_times: list[float] = []

    @pg.queue.register("flaky")
    def flaky(payload):
        attempt_times.append(time.time())
        raise ValueError("intentional failure")

    pg.queue.enqueue("flaky", {}, max_attempts=2)

    # First attempt — job transitions to pending with backoff.
    pg.queue.process_one()
    stats = pg.queue.stats()
    assert stats.get("pending", 0) == 1

    # Second attempt (simulate time passing by forcing run_at into the past).
    with pg._pool.connection() as conn:
        conn.execute("UPDATE pg_jobs SET run_at = NOW() - interval '10 seconds'")

    pg.queue.process_one()
    stats = pg.queue.stats()
    assert stats.get("failed", 0) == 1
    assert stats.get("pending", 0) == 0


def test_reap_stuck(pg):
    pg.queue.enqueue("ghost", {})
    # Manually force the job into 'running' with an old lock time.
    with pg._pool.connection() as conn:
        conn.execute(
            """
            UPDATE pg_jobs
            SET status = 'running',
                locked_at = NOW() - interval '10 minutes',
                locked_by = 'dead-worker'
            """
        )

    reaped = pg.queue.reap_stuck(stuck_after_minutes=5)
    assert reaped == 1

    stats = pg.queue.stats()
    assert stats.get("pending", 0) == 1


def test_delay_seconds(pg):
    pg.queue.enqueue("delayed", {}, delay_seconds=60)
    # Should not be processed immediately.
    assert pg.queue.process_one() is False


def test_concurrent_workers_no_double_processing(pg):
    """Two workers racing on the same queue must not process the same job."""
    processed_by: list[str] = []
    lock = threading.Lock()

    @pg.queue.register("race")
    def race_handler(payload):
        with lock:
            processed_by.append(payload["id"])

    for i in range(10):
        pg.queue.enqueue("race", {"id": str(i)})

    def worker():
        while pg.queue.process_one():
            pass

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Each job processed exactly once.
    assert sorted(processed_by) == [str(i) for i in range(10)]
    assert len(processed_by) == len(set(processed_by))


def test_stats_all_queues(pg):
    pg.queue.enqueue("noop", {}, queue="q1")
    pg.queue.enqueue("noop", {}, queue="q2")
    stats = pg.queue.stats()
    assert stats.get("pending", 0) == 2
