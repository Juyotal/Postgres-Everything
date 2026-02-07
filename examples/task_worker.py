"""Standalone worker process example.

Run with:
    python examples/task_worker.py
"""
from __future__ import annotations

import os
import time

from postgres_everything import PostgresEverything

DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pg_everything")

pg = PostgresEverything(DSN)
pg.init(modules=["queue"])


@pg.queue.register("send_email")
def send_email(payload: dict) -> None:
    to = payload.get("to", "unknown")
    subject = payload.get("subject", "(no subject)")
    print(f"[send_email] → {to}: {subject}")
    time.sleep(0.05)  # simulate I/O


@pg.queue.register("process_image")
def process_image(payload: dict) -> None:
    image_id = payload.get("image_id")
    width = payload.get("width", 1920)
    print(f"[process_image] resizing image {image_id} to {width}px")
    time.sleep(0.1)


@pg.queue.register("generate_report")
def generate_report(payload: dict) -> None:
    report_id = payload.get("report_id")
    print(f"[generate_report] building report {report_id}")
    if payload.get("fail"):
        raise ValueError("Simulated failure for retry demo")


if __name__ == "__main__":
    print("Worker starting… (Ctrl-C to stop)")
    pg.queue.run_worker(queue="default", poll_interval=1.0)
    pg.close()
