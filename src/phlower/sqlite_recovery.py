"""Background recovery — rebuild in-memory aggregates from SQLite history."""

from __future__ import annotations

import logging
import time
from collections import defaultdict

from .models import TaskState
from .sqlite_store import SQLiteStore
from .store import Store

logger = logging.getLogger(__name__)

BATCH_SIZE = 10_000


def rebuild_aggregates(store: Store, sqlite_store: SQLiteStore, since_ts: float) -> int:
    """Populate TaskAggregates from SQLite skeleton data.

    Runs in a background thread while the app is already serving.
    Batches lock acquisitions to minimize contention with the event consumer.
    """
    start = time.monotonic()
    count = 0

    # Buffer rows per task, flush in batches under a single lock acquisition
    batch: list[tuple] = []

    for row in sqlite_store.load_recovery_data(since_ts):
        task_name = row["task_name"]
        state_str = row["state"]
        finished_at = row["finished_at"]

        if not task_name or finished_at is None:
            continue

        try:
            state = TaskState(state_str)
        except ValueError:
            continue

        batch.append((
            task_name,
            state,
            finished_at,
            row["runtime_ms"],
            row["worker"],
            row["queue"],
            row["exception_type"],
            row["received_at"],
            row["started_at"],
        ))

        if len(batch) >= BATCH_SIZE:
            count += _flush_batch(store, batch)
            batch.clear()

    if batch:
        count += _flush_batch(store, batch)

    elapsed = time.monotonic() - start
    logger.info("Recovery complete: %d rows in %.1fs", count, elapsed)
    return count


def _flush_batch(store: Store, batch: list[tuple]) -> int:
    """Apply a batch of rows under a single lock acquisition."""
    # Group by task to avoid repeated _get_or_create_task lookups
    by_task: dict[str, list[tuple]] = defaultdict(list)
    for row in batch:
        by_task[row[0]].append(row)

    with store._lock:
        for task_name, rows in by_task.items():
            agg = store._get_or_create_task(task_name)
            for (_, state, finished_at, runtime_ms, worker, queue,
                 exception_type, received_at, started_at) in rows:
                agg.record_terminal_event(
                    state,
                    finished_at,
                    runtime_ms=runtime_ms,
                    worker=worker,
                    queue=queue,
                    exception_type=exception_type,
                )
                if received_at is not None and started_at is not None:
                    wait_ms = (started_at - received_at) * 1000
                    q = queue or "_global"
                    store._pickup_latencies[q].append(wait_ms)

    count = len(batch)
    if count >= BATCH_SIZE:
        logger.info("Recovery progress: +%d rows", count)
    return count
