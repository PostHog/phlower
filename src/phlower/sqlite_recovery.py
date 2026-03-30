"""Startup recovery — rebuild in-memory aggregates from SQLite history."""

from __future__ import annotations

import logging
import time

from .models import TaskState
from .sqlite_store import SQLiteStore
from .store import Store

logger = logging.getLogger(__name__)


def rebuild_aggregates(store: Store, sqlite_store: SQLiteStore, since_ts: float) -> int:
    """Populate TaskAggregates from SQLite skeleton data.

    Called once at startup before the event consumer starts.
    Runs single-threaded — no locking needed.
    """
    start = time.monotonic()
    count = 0

    for row in sqlite_store.load_recovery_data(since_ts):
        task_name = row["task_name"]
        state_str = row["state"]
        finished_at = row["finished_at"]
        runtime_ms = row["runtime_ms"]
        worker = row["worker"]
        queue = row["queue"]
        exception_type = row["exception_type"]

        if not task_name or finished_at is None:
            continue

        try:
            state = TaskState(state_str)
        except ValueError:
            continue

        agg = store._get_or_create_task(task_name)
        agg.record_terminal_event(
            state,
            finished_at,
            runtime_ms=runtime_ms,
            worker=worker,
            queue=queue,
            exception_type=exception_type,
        )

        count += 1
        if count % 1_000_000 == 0:
            elapsed = time.monotonic() - start
            logger.info("Recovery progress: %dM rows in %.1fs", count // 1_000_000, elapsed)

    elapsed = time.monotonic() - start
    logger.info("Recovery complete: %d rows in %.1fs", count, elapsed)
    return count
