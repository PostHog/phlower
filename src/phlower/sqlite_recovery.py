"""Background recovery — rebuild in-memory aggregates from SQLite history."""

from __future__ import annotations

import logging
import time
from collections import defaultdict

from fastdigest import TDigest

from .models import MinuteBucket, TaskState
from .sqlite_store import SQLiteStore
from .store import Store

logger = logging.getLogger(__name__)

BATCH_SIZE = 100_000


def rebuild_aggregates(store: Store, sqlite_store: SQLiteStore, since_ts: float) -> int:
    """Populate TaskAggregates from SQLite using SQL aggregation.

    Runs in a background thread while the app is already serving.
    Three phases:
      1. Counts — GROUP BY for bucket counters and worker/queue/exception tallies
      2. Runtimes — stream runtime_ms into t-digests (global + per-bucket)
      3. Pickup latency — recent received→started wait times
    """
    start = time.monotonic()

    # Phase 1: aggregated counts
    count_rows = _load_counts(store, sqlite_store, since_ts)
    elapsed_counts = time.monotonic() - start
    logger.info("Recovery phase 1 (counts): %d grouped rows in %.1fs", count_rows, elapsed_counts)

    # Phase 2: runtime digests
    runtime_rows = _load_runtimes(store, sqlite_store, since_ts)
    elapsed_runtimes = time.monotonic() - start - elapsed_counts
    logger.info("Recovery phase 2 (runtimes): %d rows in %.1fs", runtime_rows, elapsed_runtimes)

    # Phase 3: pickup latency
    pickup_rows = _load_pickup_latency(store, sqlite_store, since_ts)
    logger.info("Recovery phase 3 (pickup latency): %d rows", pickup_rows)

    elapsed = time.monotonic() - start
    logger.info("Recovery complete: %d count groups + %d runtimes in %.1fs", count_rows, runtime_rows, elapsed)
    return count_rows


def _load_counts(store: Store, sqlite_store: SQLiteStore, since_ts: float) -> int:
    """Load pre-aggregated counts into task aggregates."""
    batch: list[tuple] = []
    total = 0

    for row in sqlite_store.load_recovery_counts(since_ts):
        task_name = row["task_name"]
        state_str = row["state"]

        try:
            state = TaskState(state_str)
        except ValueError:
            continue

        batch.append((
            task_name,
            state,
            row["minute_ts"],
            row["cnt"],
            row["worker"],
            row["queue"],
            row["exception_type"],
        ))

        if len(batch) >= BATCH_SIZE:
            _flush_counts(store, batch)
            total += len(batch)
            batch.clear()

    if batch:
        _flush_counts(store, batch)
        total += len(batch)

    return total


def _flush_counts(store: Store, batch: list[tuple]) -> None:
    """Apply count rows under a single lock acquisition."""
    with store._lock:
        for (task_name, state, minute_ts, cnt, worker, queue, exception_type) in batch:
            agg = store._get_or_create_task(task_name)
            bucket = agg._get_or_create_bucket(minute_ts)
            bucket.count += cnt

            if state == TaskState.SUCCESS:
                bucket.success += cnt
            elif state == TaskState.FAILURE:
                bucket.failure += cnt
            elif state == TaskState.RETRY:
                bucket.retry += cnt

            if worker:
                agg.workers[worker] += cnt
            if queue:
                agg.queues[queue] += cnt
            if exception_type:
                agg.exceptions[exception_type] += cnt


def _load_runtimes(store: Store, sqlite_store: SQLiteStore, since_ts: float) -> int:
    """Stream runtime values into t-digests using batch_update."""
    # Group runtimes by task (and by bucket for recent data)
    by_task: dict[str, list[float]] = defaultdict(list)
    by_bucket: dict[str, dict[int, list[float]]] = defaultdict(lambda: defaultdict(list))
    bucket_cutoff = int(time.time()) - 48 * 3600
    total = 0

    for row in sqlite_store.load_recovery_runtimes(since_ts):
        task_name = row["task_name"]
        minute_ts = row["minute_ts"]
        runtime_ms = row["runtime_ms"]

        by_task[task_name].append(runtime_ms)
        if minute_ts >= bucket_cutoff:
            by_bucket[task_name][minute_ts].append(runtime_ms)
        total += 1

        # Flush periodically to avoid unbounded memory
        if total % 1_000_000 == 0:
            _flush_runtimes(store, by_task, by_bucket)
            by_task.clear()
            by_bucket.clear()
            logger.info("Recovery runtimes progress: %dM rows", total // 1_000_000)

    if by_task:
        _flush_runtimes(store, by_task, by_bucket)

    return total


def _flush_runtimes(
    store: Store,
    by_task: dict[str, list[float]],
    by_bucket: dict[str, dict[int, list[float]]],
) -> None:
    """Batch-update t-digests under a single lock acquisition."""
    with store._lock:
        for task_name, values in by_task.items():
            agg = store._get_or_create_task(task_name)
            agg.runtime_digest.batch_update(values)

        for task_name, buckets in by_bucket.items():
            agg = store.tasks.get(task_name)
            if agg is None:
                continue
            for minute_ts, values in buckets.items():
                bucket = agg.buckets.get(minute_ts)
                if bucket is not None:
                    if bucket.digest is None:
                        bucket.digest = TDigest()
                    bucket.digest.batch_update(values)


def _load_pickup_latency(store: Store, sqlite_store: SQLiteStore, since_ts: float) -> int:
    """Rebuild pickup latency from recent data."""
    count = 0
    with store._lock:
        for row in sqlite_store.load_recovery_pickup(since_ts):
            queue = row["queue"] or "_global"
            store._pickup_latencies[queue].append(row["wait_ms"])
            count += 1
    return count
