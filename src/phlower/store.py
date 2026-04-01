"""In-memory bounded store for task aggregates and invocation records."""

from __future__ import annotations

import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from itertools import islice
from typing import TYPE_CHECKING

from fastdigest import TDigest

from .config import Config
from .models import InvocationRecord, MinuteBucket, TaskState, TaskSummary

if TYPE_CHECKING:
    from .sqlite_store import SQLiteStore


@dataclass(frozen=True, slots=True)
class CompletedRecord:
    """Snapshot for SQLite persistence — includes heavy fields for recent detail."""

    task_id: str
    task_name: str
    state: str
    received_at: float | None
    started_at: float | None
    finished_at: float | None
    runtime_ms: float | None
    worker: str | None
    queue: str | None
    exception_type: str | None
    args_preview: str | None
    kwargs_preview: str | None
    traceback_snippet: str | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _percentile_sorted(sorted_values: list[float], p: float) -> float | None:
    """Compute *p*-th percentile from a pre-sorted list (linear interpolation)."""
    n = len(sorted_values)
    if n == 0:
        return None
    k = (n - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= n:
        return sorted_values[f]
    return sorted_values[f] + (k - f) * (sorted_values[c] - sorted_values[f])


# ---------------------------------------------------------------------------
# Per-task aggregate
# ---------------------------------------------------------------------------


class TaskAggregate:
    def __init__(self, task_name: str) -> None:
        self.task_name = task_name
        self.buckets: dict[int, MinuteBucket] = {}
        self.active_count: int = 0
        self.runtime_digest: TDigest = TDigest()
        self.exceptions: Counter[str] = Counter()
        self.workers: Counter[str] = Counter()
        self.queues: Counter[str] = Counter()

    # -- mutations --------------------------------------------------------

    def _get_or_create_bucket(self, ts: float) -> MinuteBucket:
        minute_ts = int(ts) // 60 * 60
        bucket = self.buckets.get(minute_ts)
        if bucket is None:
            bucket = MinuteBucket(timestamp=minute_ts)
            self.buckets[minute_ts] = bucket
        return bucket

    def record_terminal_event(
        self,
        state: TaskState,
        ts: float,
        *,
        runtime_ms: float | None = None,
        worker: str | None = None,
        queue: str | None = None,
        exception_type: str | None = None,
    ) -> None:
        bucket = self._get_or_create_bucket(ts)
        bucket.count += 1

        if state == TaskState.SUCCESS:
            bucket.success += 1
        elif state == TaskState.FAILURE:
            bucket.failure += 1
        elif state == TaskState.RETRY:
            bucket.retry += 1

        if runtime_ms is not None:
            self.runtime_digest.update(runtime_ms)
            if bucket.digest is None:
                bucket.digest = TDigest()
            bucket.digest.update(runtime_ms)

        if worker:
            self.workers[worker] += 1
        if queue:
            self.queues[queue] += 1
        if exception_type:
            self.exceptions[exception_type] += 1

    def thin_old_buckets(self, cutoff_minute_ts: int) -> None:
        """Strip runtime digests from buckets older than cutoff. Keeps counters."""
        for ts, bucket in self.buckets.items():
            if ts < cutoff_minute_ts and bucket.digest is not None:
                bucket.digest = None

    def evict_old_buckets(self, cutoff_minute_ts: int) -> None:
        stale = [ts for ts in self.buckets if ts < cutoff_minute_ts]
        for ts in stale:
            del self.buckets[ts]

    # -- reads ------------------------------------------------------------

    def _recent_rate(self, window_minutes: int = 5) -> float:
        """Average tasks/min over the last N minutes."""
        now = int(time.time()) // 60 * 60
        total = sum(
            self.buckets[ts].count
            for ts in range(now - (window_minutes - 1) * 60, now + 60, 60)
            if ts in self.buckets
        )
        return total / window_minutes

    def summary(self) -> TaskSummary:
        total = success = failure = retry = 0
        for b in self.buckets.values():
            total += b.count
            success += b.success
            failure += b.failure
            retry += b.retry

        d = self.runtime_digest
        has_data = len(d) > 0

        return TaskSummary(
            task_name=self.task_name,
            total_count=total,
            success_count=success,
            failure_count=failure,
            retry_count=retry,
            active_count=self.active_count,
            failure_rate=failure / total if total else 0.0,
            p50_ms=d.percentile(50) if has_data else None,
            p95_ms=d.percentile(95) if has_data else None,
            p99_ms=d.percentile(99) if has_data else None,
            mean_ms=d.mean() if has_data else None,
            min_ms=d.min() if has_data else None,
            max_ms=d.max() if has_data else None,
            std_ms=d.std() if has_data else None,
            rate_per_min=self._recent_rate(),
            top_exceptions=self.exceptions.most_common(10),
            top_workers=self.workers.most_common(10),
            top_queues=self.queues.most_common(10),
            sparkline=self.sparkline(),
        )

    def sparkline(self, minutes: int = 60) -> list[int]:
        """Last N minutes of throughput counts for an inline sparkline."""
        now = int(time.time()) // 60 * 60
        return [
            self.buckets[ts].count if ts in self.buckets else 0
            for ts in range(now - (minutes - 1) * 60, now + 60, 60)
        ]

    def latency_series(self) -> list[dict]:
        """Per-minute latency + throughput data suitable for charting."""
        out: list[dict] = []
        for ts in sorted(self.buckets):
            b = self.buckets[ts]
            d = b.digest
            has_data = d is not None and len(d) > 0
            out.append(
                {
                    "t": ts,
                    "count": b.count,
                    "success": b.success,
                    "failure": b.failure,
                    "retry": b.retry,
                    "p50": d.percentile(50) if has_data else None,
                    "p95": d.percentile(95) if has_data else None,
                    "p99": d.percentile(99) if has_data else None,
                    "failure_rate": b.failure / b.count if b.count else 0.0,
                }
            )
        return out


# ---------------------------------------------------------------------------
# Global store
# ---------------------------------------------------------------------------


class Store:
    """Thread-safe in-memory store shared between the Celery consumer thread
    and the async FastAPI request handlers."""

    def __init__(self, config: Config, sqlite_store: SQLiteStore | None = None) -> None:
        self.config = config
        self.sqlite_store = sqlite_store
        self._lock = threading.Lock()

        # task_name → aggregate
        self.tasks: dict[str, TaskAggregate] = {}

        # task_id → record
        self.invocations: dict[str, InvocationRecord] = {}
        # task_name → deque of task_ids (insertion order)
        self.invocations_by_task: dict[str, deque[str]] = defaultdict(deque)
        # global insertion order for eviction
        self._invocation_order: deque[str] = deque()

        # dirty tracking for throttled SSE push
        self._dirty_tasks: set[str] = set()
        self._new_invocation_ids: list[str] = []

        # rolling event counter for tasks/sec display
        self._event_timestamps: deque[float] = deque(maxlen=2000)

        # SQLite write-behind buffer (CompletedRecords, snapshotted at completion)
        self._sqlite_pending: list[CompletedRecord] = []

        # pickup latency (received→started) per queue, rolling buffer
        self._pickup_latencies: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=500)
        )

    # -- internal helpers (call with lock held) ---------------------------

    def _should_track(self, task_name: str) -> bool:
        return bool(self.config.task_allowlist_regex.match(task_name))

    @staticmethod
    def _snapshot(rec: InvocationRecord) -> CompletedRecord:
        return CompletedRecord(
            task_id=rec.task_id,
            task_name=rec.task_name,
            state=rec.state.value,
            received_at=rec.received_at,
            started_at=rec.started_at,
            finished_at=rec.finished_at,
            runtime_ms=rec.runtime_ms,
            worker=rec.worker,
            queue=rec.queue,
            exception_type=rec.exception_type,
            args_preview=rec.args_preview,
            kwargs_preview=rec.kwargs_preview,
            traceback_snippet=rec.traceback_snippet,
        )


    def _evict_global(self) -> None:
        while len(self.invocations) > self.config.max_global_invocations:
            oldest_id = self._invocation_order.popleft()
            self.invocations.pop(oldest_id, None)
            # Per-task deque entries become stale — readers filter via
            # `if tid in self.invocations` so no O(n) dq.remove() needed.

    def _evict_per_task(self, task_name: str) -> None:
        dq = self.invocations_by_task.get(task_name)
        if not dq:
            return
        while len(dq) > self.config.max_invocations_per_task:
            old_id = dq.popleft()
            self.invocations.pop(old_id, None)

    def _get_or_create_task(self, task_name: str) -> TaskAggregate:
        agg = self.tasks.get(task_name)
        if agg is None:
            agg = TaskAggregate(task_name)
            self.tasks[task_name] = agg
        return agg

    def _resolve_name(self, task_id: str, event_name: str | None) -> str:
        """Best-effort task name: event field → stored record → 'unknown'."""
        if event_name:
            return event_name
        rec = self.invocations.get(task_id)
        if rec:
            return rec.task_name
        return "unknown"

    def _ensure_record(self, task_id: str, task_name: str) -> InvocationRecord:
        rec = self.invocations.get(task_id)
        if rec is not None:
            # upgrade name if we now have it
            if task_name != "unknown" and rec.task_name == "unknown":
                rec.task_name = task_name
                # Don't dq.remove from "unknown" (O(n)) — stale entries are
                # filtered on read via `if tid in self.invocations` check.
                self.invocations_by_task[task_name].append(task_id)
            return rec

        rec = InvocationRecord(
            task_id=task_id,
            task_name=task_name,
            state=TaskState.RECEIVED,
        )
        self.invocations[task_id] = rec
        self.invocations_by_task[task_name].append(task_id)
        self._invocation_order.append(task_id)
        self._evict_global()
        self._evict_per_task(task_name)
        return rec

    # -- write methods (called from Celery consumer thread) ---------------

    def process_received(
        self,
        task_id: str,
        task_name: str,
        ts: float,
        *,
        args: str | None = None,
        kwargs: str | None = None,
        queue: str | None = None,
    ) -> None:
        with self._lock:
            if not self._should_track(task_name):
                return

            # Check if this record was already filed under "unknown"
            # with a terminal state (prefork event ordering issue)
            old_rec = self.invocations.get(task_id)
            was_unknown_terminal = (
                old_rec is not None
                and old_rec.task_name == "unknown"
                and old_rec.state in (TaskState.SUCCESS, TaskState.FAILURE, TaskState.RETRY)
            )

            agg = self._get_or_create_task(task_name)
            rec = self._ensure_record(task_id, task_name)
            rec.received_at = ts
            rec.queue = queue

            # Re-apply terminal event to the correct aggregate
            if was_unknown_terminal:
                agg.record_terminal_event(
                    rec.state,
                    rec.finished_at or ts,
                    runtime_ms=rec.runtime_ms,
                    worker=rec.worker,
                    queue=rec.queue,
                    exception_type=rec.exception_type,
                )
            rec.args_preview = (
                args[: self.config.max_args_preview_chars] if args else None
            )
            rec.kwargs_preview = (
                kwargs[: self.config.max_kwargs_preview_chars] if kwargs else None
            )
            rec.transitions.append((TaskState.RECEIVED, ts))
            self._dirty_tasks.add(task_name)
            self.record_event()

    def process_started(
        self,
        task_id: str,
        ts: float,
        *,
        task_name: str | None = None,
        worker: str | None = None,
        worker_group: str | None = None,
        queue: str | None = None,
    ) -> None:
        with self._lock:
            name = self._resolve_name(task_id, task_name)
            if not self._should_track(name):
                return
            agg = self._get_or_create_task(name)
            agg.active_count += 1
            rec = self._ensure_record(task_id, name)
            rec.state = TaskState.STARTED
            rec.started_at = ts
            rec.worker = worker
            rec.worker_group = worker_group
            if queue and not rec.queue:
                rec.queue = queue
            rec.transitions.append((TaskState.STARTED, ts))
            # Track pickup latency (time spent waiting in queue)
            if rec.received_at is not None and not rec.transitions[0][0] == TaskState.STARTED:
                wait_ms = (ts - rec.received_at) * 1000
                q = rec.queue or "_global"
                self._pickup_latencies[q].append(wait_ms)
            self._dirty_tasks.add(name)

    def process_succeeded(
        self,
        task_id: str,
        ts: float,
        *,
        task_name: str | None = None,
        runtime_ms: float | None = None,
    ) -> None:
        with self._lock:
            name = self._resolve_name(task_id, task_name)
            if not self._should_track(name):
                return
            agg = self._get_or_create_task(name)
            agg.active_count = max(0, agg.active_count - 1)

            rec = self._ensure_record(task_id, name)
            worker = rec.worker
            queue = rec.queue
            agg.record_terminal_event(
                TaskState.SUCCESS, ts, runtime_ms=runtime_ms, worker=worker, queue=queue
            )

            rec.state = TaskState.SUCCESS
            rec.finished_at = ts
            rec.runtime_ms = runtime_ms
            rec.transitions.append((TaskState.SUCCESS, ts))
            self._new_invocation_ids.append(task_id)
            self._dirty_tasks.add(name)
            self._sqlite_pending.append(self._snapshot(rec))

    def process_failed(
        self,
        task_id: str,
        ts: float,
        *,
        task_name: str | None = None,
        runtime_ms: float | None = None,
        exception_type: str | None = None,
        exception_message: str | None = None,
        traceback_snippet: str | None = None,
    ) -> None:
        with self._lock:
            name = self._resolve_name(task_id, task_name)
            if not self._should_track(name):
                return
            agg = self._get_or_create_task(name)
            agg.active_count = max(0, agg.active_count - 1)

            rec = self._ensure_record(task_id, name)
            worker = rec.worker
            queue = rec.queue
            agg.record_terminal_event(
                TaskState.FAILURE,
                ts,
                runtime_ms=runtime_ms,
                worker=worker,
                queue=queue,
                exception_type=exception_type,
            )

            rec.state = TaskState.FAILURE
            rec.finished_at = ts
            if runtime_ms is None and rec.started_at is not None:
                runtime_ms = (ts - rec.started_at) * 1000
            rec.runtime_ms = runtime_ms
            rec.exception_type = exception_type
            rec.exception_message = exception_message
            rec.traceback_snippet = traceback_snippet
            rec.transitions.append((TaskState.FAILURE, ts))
            self._new_invocation_ids.append(task_id)
            self._dirty_tasks.add(name)
            self._sqlite_pending.append(self._snapshot(rec))

    def process_retried(
        self,
        task_id: str,
        ts: float,
        *,
        task_name: str | None = None,
        exception_type: str | None = None,
        exception_message: str | None = None,
        traceback_snippet: str | None = None,
    ) -> None:
        with self._lock:
            name = self._resolve_name(task_id, task_name)
            if not self._should_track(name):
                return
            agg = self._get_or_create_task(name)

            rec = self._ensure_record(task_id, name)
            queue = rec.queue
            agg.record_terminal_event(TaskState.RETRY, ts, exception_type=exception_type, queue=queue)

            rec.state = TaskState.RETRY
            rec.retries += 1
            rec.exception_type = exception_type
            rec.exception_message = exception_message
            rec.traceback_snippet = traceback_snippet
            rec.transitions.append((TaskState.RETRY, ts))
            self._new_invocation_ids.append(task_id)
            self._dirty_tasks.add(name)
            self._sqlite_pending.append(self._snapshot(rec))

    # -- periodic maintenance ---------------------------------------------

    def evict_expired(self) -> None:
        now = int(time.time())
        inv_cutoff = now - self.config.retention_hours * 3600
        agg_cutoff = now - self.config.aggregate_retention_hours * 3600
        agg_cutoff_minute = agg_cutoff // 60 * 60

        thin_cutoff_minute = (inv_cutoff // 60) * 60

        with self._lock:
            for agg in self.tasks.values():
                # Strip runtimes from buckets older than 48h (saves ~4KB/bucket)
                agg.thin_old_buckets(thin_cutoff_minute)
                # Delete buckets older than 7 days entirely
                agg.evict_old_buckets(agg_cutoff_minute)

            # Invocations: shorter retention (default 48h)
            while self._invocation_order:
                task_id = self._invocation_order[0]
                rec = self.invocations.get(task_id)
                if rec is None:
                    self._invocation_order.popleft()
                    continue
                ts = rec.finished_at or rec.started_at or rec.received_at
                if ts is not None and ts < inv_cutoff:
                    self._invocation_order.popleft()
                    self.invocations.pop(task_id, None)
                else:
                    break

    # -- SSE dirty tracking -----------------------------------------------

    def flush_dirty(self) -> tuple[set[str], list[str]]:
        with self._lock:
            tasks = self._dirty_tasks
            invocations = self._new_invocation_ids
            self._dirty_tasks = set()
            self._new_invocation_ids = []
            return tasks, invocations

    def drain_completed_for_sqlite(self) -> list[CompletedRecord]:
        """Pop pending completed records for SQLite flush."""
        with self._lock:
            records = self._sqlite_pending
            self._sqlite_pending = []
            return records

    def record_event(self) -> None:
        """Track an incoming event for rate computation."""
        self._event_timestamps.append(time.time())

    def events_per_second(self, window: float = 1.0) -> float:
        """Events/sec over the last N seconds. Default 1s for raw jittery feel."""
        now = time.time()
        cutoff = now - window
        # Trim old entries from the left
        while self._event_timestamps and self._event_timestamps[0] < cutoff:
            self._event_timestamps.popleft()
        count = len(self._event_timestamps)
        return count / window if count else 0.0

    def get_sparkline_points(self) -> dict[str, int]:
        """Latest minute count per task for sparkline push."""
        now_minute = int(time.time()) // 60 * 60
        with self._lock:
            return {
                name: (agg.buckets[now_minute].count if now_minute in agg.buckets else 0)
                for name, agg in self.tasks.items()
            }

    def pickup_latency_by_queue(self) -> dict[str, float | None]:
        """p95 pickup latency (ms) per queue from recent data."""
        with self._lock:
            result: dict[str, float | None] = {}
            for q, latencies in self._pickup_latencies.items():
                if q == "_global":
                    continue
                sr = sorted(latencies) if latencies else []
                result[q] = _percentile_sorted(sr, 95)
            return result

    # -- read methods (called from async handlers) ------------------------

    def get_task_list(self) -> list[TaskSummary]:
        with self._lock:
            summaries = [agg.summary() for agg in self.tasks.values()]
        summaries.sort(key=lambda s: s.task_name)
        return summaries

    def get_task_summary(self, task_name: str) -> TaskSummary | None:
        with self._lock:
            agg = self.tasks.get(task_name)
            return agg.summary() if agg else None

    def get_task_summaries(self, names: set[str]) -> list[TaskSummary]:
        """Bulk summary — one lock acquisition for N tasks."""
        with self._lock:
            return [
                agg.summary()
                for name in names
                if (agg := self.tasks.get(name)) is not None
            ]

    def get_task_latency(self, task_name: str) -> list[dict] | None:
        with self._lock:
            agg = self.tasks.get(task_name)
            return agg.latency_series() if agg else None

    def get_task_invocations(
        self,
        task_name: str,
        *,
        limit: int = 100,
        before_ts: float | None = None,
        after_ts: float | None = None,
    ) -> list[InvocationRecord]:
        with self._lock:
            dq = self.invocations_by_task.get(task_name, deque())
            results: list[InvocationRecord] = []
            for tid in reversed(dq):
                rec = self.invocations.get(tid)
                if rec is None:
                    continue
                ts = rec.received_at or rec.started_at or 0.0
                if before_ts is not None and ts >= before_ts:
                    continue
                if after_ts is not None and ts <= after_ts:
                    break  # older than cursor, stop (list is newest-first)
                results.append(rec)
                if len(results) >= limit:
                    break

        # Fill remaining slots from SQLite if in-memory didn't have enough
        remaining = limit - len(results)
        if remaining > 0 and self.sqlite_store is not None:
            seen = {r.task_id for r in results}
            sqlite_results = self.sqlite_store.list_by_task(
                task_name,
                limit=remaining,
                before_ts=before_ts,
                after_ts=after_ts,
                exclude_ids=seen,
            )
            results.extend(sqlite_results)

        return results

    def get_invocation(self, task_id: str) -> InvocationRecord | None:
        with self._lock:
            rec = self.invocations.get(task_id)
        if rec is not None:
            return rec
        if self.sqlite_store is not None:
            return self.sqlite_store.lookup_task_id(task_id)
        return None

    def get_known_queues(self) -> list[str]:
        with self._lock:
            queues: set[str] = set()
            for agg in self.tasks.values():
                queues.update(agg.queues.keys())
            return sorted(queues)

    def get_known_workers(self) -> list[str]:
        with self._lock:
            workers: set[str] = set()
            for agg in self.tasks.values():
                workers.update(agg.workers.keys())
            return sorted(workers)

    def search_invocations(
        self,
        *,
        task_name: str | None = None,
        status: str | None = None,
        worker: str | None = None,
        queue: str | None = None,
        task_id: str | None = None,
        q: str | None = None,
        time_from: float | None = None,
        time_to: float | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[InvocationRecord]:
        with self._lock:
            if task_id:
                rec = self.invocations.get(task_id)
                if rec:
                    return [rec]
                # Fall through to SQLite for historical lookup
                if self.sqlite_store is not None:
                    sqlite_rec = self.sqlite_store.lookup_task_id(task_id)
                    if sqlite_rec:
                        return [sqlite_rec]
                return []

            # Iterate newest-first via _invocation_order (already time-ordered).
            # Early exit after offset+limit matches — avoids full scan + sort.
            q_lower = q.lower() if q else None
            status_upper = status.upper() if status else None
            results: list[InvocationRecord] = []
            skipped = 0

            for tid in reversed(self._invocation_order):
                rec = self.invocations.get(tid)
                if rec is None:
                    continue
                if task_name and rec.task_name != task_name:
                    continue
                if status_upper and rec.state.value != status_upper:
                    continue
                if worker and rec.worker != worker:
                    continue
                if queue and rec.queue != queue:
                    continue
                ts = rec.received_at or 0.0
                if time_from and ts < time_from:
                    continue
                if time_to and ts > time_to:
                    continue
                if q_lower:
                    haystack = " ".join(
                        filter(
                            None,
                            (
                                rec.task_id,
                                rec.task_name,
                                rec.args_preview,
                                rec.kwargs_preview,
                                rec.exception_type,
                                rec.exception_message,
                                rec.worker,
                                rec.queue,
                            ),
                        )
                    ).lower()
                    if q_lower not in haystack:
                        continue
                if skipped < offset:
                    skipped += 1
                    continue
                results.append(rec)
                if len(results) >= limit:
                    break

            return results
