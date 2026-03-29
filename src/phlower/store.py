"""In-memory bounded store for task aggregates and invocation records."""

from __future__ import annotations

import random
import threading
import time
from collections import Counter, defaultdict, deque
from itertools import islice

from .config import Config
from .models import InvocationRecord, MinuteBucket, TaskState, TaskSummary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def percentile(sorted_values: list[float], p: float) -> float | None:
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
    def __init__(
        self,
        task_name: str,
        max_runtime_buffer: int,
        max_runtimes_per_bucket: int,
    ) -> None:
        self.task_name = task_name
        self.buckets: dict[int, MinuteBucket] = {}
        self.active_count: int = 0
        self.runtime_buffer: deque[float] = deque(maxlen=max_runtime_buffer)
        self._sorted_cache: list[float] | None = None
        self.exceptions: Counter[str] = Counter()
        self.workers: Counter[str] = Counter()
        self.queues: Counter[str] = Counter()
        self._max_runtimes_per_bucket = max_runtimes_per_bucket

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
            self.runtime_buffer.append(runtime_ms)
            self._sorted_cache = None
            if len(bucket.runtimes) < self._max_runtimes_per_bucket:
                bucket.runtimes.append(runtime_ms)

        if worker:
            self.workers[worker] += 1
        if queue:
            self.queues[queue] += 1
        if exception_type:
            self.exceptions[exception_type] += 1

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

        if self._sorted_cache is None:
            self._sorted_cache = sorted(self.runtime_buffer) if self.runtime_buffer else []
        sr = self._sorted_cache

        return TaskSummary(
            task_name=self.task_name,
            total_count=total,
            success_count=success,
            failure_count=failure,
            retry_count=retry,
            active_count=self.active_count,
            failure_rate=failure / total if total else 0.0,
            p50_ms=percentile(sr, 50),
            p95_ms=percentile(sr, 95),
            p99_ms=percentile(sr, 99),
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
            sr = sorted(b.runtimes) if b.runtimes else []
            out.append(
                {
                    "t": ts,
                    "count": b.count,
                    "success": b.success,
                    "failure": b.failure,
                    "retry": b.retry,
                    "p50": percentile(sr, 50),
                    "p95": percentile(sr, 95),
                    "p99": percentile(sr, 99),
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

    def __init__(self, config: Config) -> None:
        self.config = config
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

    # -- internal helpers (call with lock held) ---------------------------

    def _should_track(self, task_name: str) -> bool:
        return bool(self.config.task_allowlist_regex.match(task_name))

    def _should_store_invocation(self, record: InvocationRecord) -> bool:
        if record.state in (TaskState.FAILURE, TaskState.RETRY):
            return True
        if record.task_name in self.config.task_watchlist:
            return True
        if record.state == TaskState.SUCCESS:
            return random.random() < self.config.success_sample_rate
        # RECEIVED / STARTED are kept until terminal state decides
        return True

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
            agg = TaskAggregate(
                task_name,
                self.config.max_runtime_buffer,
                self.config.max_runtimes_per_bucket,
            )
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
                old = rec.task_name
                rec.task_name = task_name
                old_dq = self.invocations_by_task.get(old)
                if old_dq:
                    try:
                        old_dq.remove(task_id)
                    except ValueError:
                        pass
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
            self._get_or_create_task(task_name)
            rec = self._ensure_record(task_id, task_name)
            rec.received_at = ts
            rec.queue = queue
            rec.args_preview = (
                args[: self.config.max_args_preview_chars] if args else None
            )
            rec.kwargs_preview = (
                kwargs[: self.config.max_kwargs_preview_chars] if kwargs else None
            )
            rec.transitions.append((TaskState.RECEIVED, ts))
            self._dirty_tasks.add(task_name)

    def process_started(
        self,
        task_id: str,
        ts: float,
        *,
        task_name: str | None = None,
        worker: str | None = None,
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
            rec.transitions.append((TaskState.STARTED, ts))
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

            if not self._should_store_invocation(rec):
                self.invocations.pop(task_id, None)
            else:
                self._new_invocation_ids.append(task_id)

            self._dirty_tasks.add(name)

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

    # -- periodic maintenance ---------------------------------------------

    def evict_expired(self) -> None:
        cutoff = int(time.time()) - self.config.retention_hours * 3600
        cutoff_minute = cutoff // 60 * 60

        with self._lock:
            for agg in self.tasks.values():
                agg.evict_old_buckets(cutoff_minute)

            # _invocation_order is oldest-first — pop from front until
            # we hit a non-expired record. O(evicted) instead of O(total).
            # Per-task deque entries become stale but readers already
            # tolerate missing IDs via `if tid in self.invocations`.
            while self._invocation_order:
                task_id = self._invocation_order[0]
                rec = self.invocations.get(task_id)
                if rec is None:
                    self._invocation_order.popleft()
                    continue
                ts = rec.finished_at or rec.started_at or rec.received_at
                if ts is not None and ts < cutoff:
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

    def get_task_latency(self, task_name: str) -> list[dict] | None:
        with self._lock:
            agg = self.tasks.get(task_name)
            return agg.latency_series() if agg else None

    def get_task_invocations(
        self, task_name: str, *, limit: int = 50, offset: int = 0
    ) -> list[InvocationRecord]:
        with self._lock:
            dq = self.invocations_by_task.get(task_name, deque())
            ids = list(islice(reversed(dq), offset, offset + limit))
            return [self.invocations[tid] for tid in ids if tid in self.invocations]

    def get_invocation(self, task_id: str) -> InvocationRecord | None:
        with self._lock:
            return self.invocations.get(task_id)

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
                return [rec] if rec else []

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
