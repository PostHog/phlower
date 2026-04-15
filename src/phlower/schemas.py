"""Pydantic response schemas — single source of truth for the API contract."""

from __future__ import annotations

from pydantic import BaseModel


# -- shared sub-models -------------------------------------------------------

class ExceptionCount(BaseModel):
    type: str
    count: int


class WorkerCount(BaseModel):
    worker: str
    count: int


class QueueCount(BaseModel):
    queue: str
    count: int


class Transition(BaseModel):
    state: str
    ts: float


# -- task summary ------------------------------------------------------------

class TaskSummaryResponse(BaseModel):
    task_name: str
    total_count: int
    success_count: int
    failure_count: int
    retry_count: int
    active_count: int
    failure_rate: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    mean_ms: float | None
    min_ms: float | None
    max_ms: float | None
    std_ms: float | None
    rate_per_min: float
    top_exceptions: list[ExceptionCount]
    top_workers: list[WorkerCount]
    top_queues: list[QueueCount]
    sparkline: list[int]

    @classmethod
    def from_internal(cls, s) -> TaskSummaryResponse:
        return cls(
            task_name=s.task_name,
            total_count=s.total_count,
            success_count=s.success_count,
            failure_count=s.failure_count,
            retry_count=s.retry_count,
            active_count=s.active_count,
            failure_rate=s.failure_rate,
            p50_ms=s.p50_ms,
            p95_ms=s.p95_ms,
            p99_ms=s.p99_ms,
            mean_ms=s.mean_ms,
            min_ms=s.min_ms,
            max_ms=s.max_ms,
            std_ms=s.std_ms,
            rate_per_min=s.rate_per_min,
            top_exceptions=[ExceptionCount(type=t, count=c) for t, c in s.top_exceptions],
            top_workers=[WorkerCount(worker=w, count=c) for w, c in s.top_workers],
            top_queues=[QueueCount(queue=q, count=c) for q, c in s.top_queues],
            sparkline=s.sparkline,
        )


class SlimSummary(BaseModel):
    """Lightweight summary for SSE — no sparkline, no top lists."""
    task_name: str
    total_count: int
    success_count: int
    failure_count: int
    retry_count: int
    active_count: int
    failure_rate: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    mean_ms: float | None
    min_ms: float | None
    max_ms: float | None
    std_ms: float | None
    rate_per_min: float

    @classmethod
    def from_internal(cls, s) -> SlimSummary:
        return cls(
            task_name=s.task_name,
            total_count=s.total_count,
            success_count=s.success_count,
            failure_count=s.failure_count,
            retry_count=s.retry_count,
            active_count=s.active_count,
            failure_rate=s.failure_rate,
            p50_ms=s.p50_ms,
            p95_ms=s.p95_ms,
            p99_ms=s.p99_ms,
            mean_ms=s.mean_ms,
            min_ms=s.min_ms,
            max_ms=s.max_ms,
            std_ms=s.std_ms,
            rate_per_min=s.rate_per_min,
        )


# -- invocation record -------------------------------------------------------

class InvocationResponse(BaseModel):
    task_id: str
    task_name: str
    state: str
    received_at: float | None
    started_at: float | None
    finished_at: float | None
    runtime_ms: float | None
    worker: str | None
    queue: str | None
    args_preview: str | None
    kwargs_preview: str | None
    exception_type: str | None
    exception_message: str | None
    traceback_snippet: str | None
    retries: int
    transitions: list[Transition]

    @classmethod
    def from_internal(cls, r) -> InvocationResponse:
        return cls(
            task_id=r.task_id,
            task_name=r.task_name,
            state=r.state.value,
            received_at=r.received_at,
            started_at=r.started_at,
            finished_at=r.finished_at,
            runtime_ms=r.runtime_ms,
            worker=r.worker,
            queue=r.queue,
            args_preview=r.args_preview,
            kwargs_preview=r.kwargs_preview,
            exception_type=r.exception_type,
            exception_message=r.exception_message,
            traceback_snippet=r.traceback_snippet,
            retries=r.retries,
            transitions=[Transition(state=s, ts=t) for s, t in r.transitions],
        )


# -- latency point -----------------------------------------------------------

class LatencyPoint(BaseModel):
    t: int
    count: int
    success: int
    failure: int
    retry: int
    p50: float | None
    p95: float | None
    p99: float | None
    failure_rate: float


# -- meta / health -----------------------------------------------------------

class MetaResponse(BaseModel):
    queues: list[str]
    worker_groups: list[str]
    workers_seen: int
    last_inspect_at: float
    pickup_latency_p95: dict[str, float | None]
    workers_per_queue: dict[str, int]
    workers_per_group: dict[str, int]


class StatsResponse(BaseModel):
    events_per_sec: float
    tasks_tracked: int
    uptime_sec: int
    retention_sec: int
    broker_connected: bool
    sqlite_rows: int | None


class HealthResponse(BaseModel):
    status: str
    broker_connected: bool
    broker_error: str | None
    broker_reconnects: int
    tasks_tracked: int
    invocations_stored: int
    sqlite_rows: int | None
    sse_clients: int
    queues: list[str]
    worker_groups: list[str]
    workers_seen: int
