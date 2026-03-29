from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TaskState(str, Enum):
    RECEIVED = "RECEIVED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"


@dataclass
class InvocationRecord:
    task_id: str
    task_name: str
    state: TaskState
    received_at: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    runtime_ms: float | None = None
    worker: str | None = None
    queue: str | None = None
    args_preview: str | None = None
    kwargs_preview: str | None = None
    exception_type: str | None = None
    exception_message: str | None = None
    traceback_snippet: str | None = None
    retries: int = 0
    transitions: list[tuple[str, float]] = field(default_factory=list)


@dataclass
class MinuteBucket:
    timestamp: int  # epoch seconds, floored to minute
    count: int = 0
    success: int = 0
    failure: int = 0
    retry: int = 0
    runtimes: list[float] = field(default_factory=list)


@dataclass
class TaskSummary:
    """Serialisable snapshot of a task's aggregate state."""

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
    top_exceptions: list[tuple[str, int]]
    top_workers: list[tuple[str, int]]
    rate_per_min: float
    top_queues: list[tuple[str, int]]
    sparkline: list[int] = field(default_factory=list)
