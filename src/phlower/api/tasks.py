from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api/tasks")


def _serialise_summary(s) -> dict:
    d = asdict(s)
    d["top_exceptions"] = [{"type": t, "count": c} for t, c in s.top_exceptions]
    d["top_workers"] = [{"worker": w, "count": c} for w, c in s.top_workers]
    d["top_queues"] = [{"queue": q, "count": c} for q, c in s.top_queues]
    return d


@router.get("")
async def list_tasks(request: Request) -> list[dict]:
    summaries = request.app.state.store.get_task_list()
    return [_serialise_summary(s) for s in summaries]


@router.get("/{task_name}/summary")
async def task_summary(task_name: str, request: Request) -> dict:
    s = request.app.state.store.get_task_summary(task_name)
    if s is None:
        raise HTTPException(404, f"Task {task_name!r} not found")
    return _serialise_summary(s)


@router.get("/{task_name}/latency")
async def task_latency(task_name: str, request: Request) -> list[dict]:
    data = request.app.state.store.get_task_latency(task_name)
    if data is None:
        raise HTTPException(404, f"Task {task_name!r} not found")
    return data


@router.get("/{task_name}/invocations")
async def task_invocations(
    task_name: str,
    request: Request,
    limit: int = 100,
    before_ts: float | None = None,
    after_ts: float | None = None,
) -> list[dict]:
    records = request.app.state.store.get_task_invocations(
        task_name, limit=limit, before_ts=before_ts, after_ts=after_ts,
    )
    return [_serialise_record(r) for r in records]


def _serialise_record(r) -> dict:
    return {
        "task_id": r.task_id,
        "task_name": r.task_name,
        "state": r.state.value,
        "received_at": r.received_at,
        "started_at": r.started_at,
        "finished_at": r.finished_at,
        "runtime_ms": r.runtime_ms,
        "worker": r.worker,
        "queue": r.queue,
        "retries": r.retries,
        "exception_type": r.exception_type,
    }
