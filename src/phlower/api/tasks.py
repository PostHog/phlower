from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from ..schemas import InvocationResponse, LatencyPoint, TaskSummaryResponse

router = APIRouter(prefix="/api/tasks")


@router.get("", response_model=list[TaskSummaryResponse], operation_id="listTasks")
async def list_tasks(request: Request) -> list[TaskSummaryResponse]:
    summaries = request.app.state.store.get_task_list()
    return [TaskSummaryResponse.from_internal(s) for s in summaries]


@router.get("/{task_name}/summary", response_model=TaskSummaryResponse, operation_id="taskSummary")
async def task_summary(task_name: str, request: Request) -> TaskSummaryResponse:
    s = request.app.state.store.get_task_summary(task_name)
    if s is None:
        raise HTTPException(404, f"Task {task_name!r} not found")
    return TaskSummaryResponse.from_internal(s)


@router.get("/{task_name}/latency", response_model=list[LatencyPoint], operation_id="taskLatency")
async def task_latency(task_name: str, request: Request) -> list[dict]:
    data = request.app.state.store.get_task_latency(task_name)
    if data is None:
        raise HTTPException(404, f"Task {task_name!r} not found")
    return data


@router.get("/{task_name}/invocations", response_model=list[InvocationResponse], operation_id="taskInvocations")
async def task_invocations(
    task_name: str,
    request: Request,
    limit: int = 100,
    before_ts: float | None = None,
    after_ts: float | None = None,
) -> list[InvocationResponse]:
    records = request.app.state.store.get_task_invocations(
        task_name, limit=limit, before_ts=before_ts, after_ts=after_ts,
    )
    return [InvocationResponse.from_internal(r) for r in records]
