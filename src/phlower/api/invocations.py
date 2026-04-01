from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request

from ..schemas import InvocationResponse

router = APIRouter()


@router.get("/api/invocations/{task_id}", response_model=InvocationResponse, operation_id="invocationDetail")
async def invocation_detail(task_id: str, request: Request) -> InvocationResponse:
    rec = request.app.state.store.get_invocation(task_id)
    if rec is None:
        raise HTTPException(404, f"Invocation {task_id!r} not found")
    return InvocationResponse.from_internal(rec)


@router.get("/api/search/invocations", response_model=list[InvocationResponse], operation_id="searchInvocations")
async def search_invocations(
    request: Request,
    task_name: str | None = None,
    status: str | None = None,
    worker: str | None = None,
    queue: str | None = None,
    task_id: str | None = None,
    q: str | None = None,
    time_from: float | None = None,
    time_to: float | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[InvocationResponse]:
    results = request.app.state.store.search_invocations(
        task_name=task_name,
        status=status,
        worker=worker,
        queue=queue,
        task_id=task_id,
        q=q,
        time_from=time_from,
        time_to=time_to,
        limit=limit,
        offset=offset,
    )
    return [InvocationResponse.from_internal(r) for r in results]
