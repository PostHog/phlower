from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()


def _serialise(r) -> dict:
    d = asdict(r)
    d["state"] = r.state.value
    d["transitions"] = [{"state": s, "ts": t} for s, t in r.transitions]
    return d


@router.get("/api/invocations/{task_id}")
async def invocation_detail(task_id: str, request: Request) -> dict:
    rec = request.app.state.store.get_invocation(task_id)
    if rec is None:
        raise HTTPException(404, f"Invocation {task_id!r} not found")
    return _serialise(rec)


@router.get("/api/search/invocations")
async def search_invocations(
    request: Request,
    task_name: str | None = None,
    status: str | None = None,
    worker: str | None = None,
    task_id: str | None = None,
    q: str | None = None,
    time_from: float | None = None,
    time_to: float | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    results = request.app.state.store.search_invocations(
        task_name=task_name,
        status=status,
        worker=worker,
        task_id=task_id,
        q=q,
        time_from=time_from,
        time_to=time_to,
        limit=limit,
        offset=offset,
    )
    return [_serialise(r) for r in results]
