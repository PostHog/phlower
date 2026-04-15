from __future__ import annotations

import time

from fastapi import APIRouter, Request

from ..schemas import HealthResponse, MetaResponse, StatsResponse

router = APIRouter()


@router.get("/api/meta", response_model=MetaResponse, operation_id="meta")
async def meta(request: Request) -> MetaResponse:
    consumer = request.app.state.consumer
    store = request.app.state.store
    return MetaResponse(
        queues=sorted(set(consumer.registry.all_queues()) | set(store.get_known_queues_nonblocking())),
        worker_groups=consumer.registry.all_groups(),
        workers_seen=consumer.registry.worker_count(),
        last_inspect_at=consumer.registry.last_inspect_at,
        pickup_latency_p95=store.pickup_latency_by_queue(),
        workers_per_queue=consumer.registry.workers_per_queue(),
        workers_per_group=consumer.registry.workers_per_group(),
    )


@router.get("/api/stats", response_model=StatsResponse, operation_id="stats")
async def stats(request: Request) -> StatsResponse:
    store = request.app.state.store
    started_at = request.app.state.started_at
    uptime = time.time() - started_at
    retention = store.config.retention_hours * 3600
    sqlite_store = request.app.state.sqlite_store
    return StatsResponse(
        events_per_sec=round(store.events_per_second(), 1),
        tasks_tracked=len(store.tasks),
        uptime_sec=round(uptime),
        retention_sec=retention,
        broker_connected=request.app.state.consumer.connected,
        sqlite_rows=sqlite_store._cached_row_count if sqlite_store else None,
    )


@router.get("/healthz", response_model=HealthResponse, operation_id="healthz")
async def healthz(request: Request) -> HealthResponse:
    store = request.app.state.store
    consumer = request.app.state.consumer
    sqlite_store = request.app.state.sqlite_store
    return HealthResponse(
        status="ok",
        broker_connected=consumer.connected,
        broker_error=consumer.last_error,
        broker_reconnects=consumer.reconnect_count,
        tasks_tracked=len(store.tasks),
        invocations_stored=len(store.invocations),
        sqlite_rows=sqlite_store._cached_row_count if sqlite_store else None,
        sse_clients=request.app.state.broadcaster.client_count,
        queues=sorted(set(consumer.registry.all_queues()) | set(store.get_known_queues_nonblocking())),
        worker_groups=consumer.registry.all_groups(),
        workers_seen=consumer.registry.worker_count(),
    )
