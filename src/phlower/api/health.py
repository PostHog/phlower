from __future__ import annotations

import time

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/api/meta")
async def meta(request: Request) -> dict:
    """Worker groups, queues, and other metadata from celery inspect."""
    consumer = request.app.state.consumer
    store = request.app.state.store
    return {
        "queues": consumer.registry.all_queues(),
        "worker_groups": consumer.registry.all_groups(),
        "workers_seen": consumer.registry.worker_count(),
        "last_inspect_at": consumer.registry.last_inspect_at,
        "pickup_latency_p95": store.pickup_latency_by_queue(),
    }


@router.get("/api/stats")
async def stats(request: Request) -> dict:
    """Live stats for the nav bar ticker."""
    store = request.app.state.store
    started_at = request.app.state.started_at
    uptime = time.time() - started_at
    retention = store.config.retention_hours * 3600
    sqlite_store = request.app.state.sqlite_store
    return {
        "events_per_sec": round(store.events_per_second(), 1),
        "tasks_tracked": len(store.tasks),
        "uptime_sec": round(uptime),
        "retention_sec": retention,
        "broker_connected": request.app.state.consumer.connected,
        "sqlite_rows": sqlite_store.row_count() if sqlite_store else None,
    }


@router.get("/healthz")
async def healthz(request: Request) -> dict:
    store = request.app.state.store
    consumer = request.app.state.consumer
    sqlite_store = request.app.state.sqlite_store
    return {
        "status": "ok",
        "broker_connected": consumer.connected,
        "broker_error": consumer.last_error,
        "broker_reconnects": consumer.reconnect_count,
        "tasks_tracked": len(store.tasks),
        "invocations_stored": len(store.invocations),
        "sqlite_rows": sqlite_store.row_count() if sqlite_store else None,
        "sse_clients": request.app.state.broadcaster.client_count,
        "queues": consumer.registry.all_queues(),
        "worker_groups": consumer.registry.all_groups(),
        "workers_seen": consumer.registry.worker_count(),
    }
