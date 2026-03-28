from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/healthz")
async def healthz(request: Request) -> dict:
    store = request.app.state.store
    consumer = request.app.state.consumer
    return {
        "status": "ok",
        "broker_connected": consumer.connected,
        "broker_error": consumer.last_error,
        "broker_reconnects": consumer.reconnect_count,
        "tasks_tracked": len(store.tasks),
        "invocations_stored": len(store.invocations),
        "sse_clients": request.app.state.broadcaster.client_count,
        "queues": store.get_known_queues(),
    }
