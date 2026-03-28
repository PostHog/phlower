"""SSE endpoint — one EventSource per browser tab."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

router = APIRouter()


@router.get("/api/stream")
async def stream(request: Request) -> EventSourceResponse:
    broadcaster = request.app.state.broadcaster

    queue = broadcaster.subscribe()

    async def generate():
        try:
            while True:
                event_type, payload = await queue.get()
                yield {"event": event_type, "data": payload}
        except asyncio.CancelledError:
            pass
        finally:
            broadcaster.unsubscribe(queue)

    return EventSourceResponse(generate())
