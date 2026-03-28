"""Thread-safe SSE broadcaster that bridges sync Celery thread → async FastAPI."""

from __future__ import annotations

import asyncio
import json
import logging

logger = logging.getLogger(__name__)


class SSEBroadcaster:
    def __init__(self) -> None:
        self._clients: set[asyncio.Queue[tuple[str, str]]] = set()
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    # -- client management (called from async context) --------------------

    def subscribe(self) -> asyncio.Queue[tuple[str, str]]:
        q: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=64)
        self._clients.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[tuple[str, str]]) -> None:
        self._clients.discard(q)

    @property
    def client_count(self) -> int:
        return len(self._clients)

    # -- broadcasting (called from ANY thread) ----------------------------

    def broadcast(self, event_type: str, data: dict) -> None:
        if not self._loop or not self._clients:
            return
        payload = json.dumps(data, default=str)
        for q in list(self._clients):
            try:
                self._loop.call_soon_threadsafe(q.put_nowait, (event_type, payload))
            except asyncio.QueueFull:
                pass  # client can't keep up — drop
            except Exception:
                logger.debug("SSE enqueue failed", exc_info=True)
