"""FastAPI application — wires everything together."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import Config
from .events import CeleryEventConsumer
from .sse import SSEBroadcaster
from .store import Store

logger = logging.getLogger(__name__)

FRONTEND_DIR = Path(__file__).parent / "frontend_dist"


# ---------------------------------------------------------------------------
# Background loops
# ---------------------------------------------------------------------------


async def _sse_push_loop(store: Store, broadcaster: SSEBroadcaster, config: Config) -> None:
    """Throttled SSE broadcaster — flushes dirty state every N seconds."""
    while True:
        await asyncio.sleep(config.sse_throttle_seconds)
        dirty_tasks, new_ids = store.flush_dirty()
        if dirty_tasks:
            broadcaster.broadcast("task_update", {"updated": list(dirty_tasks)})
        if new_ids:
            broadcaster.broadcast("invocation_update", {"ids": new_ids[-20:]})


async def _eviction_loop(store: Store, config: Config) -> None:
    """Periodic cleanup of data older than retention window."""
    interval = max(60, config.retention_hours * 3600 // 24)
    while True:
        await asyncio.sleep(interval)
        store.evict_expired()
        logger.debug("Eviction pass completed")


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = Config()
    store = Store(config)
    broadcaster = SSEBroadcaster()
    broadcaster.set_loop(asyncio.get_event_loop())

    consumer = CeleryEventConsumer(config, store)
    consumer.start()

    sse_task = asyncio.create_task(_sse_push_loop(store, broadcaster, config))
    evict_task = asyncio.create_task(_eviction_loop(store, config))

    app.state.store = store
    app.state.broadcaster = broadcaster
    app.state.config = config
    app.state.consumer = consumer
    app.state.started_at = time.time()

    logger.info(
        "phlower started — broker=%s retention=%dh max_invocations=%d",
        config.broker_url,
        config.retention_hours,
        config.max_global_invocations,
    )

    yield

    sse_task.cancel()
    evict_task.cancel()
    consumer.stop()
    logger.info("phlower shut down")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    from .api.health import router as health_router
    from .api.invocations import router as inv_router
    from .api.stream import router as stream_router
    from .api.tasks import router as tasks_router

    app = FastAPI(title="phlower", lifespan=lifespan)

    app.include_router(health_router)
    app.include_router(tasks_router)
    app.include_router(inv_router)
    app.include_router(stream_router)

    # Serve React SPA — static assets + catch-all for client-side routing
    if FRONTEND_DIR.exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(FRONTEND_DIR / "assets")),
            name="assets",
        )

        @app.get("/{path:path}")
        async def spa_catchall(path: str):
            # Serve actual files if they exist, otherwise index.html for SPA routing
            file = FRONTEND_DIR / path
            if file.is_file():
                return FileResponse(file)
            return FileResponse(FRONTEND_DIR / "index.html")
    else:
        logger.warning(
            "Frontend not built — run 'cd frontend && pnpm build'. "
            "API endpoints are still available."
        )

    return app
