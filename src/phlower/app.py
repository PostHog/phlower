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


async def _sqlite_flush_loop(store: Store, sqlite_store) -> None:
    """Batch flush completed invocations to SQLite every 1.5 seconds."""
    while True:
        await asyncio.sleep(1.5)
        records = store.drain_completed_for_sqlite()
        if records:
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, sqlite_store.flush_batch, records)
            logger.debug("SQLite flush: %d records", count)


async def _sqlite_purge_loop(sqlite_store, retention_hours: int) -> None:
    """Delete rows older than retention window, once per hour."""
    while True:
        await asyncio.sleep(3600)
        cutoff = time.time() - retention_hours * 3600
        loop = asyncio.get_event_loop()
        deleted = await loop.run_in_executor(None, sqlite_store.purge_expired, cutoff)
        if deleted:
            logger.info("SQLite purge: deleted %d expired rows", deleted)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = Config()

    # SQLite layer (optional — only when SQLITE_PATH is set)
    sqlite_store = None
    if config.sqlite_path:
        from .sqlite_store import SQLiteStore

        sqlite_store = SQLiteStore(config.sqlite_path)
        sqlite_store.init_schema()
        logger.info("SQLite enabled at %s", config.sqlite_path)

        # Blocking recovery — rebuild aggregates before accepting traffic
        from .sqlite_recovery import rebuild_aggregates

        since = time.time() - config.sqlite_recovery_hours * 3600
        store = Store(config, sqlite_store=sqlite_store)
        count = rebuild_aggregates(store, sqlite_store, since)
        logger.info("Recovered %d rows from SQLite", count)
    else:
        store = Store(config)

    broadcaster = SSEBroadcaster()
    broadcaster.set_loop(asyncio.get_event_loop())

    consumer = CeleryEventConsumer(config, store)
    consumer.start()

    sse_task = asyncio.create_task(_sse_push_loop(store, broadcaster, config))
    evict_task = asyncio.create_task(_eviction_loop(store, config))

    # SQLite background tasks (only if enabled)
    sqlite_tasks: list[asyncio.Task] = []
    if sqlite_store:
        sqlite_tasks.append(asyncio.create_task(_sqlite_flush_loop(store, sqlite_store)))
        sqlite_tasks.append(
            asyncio.create_task(
                _sqlite_purge_loop(sqlite_store, config.aggregate_retention_hours)
            )
        )

    app.state.store = store
    app.state.broadcaster = broadcaster
    app.state.config = config
    app.state.consumer = consumer
    app.state.started_at = time.time()
    app.state.sqlite_store = sqlite_store

    logger.info(
        "Phlower started — broker=%s retention=%dh max_invocations=%d sqlite=%s",
        config.broker_url,
        config.retention_hours,
        config.max_global_invocations,
        config.sqlite_path or "disabled",
    )

    yield

    # Graceful shutdown — flush remaining SQLite buffer
    if sqlite_store:
        remaining = store.drain_completed_for_sqlite()
        if remaining:
            sqlite_store.flush_batch(remaining)
            logger.info("Final SQLite flush: %d records", len(remaining))
        sqlite_store.close()

    for t in sqlite_tasks:
        t.cancel()
    sse_task.cancel()
    evict_task.cancel()
    consumer.stop()
    logger.info("Phlower shut down")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    from .api.health import router as health_router
    from .api.invocations import router as inv_router
    from .api.stream import router as stream_router
    from .api.tasks import router as tasks_router

    app = FastAPI(title="Phlower", lifespan=lifespan)

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
