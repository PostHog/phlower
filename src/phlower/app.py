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


def _serialise_summary(s) -> dict:
    from dataclasses import asdict

    d = asdict(s)
    d["top_exceptions"] = [{"type": t, "count": c} for t, c in s.top_exceptions]
    d["top_workers"] = [{"worker": w, "count": c} for w, c in s.top_workers]
    d["top_queues"] = [{"queue": q, "count": c} for q, c in s.top_queues]
    return d


def _slim_summary(s) -> dict:
    """Lightweight summary for SSE — no sparkline, no top lists."""
    return {
        "task_name": s.task_name,
        "total_count": s.total_count,
        "success_count": s.success_count,
        "failure_count": s.failure_count,
        "retry_count": s.retry_count,
        "active_count": s.active_count,
        "failure_rate": s.failure_rate,
        "p50_ms": s.p50_ms,
        "p95_ms": s.p95_ms,
        "p99_ms": s.p99_ms,
        "rate_per_min": s.rate_per_min,
    }


async def _sse_push_loop(
    store: Store, broadcaster: SSEBroadcaster, config: Config
) -> None:
    """Push only changed task summaries + stats via SSE."""
    stats_tick = 0
    while True:
        await asyncio.sleep(config.sse_throttle_seconds)
        dirty_tasks, new_ids = store.flush_dirty()
        stats_tick += 1

        # Push stats every ~2s (every 7th tick at 300ms) or when there's activity
        send_stats = dirty_tasks or stats_tick >= 7
        if send_stats:
            stats_tick = 0

        if not dirty_tasks and not send_stats:
            continue

        started_at = getattr(store, "_app_started_at", time.time())
        payload: dict = {"changed": []}

        if dirty_tasks:
            for name in dirty_tasks:
                s = store.get_task_summary(name)
                if s:
                    payload["changed"].append(_slim_summary(s))

        if send_stats:
            payload["stats"] = {
                "events_per_sec": round(store.events_per_second(), 1),
                "tasks_tracked": len(store.tasks),
                "uptime_sec": round(time.time() - started_at),
                "broker_connected": True,
            }

        broadcaster.broadcast("task_update", payload)

        if new_ids:
            broadcaster.broadcast("invocation_update", {"ids": new_ids[-20:]})


async def _sparkline_push_loop(store: Store, broadcaster: SSEBroadcaster) -> None:
    """Push latest sparkline data points every 60s."""
    while True:
        await asyncio.sleep(60)
        points: dict[str, int] = {}
        now_minute = int(time.time()) // 60 * 60
        with store._lock:
            for name, agg in store.tasks.items():
                bucket = agg.buckets.get(now_minute)
                points[name] = bucket.count if bucket else 0
        if points:
            broadcaster.broadcast("sparkline_update", {"points": points})


async def _eviction_loop(store: Store, config: Config) -> None:
    """Periodic cleanup of data older than retention window."""
    interval = max(60, config.retention_hours * 3600 // 24)
    while True:
        await asyncio.sleep(interval)
        store.evict_expired()
        logger.debug("Eviction pass completed")


async def _sqlite_flush_loop(store: Store, sqlite_store) -> None:
    """Batch flush completed invocations to SQLite every 1.5 seconds."""
    logger.info("SQLite flush loop started")
    try:
        while True:
            await asyncio.sleep(1.5)
            records = store.drain_completed_for_sqlite()
            if records:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, sqlite_store.flush_batch, records)
                logger.info("SQLite flush: %d records", len(records))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("SQLite flush loop crashed")


async def _sqlite_purge_loop(sqlite_store, config: Config) -> None:
    """Thin detail fields after SQLITE_DETAIL_HOURS, delete after AGGREGATE_RETENTION_HOURS."""
    while True:
        await asyncio.sleep(3600)
        loop = asyncio.get_event_loop()

        # Thin: NULL out args/kwargs/traceback for old records (10K batch)
        thin_cutoff = time.time() - config.sqlite_detail_hours * 3600
        thinned = await loop.run_in_executor(None, sqlite_store.thin_details, thin_cutoff)
        if thinned:
            logger.info("SQLite thin: stripped detail from %d records", thinned)

        # Purge: delete rows older than retention window
        purge_cutoff = time.time() - config.aggregate_retention_hours * 3600
        deleted = await loop.run_in_executor(None, sqlite_store.purge_expired, purge_cutoff)
        if deleted:
            logger.info("SQLite purge: deleted %d expired rows", deleted)

        logger.info("SQLite: %.1f MB, %d rows", sqlite_store.db_size_mb(), sqlite_store.row_count())


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

    store._app_started_at = time.time()
    sse_task = asyncio.create_task(_sse_push_loop(store, broadcaster, config))
    sparkline_task = asyncio.create_task(_sparkline_push_loop(store, broadcaster))
    evict_task = asyncio.create_task(_eviction_loop(store, config))

    # SQLite background tasks (only if enabled)
    sqlite_tasks: list[asyncio.Task] = []
    if sqlite_store:
        sqlite_tasks.append(asyncio.create_task(_sqlite_flush_loop(store, sqlite_store)))
        sqlite_tasks.append(
            asyncio.create_task(
                _sqlite_purge_loop(sqlite_store, config)
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
    sparkline_task.cancel()
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
