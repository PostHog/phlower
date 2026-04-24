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


def _slim_summary(s) -> dict:
    """Lightweight summary for SSE — no sparkline, no top lists."""
    from .schemas import SlimSummary
    return SlimSummary.from_internal(s).model_dump()


async def _sse_push_loop(
    store: Store, broadcaster: SSEBroadcaster, config: Config
) -> None:
    """Push only changed task summaries + stats via SSE."""
    stats_tick = 0
    while True:
        await asyncio.sleep(config.sse_throttle_seconds)
        dirty_tasks = store.flush_dirty()
        stats_tick += 1

        send_stats = dirty_tasks or stats_tick >= 7
        if send_stats:
            stats_tick = 0

        if not dirty_tasks and not send_stats:
            continue

        started_at = getattr(store, "_app_started_at", time.time())
        payload: dict = {"changed": []}

        if dirty_tasks:
            summaries = store.get_task_summaries(dirty_tasks)
            payload["changed"] = [_slim_summary(s) for s in summaries]

        if send_stats:
            payload["stats"] = {
                "events_per_sec": round(store.events_per_second(), 1),
                "tasks_tracked": len(store.tasks),
                "uptime_sec": round(time.time() - started_at),
                "broker_connected": True,
            }

        broadcaster.broadcast("task_update", payload)


async def _invocation_push_loop(
    store: Store, broadcaster: SSEBroadcaster, config: Config
) -> None:
    """Push invocation update signals at a slower cadence (default 600ms)."""
    while True:
        await asyncio.sleep(config.sse_invocation_throttle_seconds)
        new_ids = store.flush_new_invocation_ids()
        if new_ids:
            broadcaster.broadcast("invocation_update", {"ids": new_ids[-20:]})


async def _sparkline_push_loop(store: Store, broadcaster: SSEBroadcaster) -> None:
    """Push latest sparkline data points every 60s."""
    while True:
        await asyncio.sleep(60)
        points = store.get_sparkline_points()
        if points:
            broadcaster.broadcast("sparkline_update", {"points": points})


async def _eviction_loop(store: Store, config: Config) -> None:
    """Periodic cleanup of data older than retention window."""
    while True:
        await asyncio.sleep(1800)
        store.evict_expired()
        logger.debug("Eviction pass completed")


async def _sqlite_flush_loop(store: Store, sqlite_store) -> None:
    """Batch flush completed invocations to SQLite every 1.5 seconds."""
    logger.info("SQLite flush loop started")
    while True:
        await asyncio.sleep(1.5)
        records = store.drain_completed_for_sqlite()
        if not records:
            continue
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, sqlite_store.flush_batch, records)
            store.remove_flushed([r.task_id for r in records])
            logger.info("SQLite flush: %d records", len(records))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("SQLite flush error (%d records retained in memory)", len(records))


async def _aggregate_snapshot_loop(
    store: Store, sqlite_store, config: Config
) -> None:
    """Periodically persist dirty task aggregates as compressed snapshots."""
    while True:
        await asyncio.sleep(config.snapshot_interval_seconds)
        dirty = store.drain_snapshot_dirty()
        if not dirty:
            continue
        try:
            snapshots = store.snapshot_aggregates(dirty)
            if snapshots:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, sqlite_store.save_snapshots, snapshots)
                logger.info("Snapshot flush: %d tasks", len(snapshots))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Snapshot flush error")


async def _background_recovery(store: Store, sqlite_store, config: Config) -> None:
    """Restore aggregates from snapshots, with gap replay for recent events."""
    from .sqlite_recovery import rebuild_aggregates, restore_from_snapshots

    loop = asyncio.get_event_loop()
    try:
        snapshot_ts = await loop.run_in_executor(
            None, restore_from_snapshots, store, sqlite_store
        )

        if snapshot_ts is not None:
            count = await loop.run_in_executor(
                None, rebuild_aggregates, store, sqlite_store, snapshot_ts
            )
            logger.info("Snapshot recovery + gap replay: %d gap rows", count)
        else:
            since = time.time() - config.sqlite_recovery_hours * 3600
            count = await loop.run_in_executor(
                None, rebuild_aggregates, store, sqlite_store, since
            )
            logger.info("Full row-replay recovery: %d rows restored", count)

        await loop.run_in_executor(None, sqlite_store.checkpoint)
    except Exception:
        logger.exception("Background recovery failed")


async def _sqlite_purge_loop(
    store: Store, sqlite_store, config: Config, consumer=None
) -> None:
    """Purge detail rows after SQLITE_DETAIL_HOURS, core rows after SQLITE_INVOCATION_RETENTION_HOURS."""
    while True:
        await asyncio.sleep(3600)
        loop = asyncio.get_event_loop()
        now = time.time()

        # Disk pressure: if usage exceeds cap, halve the retention window
        # repeatedly until it fits or hits a 1-hour floor.
        retention_hours = config.sqlite_invocation_retention_hours
        detail_hours = config.sqlite_detail_hours
        disk_pct = await loop.run_in_executor(None, sqlite_store.disk_usage_pct)
        if disk_pct > config.sqlite_disk_usage_pct_cap:
            while retention_hours > 1 and disk_pct > config.sqlite_disk_usage_pct_cap:
                retention_hours = max(1, retention_hours // 2)
                detail_hours = max(1, detail_hours // 2)
                logger.warning(
                    "Disk %.0f%% > %d%% cap — emergency purge with %dh retention, %dh details",
                    disk_pct, config.sqlite_disk_usage_pct_cap, retention_hours, detail_hours,
                )
                purge_cutoff = now - retention_hours * 3600
                await loop.run_in_executor(None, sqlite_store.purge_expired, purge_cutoff)
                detail_cutoff = now - detail_hours * 3600
                await loop.run_in_executor(None, sqlite_store.purge_details, detail_cutoff)
                disk_pct = await loop.run_in_executor(None, sqlite_store.disk_usage_pct)
        else:
            # Normal purge: details first (short retention), then core rows
            detail_cutoff = now - detail_hours * 3600
            purged_details = await loop.run_in_executor(None, sqlite_store.purge_details, detail_cutoff)
            if purged_details:
                logger.info("SQLite purge: deleted %d detail rows (>%dh)", purged_details, detail_hours)

            purge_cutoff = now - retention_hours * 3600
            deleted = await loop.run_in_executor(None, sqlite_store.purge_expired, purge_cutoff)
            if deleted:
                logger.info("SQLite purge: deleted %d expired rows (>%dh)", deleted, retention_hours)

        # Remove snapshots for tasks no longer tracked
        active = set(store.tasks.keys())
        await loop.run_in_executor(
            None, sqlite_store.purge_stale_snapshots, active
        )

        # Checkpoint WAL
        await loop.run_in_executor(None, sqlite_store.checkpoint)

        # Persist registry metadata for fast recovery on restart
        if consumer:
            consumer._persist_metadata()

        size_mb = await loop.run_in_executor(None, sqlite_store.db_size_mb)
        row_ct = await loop.run_in_executor(None, sqlite_store.row_count)
        wal_mb = await loop.run_in_executor(None, sqlite_store.wal_size_mb)
        logger.info("SQLite: %.1f MB, %d rows, WAL: %.1f MB", size_mb, row_ct, wal_mb)


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

        try:
            sqlite_store = SQLiteStore(config.sqlite_path)
            sqlite_store.init_schema()
            logger.info("SQLite enabled at %s", config.sqlite_path)
        except Exception:
            logger.exception(
                "SQLite failed to open at %s — continuing without persistence. "
                "Delete or move the DB file to recover.",
                config.sqlite_path,
            )
            sqlite_store = None

        store = Store(config, sqlite_store=sqlite_store)
    else:
        store = Store(config)

    broadcaster = SSEBroadcaster()
    broadcaster.set_loop(asyncio.get_event_loop())

    consumer = CeleryEventConsumer(config, store, sqlite_store=sqlite_store)
    consumer.seed_registry_from_sqlite()
    consumer.start()

    store._app_started_at = time.time()
    sse_task = asyncio.create_task(_sse_push_loop(store, broadcaster, config))
    inv_push_task = asyncio.create_task(_invocation_push_loop(store, broadcaster, config))
    sparkline_task = asyncio.create_task(_sparkline_push_loop(store, broadcaster))
    evict_task = asyncio.create_task(_eviction_loop(store, config))

    # SQLite background tasks (only if enabled)
    sqlite_tasks: list[asyncio.Task] = []
    if sqlite_store:
        sqlite_tasks.append(asyncio.create_task(_sqlite_flush_loop(store, sqlite_store)))
        sqlite_tasks.append(
            asyncio.create_task(
                _aggregate_snapshot_loop(store, sqlite_store, config)
            )
        )
        sqlite_tasks.append(
            asyncio.create_task(
                _sqlite_purge_loop(store, sqlite_store, config, consumer=consumer)
            )
        )
        sqlite_tasks.append(
            asyncio.create_task(_background_recovery(store, sqlite_store, config))
        )

    app.state.store = store
    app.state.broadcaster = broadcaster
    app.state.config = config
    app.state.consumer = consumer
    app.state.started_at = time.time()
    app.state.sqlite_store = sqlite_store

    logger.info(
        "Phlower started — broker=%s retention=%dh sqlite=%s",
        config.broker_url,
        config.retention_hours,
        config.sqlite_path or "disabled",
    )

    yield

    # Graceful shutdown — flush remaining SQLite buffer + snapshots + metadata
    if sqlite_store:
        remaining = store.drain_completed_for_sqlite()
        if remaining:
            sqlite_store.flush_batch(remaining)
            logger.info("Final SQLite flush: %d records", len(remaining))
        all_task_names = set(store.tasks.keys())
        if all_task_names:
            snapshots = store.snapshot_aggregates(all_task_names)
            if snapshots:
                sqlite_store.save_snapshots(snapshots)
                logger.info("Final snapshot flush: %d tasks", len(snapshots))
        consumer._persist_metadata()
        sqlite_store.close()

    for t in sqlite_tasks:
        t.cancel()
    sse_task.cancel()
    inv_push_task.cancel()
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
