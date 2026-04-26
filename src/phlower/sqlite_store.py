"""SQLite write-behind warm index for historical task ID lookups."""

from __future__ import annotations

import functools
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterator

from .models import InvocationRecord, TaskState


def _serialized(method):
    """Serialize access to the shared SQLite connection via _write_lock."""
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._write_lock:
            return method(self, *args, **kwargs)
    return wrapper

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS invocations (
    task_id     TEXT PRIMARY KEY,
    task_name   TEXT NOT NULL,
    state       TEXT NOT NULL,
    received_at REAL,
    started_at  REAL,
    finished_at REAL,
    runtime_ms  REAL,
    worker      TEXT,
    queue       TEXT,
    exception_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_inv_finished ON invocations (finished_at);
CREATE INDEX IF NOT EXISTS idx_inv_task_name ON invocations (task_name, finished_at);

CREATE TABLE IF NOT EXISTS invocation_details (
    task_id            TEXT PRIMARY KEY,
    args_preview       TEXT,
    kwargs_preview     TEXT,
    traceback_snippet  TEXT
);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (key, value)
);

CREATE TABLE IF NOT EXISTS aggregate_snapshots (
    task_name    TEXT PRIMARY KEY,
    snapshot_ts  REAL NOT NULL,
    data         BLOB NOT NULL
);
"""

UPSERT_SQL = """
INSERT OR REPLACE INTO invocations
    (task_id, task_name, state, received_at, started_at, finished_at,
     runtime_ms, worker, queue, exception_type)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_DETAILS_SQL = """
INSERT OR REPLACE INTO invocation_details
    (task_id, args_preview, kwargs_preview, traceback_snippet)
VALUES (?, ?, ?, ?)
"""


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._cached_row_count: int = 0
        self._cached_detail_row_count: int = 0
        self._cached_oldest_at: float | None = None
        self._write_lock = threading.Lock()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._connect(db_path)

    def _connect(self, path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        # Cap WAL file size after successful checkpoint — prevents the file
        # from staying at its high-water mark forever after a growth spike.
        conn.execute("PRAGMA journal_size_limit=67108864")  # 64 MB
        return conn

    def init_schema(self) -> None:
        self._conn.executescript(SCHEMA)
        self._migrate()
        # Checkpoint any WAL inherited from a crash — this must happen before
        # recovery opens its read connection, otherwise the stale WAL blocks
        # checkpointing for the entire recovery duration.
        self.checkpoint(truncate=True)

    def _migrate(self) -> None:
        """Migrate from single-table to split-table schema if needed."""
        cols = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(invocations)").fetchall()
        }
        if "args_preview" not in cols:
            return

        logger.info("Migrating to split-table schema (invocations + invocation_details)")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS invocation_details ("
            "  task_id TEXT PRIMARY KEY,"
            "  args_preview TEXT,"
            "  kwargs_preview TEXT,"
            "  traceback_snippet TEXT"
            ")"
        )
        self._conn.commit()
        total = 0
        while True:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO invocation_details (task_id, args_preview, kwargs_preview, traceback_snippet) "
                "SELECT task_id, args_preview, kwargs_preview, traceback_snippet "
                "FROM invocations "
                "WHERE (args_preview IS NOT NULL OR kwargs_preview IS NOT NULL OR traceback_snippet IS NOT NULL) "
                "AND task_id NOT IN (SELECT task_id FROM invocation_details) "
                "LIMIT 50000"
            )
            self._conn.commit()
            batch = cur.rowcount
            total += batch
            if batch > 0:
                logger.info("Migration progress: %d rows copied", total)
            if batch < 50000:
                break
        for col in ("args_preview", "kwargs_preview", "traceback_snippet"):
            self._conn.execute(f"ALTER TABLE invocations DROP COLUMN {col}")
        self._conn.commit()
        logger.info("Split-table migration complete — %d detail rows", total)

    # -- writes -----------------------------------------------------------

    @_serialized
    def flush_batch(self, records: list) -> int:
        if not records:
            return 0
        self._conn.executemany(
            UPSERT_SQL,
            [
                (
                    r.task_id, r.task_name, r.state,
                    r.received_at, r.started_at, r.finished_at,
                    r.runtime_ms, r.worker, r.queue, r.exception_type,
                )
                for r in records
            ],
        )
        details = []
        detail_deletes = []
        for r in records:
            if r.args_preview or r.kwargs_preview or r.traceback_snippet:
                details.append((r.task_id, r.args_preview, r.kwargs_preview, r.traceback_snippet))
            else:
                detail_deletes.append((r.task_id,))
        if details:
            self._conn.executemany(UPSERT_DETAILS_SQL, details)
        if detail_deletes:
            self._conn.executemany("DELETE FROM invocation_details WHERE task_id = ?", detail_deletes)
        self._conn.commit()
        return len(records)

    @_serialized
    def purge_details(self, cutoff_ts: float) -> int:
        """Delete detail rows for invocations finished before cutoff."""
        total = 0
        while True:
            cur = self._conn.execute(
                "DELETE FROM invocation_details WHERE task_id IN ("
                "  SELECT d.task_id FROM invocation_details d"
                "  JOIN invocations i ON d.task_id = i.task_id"
                "  WHERE i.finished_at < ? LIMIT 50000"
                ")",
                (cutoff_ts,),
            )
            self._conn.commit()
            affected = cur.rowcount
            total += affected
            if affected < 50000:
                break
        return total

    @_serialized
    def purge_expired(self, cutoff_ts: float) -> int:
        """Delete old core rows + their details in batches."""
        total = 0
        while True:
            self._conn.execute(
                "DELETE FROM invocation_details WHERE task_id IN ("
                "  SELECT task_id FROM invocations WHERE finished_at < ? LIMIT 50000"
                ")",
                (cutoff_ts,),
            )
            cur = self._conn.execute(
                "DELETE FROM invocations WHERE rowid IN ("
                "  SELECT rowid FROM invocations WHERE finished_at < ? LIMIT 50000"
                ")",
                (cutoff_ts,),
            )
            self._conn.commit()
            affected = cur.rowcount
            total += affected
            if affected < 50000:
                break
        return total

    # -- reads ------------------------------------------------------------

    def lookup_task_id(self, task_id: str) -> InvocationRecord | None:
        row = self._conn.execute(
            "SELECT i.*, d.args_preview, d.kwargs_preview, d.traceback_snippet "
            "FROM invocations i LEFT JOIN invocation_details d ON i.task_id = d.task_id "
            "WHERE i.task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def list_by_task(
        self,
        task_name: str,
        *,
        limit: int = 100,
        before_ts: float | None = None,
        after_ts: float | None = None,
        exclude_ids: set[str] | None = None,
    ) -> list[InvocationRecord]:
        """List invocations for a task, newest first. Uses idx_inv_task_name."""
        clauses = ["i.task_name = ?"]
        params: list[object] = [task_name]
        if before_ts is not None:
            clauses.append("i.finished_at < ?")
            params.append(before_ts)
        if after_ts is not None:
            clauses.append("i.finished_at > ?")
            params.append(after_ts)
        where = " AND ".join(clauses)
        fetch_limit = limit + (len(exclude_ids) if exclude_ids else 0)
        sql = (
            "SELECT i.*, d.args_preview, d.kwargs_preview, d.traceback_snippet "
            "FROM invocations i LEFT JOIN invocation_details d ON i.task_id = d.task_id "
            f"WHERE {where} ORDER BY i.finished_at DESC LIMIT ?"
        )
        params.append(fetch_limit)
        rows = self._conn.execute(sql, params).fetchall()
        results: list[InvocationRecord] = []
        for row in rows:
            if exclude_ids and row[0] in exclude_ids:
                continue
            results.append(self._row_to_record(row))
            if len(results) >= limit:
                break
        return results

    def search(
        self,
        *,
        task_name: str | None = None,
        state: str | None = None,
        worker: str | None = None,
        queue: str | None = None,
        q: str | None = None,
        time_from: float | None = None,
        time_to: float | None = None,
        limit: int = 50,
        offset: int = 0,
        exclude_ids: set[str] | None = None,
    ) -> list[InvocationRecord]:
        clauses: list[str] = []
        params: list[object] = []
        if task_name:
            clauses.append("i.task_name = ?")
            params.append(task_name)
        if state:
            clauses.append("i.state = ?")
            params.append(state)
        if worker:
            clauses.append("i.worker = ?")
            params.append(worker)
        if queue:
            clauses.append("i.queue = ?")
            params.append(queue)
        if time_from:
            clauses.append("i.finished_at >= ?")
            params.append(time_from)
        if time_to:
            clauses.append("i.finished_at <= ?")
            params.append(time_to)
        if q:
            clauses.append(
                "(i.task_id LIKE ? OR i.task_name LIKE ? OR d.args_preview LIKE ?"
                " OR d.kwargs_preview LIKE ? OR i.exception_type LIKE ?"
                " OR i.worker LIKE ? OR i.queue LIKE ?)"
            )
            pattern = f"%{q}%"
            params.extend([pattern] * 7)

        where = " AND ".join(clauses) if clauses else "1=1"
        fetch_limit = limit + (len(exclude_ids) if exclude_ids else 0)
        sql = (
            "SELECT i.*, d.args_preview, d.kwargs_preview, d.traceback_snippet "
            "FROM invocations i LEFT JOIN invocation_details d ON i.task_id = d.task_id "
            f"WHERE {where} ORDER BY i.finished_at DESC LIMIT ? OFFSET ?"
        )
        params.extend([fetch_limit, offset])
        rows = self._conn.execute(sql, params).fetchall()
        results: list[InvocationRecord] = []
        for row in rows:
            if exclude_ids and row[0] in exclude_ids:
                continue
            results.append(self._row_to_record(row))
            if len(results) >= limit:
                break
        return results

    def open_recovery_conn(self) -> sqlite3.Connection:
        """Open a separate connection for recovery. Caller must close it."""
        return self._connect(self.db_path)

    def load_recovery_counts(self, conn: sqlite3.Connection, since_ts: float) -> Iterator[sqlite3.Row]:
        """Aggregated counts per task/state/minute for fast recovery.

        Processes in 4-hour chunks so the read lock is released between
        chunks, allowing WAL checkpointing to proceed.
        """
        now = time.time()
        chunk_start = since_ts
        while chunk_start < now:
            chunk_end = min(chunk_start + 14400, now + 1)  # 4-hour windows
            cur = conn.cursor()
            cur.row_factory = sqlite3.Row
            cur.execute(
                "SELECT task_name, state, "
                "  (CAST(finished_at AS INTEGER) / 60 * 60) AS minute_ts, "
                "  COUNT(*) AS cnt, "
                "  worker, queue, exception_type "
                "FROM invocations WHERE finished_at >= ? AND finished_at < ? "
                "GROUP BY task_name, state, minute_ts, worker, queue, exception_type "
                "ORDER BY task_name",
                (chunk_start, chunk_end),
            )
            yield from cur
            cur.close()
            # Explicit commit releases any read snapshot Python's sqlite3
            # module may hold, allowing WAL checkpointing to proceed.
            conn.commit()
            chunk_start = chunk_end

    def load_recovery_runtimes(self, conn: sqlite3.Connection, since_ts: float) -> Iterator[sqlite3.Row]:
        """Stream individual runtime values for t-digest population.

        Chunked in 4-hour windows to release read locks periodically.
        """
        now = time.time()
        chunk_start = since_ts
        while chunk_start < now:
            chunk_end = min(chunk_start + 14400, now + 1)
            cur = conn.cursor()
            cur.row_factory = sqlite3.Row
            cur.execute(
                "SELECT task_name, "
                "  (CAST(finished_at AS INTEGER) / 60 * 60) AS minute_ts, "
                "  runtime_ms "
                "FROM invocations "
                "WHERE finished_at >= ? AND finished_at < ? AND runtime_ms IS NOT NULL "
                "ORDER BY task_name",
                (chunk_start, chunk_end),
            )
            yield from cur
            cur.close()
            conn.commit()
            chunk_start = chunk_end

    def load_recovery_pickup(self, conn: sqlite3.Connection, since_ts: float) -> Iterator[sqlite3.Row]:
        """Stream received_at/started_at pairs for pickup latency rebuild."""
        cur = conn.cursor()
        cur.row_factory = sqlite3.Row
        cur.execute(
            "SELECT queue, (started_at - received_at) * 1000 AS wait_ms "
            "FROM invocations "
            "WHERE finished_at >= ? "
            "  AND received_at IS NOT NULL AND started_at IS NOT NULL "
            "  AND started_at > received_at "
            "ORDER BY finished_at DESC LIMIT 5000",
            (since_ts,),
        )
        yield from cur

    @_serialized
    def refresh_cached_stats(self) -> None:
        """Update cached stats for healthz — called from purge loop."""
        row = self._conn.execute("SELECT count(*) FROM invocations").fetchone()
        self._cached_row_count = row[0] if row else 0
        row = self._conn.execute("SELECT count(*) FROM invocation_details").fetchone()
        self._cached_detail_row_count = row[0] if row else 0
        row = self._conn.execute("SELECT MIN(finished_at) FROM invocations").fetchone()
        self._cached_oldest_at = row[0] if row and row[0] is not None else None

    def db_size_mb(self) -> float:
        """Approximate DB file size in MB."""
        row = self._conn.execute("PRAGMA page_count").fetchone()
        pages = row[0] if row else 0
        row = self._conn.execute("PRAGMA page_size").fetchone()
        page_size = row[0] if row else 4096
        return (pages * page_size) / (1024 * 1024)

    # -- metadata persistence -----------------------------------------------

    @_serialized
    def save_metadata(self, key: str, values: list[str]) -> None:
        """Replace all values for a metadata key."""
        self._conn.execute("DELETE FROM metadata WHERE key = ?", (key,))
        if values:
            self._conn.executemany(
                "INSERT OR IGNORE INTO metadata (key, value) VALUES (?, ?)",
                [(key, v) for v in values],
            )
        self._conn.commit()

    def load_metadata(self, key: str) -> list[str]:
        """Load all values for a metadata key."""
        rows = self._conn.execute(
            "SELECT value FROM metadata WHERE key = ? ORDER BY value",
            (key,),
        ).fetchall()
        return [r[0] for r in rows]

    # -- aggregate snapshots --------------------------------------------------

    @_serialized
    def save_snapshots(self, snapshots: list[tuple[str, float, bytes]]) -> int:
        """Batch-upsert aggregate snapshots: (task_name, snapshot_ts, data)."""
        if not snapshots:
            return 0
        self._conn.executemany(
            "INSERT OR REPLACE INTO aggregate_snapshots (task_name, snapshot_ts, data) VALUES (?, ?, ?)",
            snapshots,
        )
        self._conn.commit()
        return len(snapshots)

    def load_snapshots(self) -> list[tuple[str, float, bytes]]:
        """Load all aggregate snapshots."""
        rows = self._conn.execute(
            "SELECT task_name, snapshot_ts, data FROM aggregate_snapshots"
        ).fetchall()
        return rows

    def min_snapshot_ts(self) -> float | None:
        """Oldest snapshot timestamp, or None if table is empty."""
        row = self._conn.execute(
            "SELECT MIN(snapshot_ts) FROM aggregate_snapshots"
        ).fetchone()
        return row[0] if row and row[0] is not None else None

    @_serialized
    def purge_stale_snapshots(self, active_tasks: set[str]) -> int:
        """Remove snapshots for tasks no longer tracked in memory."""
        if not active_tasks:
            self._conn.execute("DELETE FROM aggregate_snapshots")
            self._conn.commit()
            return 0
        placeholders = ",".join("?" for _ in active_tasks)
        cur = self._conn.execute(
            f"DELETE FROM aggregate_snapshots WHERE task_name NOT IN ({placeholders})",
            list(active_tasks),
        )
        self._conn.commit()
        return cur.rowcount

    def close(self) -> None:
        self.checkpoint(truncate=True)
        self._conn.close()

    # -- WAL management -----------------------------------------------------

    @_serialized
    def checkpoint(self, *, truncate: bool = False) -> None:
        """Force WAL checkpoint.

        truncate=True: TRUNCATE mode — merges all WAL frames, waits for
        readers, then truncates the file to zero. Use on startup/shutdown
        when there are no concurrent readers; blocks writers while waiting.

        truncate=False (default): PASSIVE mode — checkpoints as many frames
        as possible without blocking readers or writers. Safe to call any
        time during normal operation.
        """
        mode = "TRUNCATE" if truncate else "PASSIVE"
        try:
            row = self._conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
            if row:
                busy, log_frames, checkpointed = row
                if busy:
                    logger.info(
                        "WAL checkpoint (%s) partial: %d/%d frames",
                        mode.lower(), checkpointed, log_frames,
                    )
                elif log_frames:
                    logger.info("WAL checkpoint (%s): %d frames", mode.lower(), checkpointed)
        except Exception:
            logger.exception("WAL checkpoint failed")

    def wal_size_mb(self) -> float:
        """WAL file size in MB (from filesystem)."""
        wal_path = Path(self.db_path + "-wal")
        try:
            return wal_path.stat().st_size / (1024 * 1024) if wal_path.exists() else 0.0
        except OSError:
            return 0.0

    # -- helpers ----------------------------------------------------------

    def disk_usage_pct(self) -> float:
        """Percentage of disk used on the partition hosting the DB file."""
        try:
            stat = os.statvfs(self.db_path)
            total = stat.f_blocks * stat.f_frsize
            free = stat.f_bavail * stat.f_frsize
            if total == 0:
                return 0.0
            return (1 - free / total) * 100
        except OSError:
            return 0.0

    def disk_free_mb(self) -> float:
        """Free disk space in MB on the partition hosting the DB file."""
        try:
            stat = os.statvfs(self.db_path)
            return (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
        except OSError:
            return 0.0

    def _row_to_record(self, row: tuple) -> InvocationRecord:
        # Core columns: 0-9 (task_id..exception_type)
        # Detail columns from LEFT JOIN: 10-12 (args_preview, kwargs_preview, traceback_snippet)
        return InvocationRecord(
            task_id=row[0],
            task_name=row[1],
            state=TaskState(row[2]),
            received_at=row[3],
            started_at=row[4],
            finished_at=row[5],
            runtime_ms=row[6],
            worker=row[7],
            queue=row[8],
            exception_type=row[9],
            args_preview=row[10] if len(row) > 10 else None,
            kwargs_preview=row[11] if len(row) > 11 else None,
            traceback_snippet=row[12] if len(row) > 12 else None,
        )
