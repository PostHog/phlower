"""SQLite write-behind warm index for historical task ID lookups."""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path
from typing import Iterator

from .models import InvocationRecord, TaskState

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
    exception_type TEXT,
    args_preview TEXT,
    kwargs_preview TEXT,
    traceback_snippet TEXT
);
CREATE INDEX IF NOT EXISTS idx_inv_finished ON invocations (finished_at);
CREATE INDEX IF NOT EXISTS idx_inv_task_name ON invocations (task_name, finished_at);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (key, value)
);
"""

UPSERT_SQL = """
INSERT OR REPLACE INTO invocations
    (task_id, task_name, state, received_at, started_at, finished_at,
     runtime_ms, worker, queue, exception_type,
     args_preview, kwargs_preview, traceback_snippet)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

THIN_SQL = """
UPDATE invocations
SET args_preview=NULL, kwargs_preview=NULL, traceback_snippet=NULL
WHERE rowid IN (
    SELECT rowid FROM invocations
    WHERE finished_at < ? AND args_preview IS NOT NULL
    LIMIT 10000
)
"""


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._cached_row_count: int = 0
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
        """Add columns that may not exist in older databases."""
        cols = {
            row[1]
            for row in self._conn.execute("PRAGMA table_info(invocations)").fetchall()
        }
        for col in ("args_preview", "kwargs_preview", "traceback_snippet"):
            if col not in cols:
                self._conn.execute(f"ALTER TABLE invocations ADD COLUMN {col} TEXT")
        self._conn.commit()

    # -- writes -----------------------------------------------------------

    def flush_batch(self, records: list) -> int:
        if not records:
            return 0
        self._conn.executemany(
            UPSERT_SQL,
            [
                (
                    r.task_id,
                    r.task_name,
                    r.state,
                    r.received_at,
                    r.started_at,
                    r.finished_at,
                    r.runtime_ms,
                    r.worker,
                    r.queue,
                    r.exception_type,
                    r.args_preview,
                    r.kwargs_preview,
                    r.traceback_snippet,
                )
                for r in records
            ],
        )
        self._conn.commit()
        return len(records)

    def thin_details(self, cutoff_ts: float) -> int:
        """NULL out heavy fields (args/kwargs/traceback) for old records.
        Processes in 10K batches to avoid long write locks."""
        total = 0
        while True:
            cur = self._conn.execute(THIN_SQL, (cutoff_ts,))
            self._conn.commit()
            affected = cur.rowcount
            total += affected
            if affected < 10000:
                break
        return total

    def purge_expired(self, cutoff_ts: float) -> int:
        """Delete old rows in batches to avoid long write locks."""
        total = 0
        while True:
            cur = self._conn.execute(
                "DELETE FROM invocations WHERE rowid IN ("
                "SELECT rowid FROM invocations WHERE finished_at < ? LIMIT 50000"
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
            "SELECT * FROM invocations WHERE task_id = ?", (task_id,)
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
        clauses = ["task_name = ?"]
        params: list[object] = [task_name]
        if before_ts is not None:
            clauses.append("finished_at < ?")
            params.append(before_ts)
        if after_ts is not None:
            clauses.append("finished_at > ?")
            params.append(after_ts)
        where = " AND ".join(clauses)
        # Fetch extra rows to compensate for exclude_ids filtering
        fetch_limit = limit + (len(exclude_ids) if exclude_ids else 0)
        sql = f"SELECT * FROM invocations WHERE {where} ORDER BY finished_at DESC LIMIT ?"
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

    def row_count(self) -> int:
        row = self._conn.execute("SELECT count(*) FROM invocations").fetchone()
        count = row[0] if row else 0
        self._cached_row_count = count
        return count

    def db_size_mb(self) -> float:
        """Approximate DB file size in MB."""
        row = self._conn.execute("PRAGMA page_count").fetchone()
        pages = row[0] if row else 0
        row = self._conn.execute("PRAGMA page_size").fetchone()
        page_size = row[0] if row else 4096
        return (pages * page_size) / (1024 * 1024)

    # -- metadata persistence -----------------------------------------------

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

    def close(self) -> None:
        self.checkpoint(truncate=True)
        self._conn.close()

    # -- WAL management -----------------------------------------------------

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

    def _row_to_record(self, row: tuple) -> InvocationRecord:
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
