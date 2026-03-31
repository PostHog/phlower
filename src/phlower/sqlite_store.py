"""SQLite write-behind warm index for historical task ID lookups."""

from __future__ import annotations

import logging
import sqlite3
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
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._connect(db_path)

    def _connect(self, path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def init_schema(self) -> None:
        self._conn.executescript(SCHEMA)
        # Add columns if upgrading from older schema
        self._migrate()

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

    def load_recovery_data(self, since_ts: float) -> Iterator[sqlite3.Row]:
        cur = self._conn.cursor()
        cur.row_factory = sqlite3.Row
        cur.execute(
            "SELECT task_name, state, finished_at, runtime_ms, worker, queue, "
            "exception_type FROM invocations "
            "WHERE finished_at >= ? ORDER BY task_name, finished_at",
            (since_ts,),
        )
        yield from cur

    def row_count(self) -> int:
        row = self._conn.execute("SELECT count(*) FROM invocations").fetchone()
        return row[0] if row else 0

    def db_size_mb(self) -> float:
        """Approximate DB file size in MB."""
        row = self._conn.execute("PRAGMA page_count").fetchone()
        pages = row[0] if row else 0
        row = self._conn.execute("PRAGMA page_size").fetchone()
        page_size = row[0] if row else 4096
        return (pages * page_size) / (1024 * 1024)

    def close(self) -> None:
        self._conn.close()

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
