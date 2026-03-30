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
    exception_type TEXT
);
CREATE INDEX IF NOT EXISTS idx_inv_finished ON invocations (finished_at);
CREATE INDEX IF NOT EXISTS idx_inv_task_name ON invocations (task_name, finished_at);
"""

UPSERT_SQL = """
INSERT OR REPLACE INTO invocations
    (task_id, task_name, state, received_at, started_at, finished_at,
     runtime_ms, worker, queue, exception_type)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    # -- writes -----------------------------------------------------------

    def flush_batch(self, records: list) -> int:
        """INSERT OR REPLACE a batch of CompletedRecords. Returns count."""
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
                )
                for r in records
            ],
        )
        self._conn.commit()
        return len(records)

    def purge_expired(self, cutoff_ts: float) -> int:
        cur = self._conn.execute(
            "DELETE FROM invocations WHERE finished_at < ?", (cutoff_ts,)
        )
        self._conn.commit()
        return cur.rowcount

    # -- reads ------------------------------------------------------------

    def lookup_task_id(self, task_id: str) -> InvocationRecord | None:
        row = self._conn.execute(
            "SELECT * FROM invocations WHERE task_id = ?", (task_id,)
        ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    def load_recovery_data(self, since_ts: float) -> Iterator[sqlite3.Row]:
        """Yield rows for aggregate rebuild, ordered by task_name."""
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.execute(
            "SELECT task_name, state, finished_at, runtime_ms, worker, queue, "
            "exception_type FROM invocations "
            "WHERE finished_at >= ? ORDER BY task_name, finished_at",
            (since_ts,),
        )
        yield from cur
        self._conn.row_factory = None

    def row_count(self) -> int:
        row = self._conn.execute("SELECT count(*) FROM invocations").fetchone()
        return row[0] if row else 0

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
        )
