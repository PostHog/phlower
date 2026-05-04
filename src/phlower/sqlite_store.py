"""SQLite write-behind store with daily-partitioned invocation tables.

Partitions are named ``invocations_YYYYMMDD`` and ``invocation_details_YYYYMMDD``
(UTC date suffix). Purge becomes ``DROP TABLE`` — a metadata operation that
takes milliseconds, so the hourly purge loop never starves the flush loop the
way row-by-row ``DELETE`` did on multi-million-row tables.

The first startup against a pre-partition database renames the existing
single tables to ``invocations_legacy`` / ``invocation_details_legacy``;
they get unioned into reads until their data ages past retention, then
they're dropped.
"""

from __future__ import annotations

import functools
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
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

# Singleton tables — never partitioned, never grow with invocation volume.
SINGLETON_SCHEMA = """
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

INV_COLUMNS = (
    "task_id TEXT PRIMARY KEY, "
    "task_name TEXT NOT NULL, "
    "state TEXT NOT NULL, "
    "received_at REAL, "
    "started_at REAL, "
    "finished_at REAL, "
    "runtime_ms REAL, "
    "worker TEXT, "
    "queue TEXT, "
    "exception_type TEXT"
)

DETAILS_COLUMNS = (
    "task_id TEXT PRIMARY KEY, "
    "args_preview TEXT, "
    "kwargs_preview TEXT, "
    "traceback_snippet TEXT"
)

UPSERT_INV_SQL = (
    "INSERT OR REPLACE INTO {tbl} "
    "(task_id, task_name, state, received_at, started_at, finished_at, "
    " runtime_ms, worker, queue, exception_type) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

UPSERT_DETAILS_SQL = (
    "INSERT OR REPLACE INTO {tbl} "
    "(task_id, args_preview, kwargs_preview, traceback_snippet) "
    "VALUES (?, ?, ?, ?)"
)

LEGACY_INV = "invocations_legacy"
LEGACY_DETAILS = "invocation_details_legacy"

# Validates partition suffixes against SQL injection — table names can't be
# parameterized in SQLite, so any name we splice into SQL must match this.
_PARTITION_SUFFIX_RE = re.compile(r"^\d{8}$")
_INV_TABLE_RE = re.compile(r"^invocations_(\d{8})$")
_DETAILS_TABLE_RE = re.compile(r"^invocation_details_(\d{8})$")


def _suffix_for_ts(ts: float) -> str:
    """Return UTC date suffix YYYYMMDD for a unix timestamp."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d")


def _ts_for_suffix(suffix: str) -> float:
    """Return UTC midnight unix timestamp for a YYYYMMDD suffix."""
    return datetime.strptime(suffix, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp()


class SQLiteStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._cached_row_count: int = 0
        self._cached_detail_row_count: int = 0
        self._cached_oldest_at: float | None = None
        self._write_lock = threading.Lock()
        # Suffixes we've verified exist this process — avoids issuing
        # "CREATE TABLE IF NOT EXISTS" on every flush.
        self._ensured_partitions: set[str] = set()
        self._has_legacy_inv: bool = False
        self._has_legacy_details: bool = False
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = self._connect(db_path)

    def _connect(self, path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, check_same_thread=False)
        # Incremental auto_vacuum: tracks freed pages in a separate list so
        # ``PRAGMA incremental_vacuum`` can return them to the OS without a
        # full VACUUM. SQLite requires this PRAGMA to be set BEFORE any
        # tables are created; on existing DBs it's a no-op without a
        # follow-up full VACUUM. We set it here so a fresh DB picks it up
        # at first init_schema() — keeps the file size tracking live data
        # instead of the high-water mark.
        conn.execute("PRAGMA auto_vacuum=INCREMENTAL")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        # Cap WAL file size after successful checkpoint — prevents the file
        # from staying at its high-water mark forever after a growth spike.
        conn.execute("PRAGMA journal_size_limit=67108864")  # 64 MB
        return conn

    def init_schema(self) -> None:
        self._conn.executescript(SINGLETON_SCHEMA)
        self._migrate_to_partitions()
        self._refresh_legacy_flags()
        # Always make sure today's partition exists at startup, so the first
        # flush after boot doesn't race with creation.
        self.ensure_partition(_suffix_for_ts(time.time()))
        # Checkpoint any WAL inherited from a crash — must happen before
        # recovery opens its read connection, otherwise the stale WAL blocks
        # checkpointing for the entire recovery duration.
        self.checkpoint(truncate=True)

    # -- migration --------------------------------------------------------

    def _table_exists(self, name: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone()
        return row is not None

    def _migrate_to_partitions(self) -> None:
        """Rename pre-partition single tables to *_legacy on first boot.

        Rename is a SQLite metadata operation — instant even on multi-GB
        tables. The legacy tables get unioned into reads until their data
        ages past retention, then dropped.
        """
        if not self._table_exists("invocations") and not self._table_exists(
            "invocation_details"
        ):
            return

        # Run the existing column-split migration (args/kwargs out of
        # invocations) before renaming, so legacy data lands in the
        # canonical layout — _split_legacy_columns() creates the
        # invocation_details table itself, so re-check existence after.
        if self._table_exists("invocations"):
            cols = {
                row[1]
                for row in self._conn.execute("PRAGMA table_info(invocations)").fetchall()
            }
            if "args_preview" in cols:
                self._split_legacy_columns()

        if self._table_exists("invocations") and not self._table_exists(LEGACY_INV):
            logger.info("Migrating: ALTER TABLE invocations RENAME TO %s", LEGACY_INV)
            self._conn.execute(f"ALTER TABLE invocations RENAME TO {LEGACY_INV}")
        if self._table_exists("invocation_details") and not self._table_exists(
            LEGACY_DETAILS
        ):
            logger.info(
                "Migrating: ALTER TABLE invocation_details RENAME TO %s", LEGACY_DETAILS
            )
            self._conn.execute(
                f"ALTER TABLE invocation_details RENAME TO {LEGACY_DETAILS}"
            )
        self._conn.commit()

    def _split_legacy_columns(self) -> None:
        """Move args_preview/kwargs_preview/traceback_snippet out of invocations."""
        logger.info("Splitting legacy invocations table (extracting detail columns)")
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
                "INSERT OR IGNORE INTO invocation_details "
                "(task_id, args_preview, kwargs_preview, traceback_snippet) "
                "SELECT task_id, args_preview, kwargs_preview, traceback_snippet "
                "FROM invocations "
                "WHERE (args_preview IS NOT NULL OR kwargs_preview IS NOT NULL "
                "       OR traceback_snippet IS NOT NULL) "
                "AND task_id NOT IN (SELECT task_id FROM invocation_details) "
                "LIMIT 50000"
            )
            self._conn.commit()
            batch = cur.rowcount
            total += batch
            if batch > 0:
                logger.info("Split-table migration progress: %d rows copied", total)
            if batch < 50000:
                break
        for col in ("args_preview", "kwargs_preview", "traceback_snippet"):
            self._conn.execute(f"ALTER TABLE invocations DROP COLUMN {col}")
        self._conn.commit()
        logger.info("Split-table migration complete — %d detail rows", total)

    def _refresh_legacy_flags(self) -> None:
        self._has_legacy_inv = self._table_exists(LEGACY_INV)
        self._has_legacy_details = self._table_exists(LEGACY_DETAILS)

    # -- partition discovery ----------------------------------------------

    def list_partition_suffixes(self) -> list[str]:
        """Suffixes (YYYYMMDD) for all existing invocation partitions, newest first."""
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'invocations_%'"
        ).fetchall()
        suffixes: list[str] = []
        for (name,) in rows:
            m = _INV_TABLE_RE.match(name)
            if m:
                suffixes.append(m.group(1))
        suffixes.sort(reverse=True)
        return suffixes

    @_serialized
    def ensure_partition(self, suffix: str) -> None:
        """Idempotently create the invocations + details partition for a date."""
        if suffix in self._ensured_partitions:
            return
        if not _PARTITION_SUFFIX_RE.match(suffix):
            raise ValueError(f"invalid partition suffix: {suffix!r}")
        inv_tbl = f"invocations_{suffix}"
        det_tbl = f"invocation_details_{suffix}"
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {inv_tbl} ({INV_COLUMNS})")
        self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{inv_tbl}_finished "
            f"ON {inv_tbl}(finished_at)"
        )
        self._conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{inv_tbl}_task_name "
            f"ON {inv_tbl}(task_name, finished_at)"
        )
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {det_tbl} ({DETAILS_COLUMNS})"
        )
        self._conn.commit()
        self._ensured_partitions.add(suffix)

    # -- writes -----------------------------------------------------------

    def flush_batch(self, records: list) -> int:
        """Persist completed records, grouping by their UTC date.

        Note: ``INSERT OR REPLACE`` only deduplicates within one partition,
        so a task whose lifecycle straddles midnight UTC (e.g. RETRY at
        23:59, SUCCESS at 00:01) can have two rows in two partitions. Read
        paths dedupe by task_id at query time. Aggregate recovery may
        slightly inflate counts for these tasks — bounded by the fraction
        of tasks crossing midnight, typically <1%.
        """
        if not records:
            return 0
        # Group by partition suffix (UTC date of finished_at, falling back
        # to received_at/started_at for terminal-without-finished edge case).
        by_suffix: dict[str, list] = {}
        for r in records:
            ts = r.finished_at or r.started_at or r.received_at or time.time()
            suffix = _suffix_for_ts(ts)
            by_suffix.setdefault(suffix, []).append(r)

        # Make sure each partition exists. Cheap: cached after first call.
        for suffix in by_suffix:
            self.ensure_partition(suffix)

        with self._write_lock:
            for suffix, group in by_suffix.items():
                inv_tbl = f"invocations_{suffix}"
                det_tbl = f"invocation_details_{suffix}"
                self._conn.executemany(
                    UPSERT_INV_SQL.format(tbl=inv_tbl),
                    [
                        (
                            r.task_id, r.task_name, r.state,
                            r.received_at, r.started_at, r.finished_at,
                            r.runtime_ms, r.worker, r.queue, r.exception_type,
                        )
                        for r in group
                    ],
                )
                details = []
                detail_deletes = []
                for r in group:
                    if r.args_preview or r.kwargs_preview or r.traceback_snippet:
                        details.append(
                            (r.task_id, r.args_preview, r.kwargs_preview, r.traceback_snippet)
                        )
                    else:
                        detail_deletes.append((r.task_id,))
                if details:
                    self._conn.executemany(UPSERT_DETAILS_SQL.format(tbl=det_tbl), details)
                if detail_deletes:
                    self._conn.executemany(
                        f"DELETE FROM {det_tbl} WHERE task_id = ?", detail_deletes
                    )
            self._conn.commit()
        return len(records)

    # -- purge ------------------------------------------------------------

    @_serialized
    def purge_old_partitions(self, retention_hours: int) -> int:
        """Drop partitions older than ``retention_hours``. Returns count dropped.

        Each DROP TABLE is a metadata operation (fast, predictable); the
        whole purge replaces the multi-minute row-by-row DELETE that
        previously starved the flush loop.

        After the drops, ``incremental_vacuum`` returns the freed pages
        to the filesystem — without it SQLite holds onto them as internal
        free pages and the file never shrinks, eventually filling the PVC.
        """
        cutoff_ts = time.time() - retention_hours * 3600
        cutoff_suffix = _suffix_for_ts(cutoff_ts)
        dropped = 0
        for suffix in self.list_partition_suffixes():
            if suffix >= cutoff_suffix:
                continue
            inv_tbl = f"invocations_{suffix}"
            det_tbl = f"invocation_details_{suffix}"
            self._conn.execute(f"DROP TABLE IF EXISTS {inv_tbl}")
            self._conn.execute(f"DROP TABLE IF EXISTS {det_tbl}")
            self._ensured_partitions.discard(suffix)
            dropped += 1
            logger.info("Dropped expired partition %s", suffix)
        self._conn.commit()
        # Legacy tables: drop wholesale once their newest row is past
        # retention. Cheap to check — single MAX() per table.
        self._maybe_drop_legacy(cutoff_ts)
        if dropped or not self._has_legacy_inv:
            self._reclaim_free_pages()
        return dropped

    def _reclaim_free_pages(self) -> None:
        """Return any freed pages to the OS via incremental_vacuum.

        No-op unless the DB was created with ``auto_vacuum=INCREMENTAL``;
        on those DBs it's fast (proportional to free-page count, not DB
        size). Called after DROP TABLE so file size tracks live data.

        The PRAGMA emits one result row per freed page, so the cursor
        MUST be drained — without ``fetchall()`` only one page gets
        reclaimed and the file barely shrinks.
        """
        try:
            row = self._conn.execute("PRAGMA freelist_count").fetchone()
            free_pages = row[0] if row else 0
            if free_pages == 0:
                return
            self._conn.execute("PRAGMA incremental_vacuum").fetchall()
            self._conn.commit()
            logger.info("incremental_vacuum reclaimed %d pages", free_pages)
        except Exception:
            logger.exception("incremental_vacuum failed")

    def _maybe_drop_legacy(self, cutoff_ts: float) -> None:
        if self._has_legacy_inv:
            row = self._conn.execute(
                f"SELECT MAX(finished_at) FROM {LEGACY_INV}"
            ).fetchone()
            newest = row[0] if row else None
            if newest is None or newest < cutoff_ts:
                logger.info("Dropping legacy table %s (newest=%s)", LEGACY_INV, newest)
                self._conn.execute(f"DROP TABLE {LEGACY_INV}")
                self._has_legacy_inv = False
        if self._has_legacy_details and not self._has_legacy_inv:
            # Details on its own carries no finished_at — drop it whenever
            # the corresponding invocations table is gone.
            logger.info("Dropping legacy table %s", LEGACY_DETAILS)
            self._conn.execute(f"DROP TABLE {LEGACY_DETAILS}")
            self._has_legacy_details = False
        self._conn.commit()

    # -- read helpers -----------------------------------------------------

    def _read_tables(self) -> list[tuple[str, str]]:
        """Return [(invocations_table, details_table_or_None), ...] newest first.

        Includes legacy as the oldest source.
        """
        out: list[tuple[str, str]] = []
        for suffix in self.list_partition_suffixes():
            out.append((f"invocations_{suffix}", f"invocation_details_{suffix}"))
        if self._has_legacy_inv:
            out.append((LEGACY_INV, LEGACY_DETAILS if self._has_legacy_details else ""))
        return out

    def _union_subqueries(
        self, where_sql: str, params: list[object], *, with_details: bool = True
    ) -> tuple[str, list[object]]:
        """Build a UNION ALL across all read tables for a given WHERE clause.

        ``where_sql`` and ``params`` are duplicated for each branch.
        """
        tables = self._read_tables()
        if not tables:
            return "SELECT NULL WHERE 0", []
        all_params: list[object] = []
        branches: list[str] = []
        for inv_tbl, det_tbl in tables:
            if with_details and det_tbl:
                join = (
                    f"FROM {inv_tbl} i LEFT JOIN {det_tbl} d ON i.task_id = d.task_id"
                )
                cols = (
                    "i.task_id, i.task_name, i.state, i.received_at, i.started_at, "
                    "i.finished_at, i.runtime_ms, i.worker, i.queue, i.exception_type, "
                    "d.args_preview, d.kwargs_preview, d.traceback_snippet"
                )
            else:
                join = f"FROM {inv_tbl} i"
                cols = (
                    "i.task_id, i.task_name, i.state, i.received_at, i.started_at, "
                    "i.finished_at, i.runtime_ms, i.worker, i.queue, i.exception_type, "
                    "NULL, NULL, NULL"
                )
            branches.append(f"SELECT {cols} {join} WHERE {where_sql}")
            all_params.extend(params)
        return " UNION ALL ".join(branches), all_params

    # -- reads ------------------------------------------------------------

    def lookup_task_id(self, task_id: str) -> InvocationRecord | None:
        for inv_tbl, det_tbl in self._read_tables():
            if det_tbl:
                sql = (
                    "SELECT i.task_id, i.task_name, i.state, i.received_at, i.started_at, "
                    "  i.finished_at, i.runtime_ms, i.worker, i.queue, i.exception_type, "
                    "  d.args_preview, d.kwargs_preview, d.traceback_snippet "
                    f"FROM {inv_tbl} i LEFT JOIN {det_tbl} d ON i.task_id = d.task_id "
                    "WHERE i.task_id = ?"
                )
            else:
                sql = (
                    "SELECT i.task_id, i.task_name, i.state, i.received_at, i.started_at, "
                    "  i.finished_at, i.runtime_ms, i.worker, i.queue, i.exception_type, "
                    "  NULL, NULL, NULL "
                    f"FROM {inv_tbl} i WHERE i.task_id = ?"
                )
            row = self._conn.execute(sql, (task_id,)).fetchone()
            if row:
                return self._row_to_record(row)
        return None

    def list_by_task(
        self,
        task_name: str,
        *,
        limit: int = 100,
        before_ts: float | None = None,
        after_ts: float | None = None,
        exclude_ids: set[str] | None = None,
    ) -> list[InvocationRecord]:
        """List invocations for a task, newest first."""
        clauses = ["i.task_name = ?"]
        params: list[object] = [task_name]
        if before_ts is not None:
            clauses.append("i.finished_at < ?")
            params.append(before_ts)
        if after_ts is not None:
            clauses.append("i.finished_at > ?")
            params.append(after_ts)
        where = " AND ".join(clauses)
        union_sql, union_params = self._union_subqueries(where, params)
        fetch_limit = limit + (len(exclude_ids) if exclude_ids else 0)
        sql = (
            f"SELECT * FROM ({union_sql}) "
            "ORDER BY finished_at DESC LIMIT ?"
        )
        # Over-fetch to leave room for dedup. Cross-partition duplicates
        # are bounded by the fraction of tasks whose lifecycle straddles
        # midnight UTC — rare in practice — but a fetch_limit of just
        # ``limit`` could underfill the result if duplicates show up.
        union_params.append(fetch_limit * 2)
        rows = self._conn.execute(sql, union_params).fetchall()
        seen_ids: set[str] = set()
        results: list[InvocationRecord] = []
        for row in rows:
            tid = row[0]
            if tid in seen_ids:
                continue  # cross-partition dedup — rows arrive newest-first
            seen_ids.add(tid)
            if exclude_ids and tid in exclude_ids:
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
        union_sql, union_params = self._union_subqueries(where, params)
        fetch_limit = limit + (len(exclude_ids) if exclude_ids else 0)
        sql = (
            f"SELECT * FROM ({union_sql}) "
            "ORDER BY finished_at DESC LIMIT ? OFFSET ?"
        )
        union_params.extend([fetch_limit * 2, offset])
        rows = self._conn.execute(sql, union_params).fetchall()
        seen_ids: set[str] = set()
        results: list[InvocationRecord] = []
        for row in rows:
            tid = row[0]
            if tid in seen_ids:
                continue  # cross-partition dedup — rows arrive newest-first
            seen_ids.add(tid)
            if exclude_ids and tid in exclude_ids:
                continue
            results.append(self._row_to_record(row))
            if len(results) >= limit:
                break
        return results

    # -- recovery loaders -------------------------------------------------
    #
    # Read from a separate connection (so the write lock isn't held) and
    # iterate per-partition. Each partition's data is naturally bounded by
    # one UTC day, which keeps the per-statement memory footprint stable.

    def open_recovery_conn(self) -> sqlite3.Connection:
        return self._connect(self.db_path)

    def _recovery_inv_tables(self) -> list[str]:
        tables = [t for t, _ in self._read_tables()]
        return tables

    def load_recovery_counts(
        self, conn: sqlite3.Connection, since_ts: float
    ) -> Iterator[sqlite3.Row]:
        """Per-task/state/minute count rows. Chunked in 4-hour windows so
        the read snapshot is released periodically — important on the
        unbounded legacy table where a full scan can take minutes."""
        for inv_tbl in self._recovery_inv_tables():
            now = time.time()
            chunk_start = since_ts
            while chunk_start < now:
                chunk_end = min(chunk_start + 14400, now + 1)
                cur = conn.cursor()
                cur.row_factory = sqlite3.Row
                cur.execute(
                    f"SELECT task_name, state, "
                    f"  (CAST(finished_at AS INTEGER) / 60 * 60) AS minute_ts, "
                    f"  COUNT(*) AS cnt, "
                    f"  worker, queue, exception_type "
                    f"FROM {inv_tbl} WHERE finished_at >= ? AND finished_at < ? "
                    f"GROUP BY task_name, state, minute_ts, worker, queue, exception_type "
                    f"ORDER BY task_name",
                    (chunk_start, chunk_end),
                )
                yield from cur
                cur.close()
                conn.commit()
                chunk_start = chunk_end

    def load_recovery_runtimes(
        self, conn: sqlite3.Connection, since_ts: float
    ) -> Iterator[sqlite3.Row]:
        """Stream individual runtime values for t-digest population."""
        for inv_tbl in self._recovery_inv_tables():
            now = time.time()
            chunk_start = since_ts
            while chunk_start < now:
                chunk_end = min(chunk_start + 14400, now + 1)
                cur = conn.cursor()
                cur.row_factory = sqlite3.Row
                cur.execute(
                    f"SELECT task_name, "
                    f"  (CAST(finished_at AS INTEGER) / 60 * 60) AS minute_ts, "
                    f"  runtime_ms "
                    f"FROM {inv_tbl} "
                    f"WHERE finished_at >= ? AND finished_at < ? "
                    f"  AND runtime_ms IS NOT NULL "
                    f"ORDER BY task_name",
                    (chunk_start, chunk_end),
                )
                yield from cur
                cur.close()
                conn.commit()
                chunk_start = chunk_end

    def load_recovery_pickup(
        self, conn: sqlite3.Connection, since_ts: float
    ) -> Iterator[sqlite3.Row]:
        """Stream received_at/started_at pairs for pickup latency rebuild.

        Only reads the newest two partitions — pickup latency is a recent-
        traffic signal, no value in pulling 5 days of history.
        """
        tables = self._recovery_inv_tables()[:2]
        for inv_tbl in tables:
            cur = conn.cursor()
            cur.row_factory = sqlite3.Row
            cur.execute(
                f"SELECT queue, (started_at - received_at) * 1000 AS wait_ms "
                f"FROM {inv_tbl} "
                f"WHERE finished_at >= ? "
                f"  AND received_at IS NOT NULL AND started_at IS NOT NULL "
                f"  AND started_at > received_at "
                f"ORDER BY finished_at DESC LIMIT 5000",
                (since_ts,),
            )
            yield from cur
            cur.close()

    @_serialized
    def refresh_cached_stats(self) -> None:
        """Update cached stats for healthz — called from purge loop.

        Daily partitions are size-bounded (~1 day of data) so ``COUNT(*)``
        is cheap. The legacy table is multi-GB, so we use ``MAX(rowid)`` —
        index-fast and accurate enough for an approximate healthz number
        that disappears once legacy is dropped.
        """
        total_inv = 0
        total_det = 0
        oldest: float | None = None
        for inv_tbl, det_tbl in self._read_tables():
            if inv_tbl == LEGACY_INV:
                row = self._conn.execute(
                    f"SELECT MAX(rowid) FROM {inv_tbl}"
                ).fetchone()
                total_inv += row[0] if row and row[0] is not None else 0
            else:
                row = self._conn.execute(f"SELECT count(*) FROM {inv_tbl}").fetchone()
                total_inv += row[0] if row else 0
            if det_tbl:
                if det_tbl == LEGACY_DETAILS:
                    row = self._conn.execute(
                        f"SELECT MAX(rowid) FROM {det_tbl}"
                    ).fetchone()
                    total_det += row[0] if row and row[0] is not None else 0
                else:
                    row = self._conn.execute(
                        f"SELECT count(*) FROM {det_tbl}"
                    ).fetchone()
                    total_det += row[0] if row else 0
            # MIN(finished_at) uses idx_inv_finished — fast on legacy too.
            row = self._conn.execute(
                f"SELECT MIN(finished_at) FROM {inv_tbl}"
            ).fetchone()
            if row and row[0] is not None:
                oldest = row[0] if oldest is None else min(oldest, row[0])
        self._cached_row_count = total_inv
        self._cached_detail_row_count = total_det
        self._cached_oldest_at = oldest

    def db_size_mb(self) -> float:
        """Approximate DB file size in MB."""
        row = self._conn.execute("PRAGMA page_count").fetchone()
        pages = row[0] if row else 0
        row = self._conn.execute("PRAGMA page_size").fetchone()
        page_size = row[0] if row else 4096
        return (pages * page_size) / (1024 * 1024)

    # -- metadata persistence ---------------------------------------------

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

    # -- aggregate snapshots ----------------------------------------------

    @_serialized
    def save_snapshots(self, snapshots: list[tuple[str, float, bytes]]) -> int:
        """Batch-upsert aggregate snapshots: (task_name, snapshot_ts, data)."""
        if not snapshots:
            return 0
        self._conn.executemany(
            "INSERT OR REPLACE INTO aggregate_snapshots "
            "(task_name, snapshot_ts, data) VALUES (?, ?, ?)",
            snapshots,
        )
        self._conn.commit()
        return len(snapshots)

    def load_snapshots(self) -> list[tuple[str, float, bytes]]:
        rows = self._conn.execute(
            "SELECT task_name, snapshot_ts, data FROM aggregate_snapshots"
        ).fetchall()
        return rows

    def min_snapshot_ts(self) -> float | None:
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

    # -- WAL management ---------------------------------------------------

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
        try:
            stat = os.statvfs(self.db_path)
            return (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
        except OSError:
            return 0.0

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
