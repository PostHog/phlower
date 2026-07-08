"""Tests for the chunked SQLite purge primitives.

These exercise the real SQLiteStore against a tmp_path SQLite file — no
mocks of SQLite itself. Row counts stay small (thousands) so the suite runs
in seconds. The purge trickles in small serialized chunks because a single
DROP+vacuum under one lock hold can take many minutes on a large database,
starving the flush loop until its write-behind buffer overflows; these tests
lock in the chunk-resume and shrink behaviour.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from phlower.models import InvocationRecord, TaskState
from phlower.sqlite_store import SQLiteStore, _suffix_for_ts


def _ts_days_ago(days: float) -> float:
    return time.time() - days * 86400


def _record(task_id: str, finished_at: float, *, with_details: bool = True) -> InvocationRecord:
    return InvocationRecord(
        task_id=task_id,
        task_name="app.tasks.do_thing",
        state=TaskState.SUCCESS,
        received_at=finished_at - 2,
        started_at=finished_at - 1,
        finished_at=finished_at,
        runtime_ms=1000.0,
        worker="worker-1",
        queue="celery",
        args_preview="(1, 2)" if with_details else None,
        kwargs_preview='{"a": 1}' if with_details else None,
        traceback_snippet=None,
    )


def _make_store(tmp_path) -> SQLiteStore:
    store = SQLiteStore(str(tmp_path / "phlower.db"))
    store.init_schema()
    return store


def _table_exists(store: SQLiteStore, name: str) -> bool:
    row = store._conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _insert_day(store: SQLiteStore, finished_at: float, count: int) -> str:
    """Insert ``count`` records that all land in one UTC-day partition."""
    suffix = _suffix_for_ts(finished_at)
    records = [_record(f"{suffix}-{i}", finished_at) for i in range(count)]
    store.flush_batch(records)
    return suffix


# -- list_expired_partition_suffixes ----------------------------------------


def test_expired_listing_honours_cutoff_boundary(tmp_path):
    store = _make_store(tmp_path)
    now = time.time()

    # Three partitions: 5 days ago, 3 days ago, today.
    old_suffix = _insert_day(store, _ts_days_ago(5), 10)
    mid_suffix = _insert_day(store, _ts_days_ago(3), 10)
    today_suffix = _insert_day(store, now, 10)

    # Retention of exactly 3 days: cutoff day == mid partition's day, so
    # mid is NOT strictly older and must be kept. Only the 5-day-old goes.
    retention_hours = 72
    expired = store.list_expired_partition_suffixes(retention_hours)

    assert old_suffix in expired
    assert mid_suffix not in expired  # partition exactly at cutoff is kept
    assert today_suffix not in expired


def test_expired_listing_is_oldest_first(tmp_path):
    store = _make_store(tmp_path)
    _insert_day(store, _ts_days_ago(5), 5)
    _insert_day(store, _ts_days_ago(7), 5)
    _insert_day(store, _ts_days_ago(6), 5)

    expired = store.list_expired_partition_suffixes(48)
    assert expired == sorted(expired)


# -- purge_partition_step ---------------------------------------------------


def test_purge_partition_step_multiple_steps_then_zero(tmp_path):
    store = _make_store(tmp_path)
    finished = _ts_days_ago(5)
    suffix = _insert_day(store, finished, 250)
    inv_tbl = f"invocations_{suffix}"
    det_tbl = f"invocation_details_{suffix}"

    assert _table_exists(store, inv_tbl)
    assert _table_exists(store, det_tbl)

    # max_rows smaller than the row count -> needs multiple steps.
    steps = []
    for _ in range(100):  # generous upper bound so a bug can't loop forever
        deleted = store.purge_partition_step(suffix, max_rows=40)
        steps.append(deleted)
        if deleted == 0:
            break

    assert steps[0] > 0
    assert steps[-1] == 0
    assert len(steps) > 2  # confirm it genuinely took multiple steps

    # Both tables gone from sqlite_master afterwards.
    assert not _table_exists(store, inv_tbl)
    assert not _table_exists(store, det_tbl)
    assert suffix not in store._ensured_partitions


def _det_row_count(store: SQLiteStore, det_tbl: str) -> int:
    row = store._conn.execute(f"SELECT COUNT(*) FROM {det_tbl}").fetchone()
    return row[0] if row else 0


def _inv_row_count(store: SQLiteStore, inv_tbl: str) -> int:
    row = store._conn.execute(f"SELECT COUNT(*) FROM {inv_tbl}").fetchone()
    return row[0] if row else 0


def test_purge_partition_step_keeps_details_table_until_end(tmp_path):
    """Details table stays until invocations is fully drained.

    Every read query LEFT JOINs invocation_details_{suffix} while
    invocations_{suffix} exists, so the details table must not be dropped
    early — it survives (empty) until the final step drops both together.
    """
    store = _make_store(tmp_path)
    finished = _ts_days_ago(5)
    suffix = _insert_day(store, finished, 200)
    inv_tbl = f"invocations_{suffix}"
    det_tbl = f"invocation_details_{suffix}"

    details_drained_while_inv_has_rows = False
    for _ in range(100):
        deleted = store.purge_partition_step(suffix, max_rows=40)
        if (
            _table_exists(store, det_tbl)
            and _det_row_count(store, det_tbl) == 0
            and _table_exists(store, inv_tbl)
            and _inv_row_count(store, inv_tbl) > 0
        ):
            details_drained_while_inv_has_rows = True
        if deleted == 0:
            break

    assert details_drained_while_inv_has_rows, (
        "details table should still EXIST (empty) while invocations still has rows"
    )
    # Final step drops both together.
    assert not _table_exists(store, inv_tbl)
    assert not _table_exists(store, det_tbl)


def test_reads_survive_partial_purge_with_drained_details(tmp_path):
    """Regression: reads must not raise while a partition is mid-purge.

    Once the details rows are drained but invocations still has rows (and the
    details table is intentionally kept for the LEFT JOIN), list_by_task and
    search must still work — they used to raise ``no such table`` when the
    details table was dropped early.
    """
    store = _make_store(tmp_path)
    now = time.time()

    # One expiring partition to purge, plus a recent one so reads have data.
    old_suffix = _insert_day(store, _ts_days_ago(5), 200)
    _insert_day(store, now, 50)
    old_inv = f"invocations_{old_suffix}"
    old_det = f"invocation_details_{old_suffix}"

    # Step until the details rows are drained but invocations still has rows.
    reached_drained_state = False
    for _ in range(100):
        deleted = store.purge_partition_step(old_suffix, max_rows=40)
        if (
            _table_exists(store, old_det)
            and _det_row_count(store, old_det) == 0
            and _table_exists(store, old_inv)
            and _inv_row_count(store, old_inv) > 0
        ):
            reached_drained_state = True
            break
        if deleted == 0:
            break

    assert reached_drained_state, "could not reach drained-details state to test"

    # Neither read path may raise, and both return sane results.
    listed = store.list_by_task("app.tasks.do_thing", limit=10)
    assert all(r.task_name == "app.tasks.do_thing" for r in listed)

    found = store.search(task_name="app.tasks.do_thing", limit=10)
    assert all(r.task_name == "app.tasks.do_thing" for r in found)


# -- vacuum_step ------------------------------------------------------------


def _freelist_count(store: SQLiteStore) -> int:
    row = store._conn.execute("PRAGMA freelist_count").fetchone()
    return row[0] if row else 0


def _page_count(store: SQLiteStore) -> int:
    row = store._conn.execute("PRAGMA page_count").fetchone()
    return row[0] if row else 0


def test_vacuum_step_drains_freelist_in_bounded_chunks(tmp_path):
    store = _make_store(tmp_path)
    # Enough rows to free a good number of pages when purged.
    finished = _ts_days_ago(5)
    suffix = _insert_day(store, finished, 5000)

    # Fully purge the partition (rows -> free pages) without vacuuming.
    while store.purge_partition_step(suffix, max_rows=5000) > 0:
        pass

    free_before = _freelist_count(store)
    pages_before = _page_count(store)
    assert free_before > 0

    # max_pages smaller than freelist -> multiple calls needed.
    max_pages = max(1, free_before // 3)
    calls = 0
    remaining = store.vacuum_step(max_pages)
    calls += 1
    while remaining > 0:
        remaining = store.vacuum_step(max_pages)
        calls += 1
        assert calls < 1000  # guard against an unbounded loop

    assert calls > 1  # bounded chunks genuinely needed more than one call
    assert _freelist_count(store) == 0
    assert _page_count(store) < pages_before  # file actually shrank


def test_vacuum_step_noop_when_freelist_empty(tmp_path):
    store = _make_store(tmp_path)
    assert store.vacuum_step(100) == 0


def test_vacuum_step_noop_when_auto_vacuum_not_incremental(tmp_path):
    """Guard against an infinite loop on a DB created without the pragma.

    A file created without ``auto_vacuum=INCREMENTAL`` runs in NONE mode, so
    ``incremental_vacuum`` is a no-op and ``freelist_count`` never drops.
    vacuum_step must return 0 immediately (not spin) even with free pages.
    """
    import sqlite3

    db_path = str(tmp_path / "legacy.db")

    # Create + populate + drop a table via a plain connection, so the file
    # ends up in auto_vacuum=NONE with a non-empty freelist.
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE junk (id INTEGER PRIMARY KEY, blob TEXT)")
    conn.executemany(
        "INSERT INTO junk (blob) VALUES (?)",
        [("x" * 500,) for _ in range(5000)],
    )
    conn.commit()
    conn.execute("DROP TABLE junk")
    conn.commit()
    assert conn.execute("PRAGMA auto_vacuum").fetchone()[0] == 0  # NONE
    conn.close()

    store = SQLiteStore(db_path)
    store.init_schema()

    # Freelist is genuinely non-empty, but vacuum can't reclaim it.
    assert _freelist_count(store) > 0
    assert store.vacuum_step(100) == 0


# -- end-to-end -------------------------------------------------------------


def test_full_pass_purges_expired_and_keeps_recent(tmp_path):
    store = _make_store(tmp_path)
    now = time.time()

    old1 = _insert_day(store, _ts_days_ago(6), 500)
    old2 = _insert_day(store, _ts_days_ago(5), 500)
    recent = _insert_day(store, now, 500)

    # Retention 96h (4 days): expires the 5- and 6-day-old partitions.
    retention_hours = 96
    expired = store.list_expired_partition_suffixes(retention_hours)
    assert set(expired) == {old1, old2}

    for suffix in expired:
        while store.purge_partition_step(suffix, max_rows=200) > 0:
            pass

    # Drain the freelist in chunks.
    while store.vacuum_step(1000) > 0:
        pass

    # Expired partitions gone, recent intact.
    assert not _table_exists(store, f"invocations_{old1}")
    assert not _table_exists(store, f"invocations_{old2}")
    assert _table_exists(store, f"invocations_{recent}")

    row = store._conn.execute(
        f"SELECT COUNT(*) FROM invocations_{recent}"
    ).fetchone()
    assert row[0] == 500

    # Recent records still readable through the public read path.
    results = store.list_by_task("app.tasks.do_thing", limit=10)
    assert len(results) == 10
    assert all(r.task_name == "app.tasks.do_thing" for r in results)

    assert _freelist_count(store) == 0
