"""Tests for the traceback / exception message caps.

Failure and retry storms can carry very large tracebacks on every event.
Without a cap they land unbounded in memory and in the SQLite detail table,
so these tests lock in that the store trims them on ingest.
"""

from __future__ import annotations

import time
from dataclasses import replace

from phlower.config import Config
from phlower.store import Store, truncate_text


def _make_store() -> Store:
    config = replace(Config(), max_traceback_chars=50, max_exception_chars=20)
    return Store(config)


def _long_traceback() -> str:
    frames = "".join(f'  File "app/tasks.py", line {i}, in step_{i}\n' for i in range(200))
    return "Traceback (most recent call last):\n" + frames + "ValueError: boom"


def test_truncate_text_passes_none_through():
    assert truncate_text(None, 10) is None
    assert truncate_text(None, 10, keep_tail=True) is None


def test_truncate_text_leaves_short_values_untouched():
    assert truncate_text("short", 10) == "short"
    assert truncate_text("short", 10, keep_tail=True) == "short"
    assert truncate_text("exactly10!", 10) == "exactly10!"


def test_truncate_text_keeps_head_by_default():
    assert truncate_text("abcdefghij", 4) == "abcd…"


def test_truncate_text_keeps_tail_for_tracebacks():
    assert truncate_text("abcdefghij", 4, keep_tail=True) == "…\nghij"


def test_process_failed_caps_traceback_and_exception_message():
    store = _make_store()
    traceback = _long_traceback()

    store.process_failed(
        "task-1",
        time.time(),
        task_name="app.tasks.do_thing",
        exception_type="ValueError",
        exception_message="x" * 500,
        traceback_snippet=traceback,
    )

    rec = store.invocations["task-1"]
    assert rec.traceback_snippet == "…\n" + traceback[-50:]
    assert rec.traceback_snippet.endswith("ValueError: boom")
    assert rec.exception_message == "x" * 20 + "…"


def test_process_retried_caps_traceback_and_exception_message():
    store = _make_store()
    traceback = _long_traceback()

    store.process_retried(
        "task-1",
        time.time(),
        task_name="app.tasks.do_thing",
        exception_type="ValueError",
        exception_message="y" * 500,
        traceback_snippet=traceback,
    )

    rec = store.invocations["task-1"]
    assert rec.traceback_snippet == "…\n" + traceback[-50:]
    assert rec.exception_message == "y" * 20 + "…"


def test_process_failed_keeps_short_values_untouched():
    store = _make_store()

    store.process_failed(
        "task-1",
        time.time(),
        task_name="app.tasks.do_thing",
        exception_type="ValueError",
        exception_message="boom",
        traceback_snippet="ValueError: boom",
    )

    rec = store.invocations["task-1"]
    assert rec.traceback_snippet == "ValueError: boom"
    assert rec.exception_message == "boom"
