"""Celery event consumer — runs in a daemon thread, writes to Store."""

from __future__ import annotations

import logging
import socket
import threading
import time
from queue import Empty

from celery import Celery

from .config import Config
from .store import Store

logger = logging.getLogger(__name__)


class CeleryEventConsumer:
    def __init__(self, config: Config, store: Store) -> None:
        self.config = config
        self.store = store
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.connected: bool = False
        self.last_error: str | None = None

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="celery-events"
        )
        self._thread.start()
        logger.info("Celery event consumer started (broker=%s)", self.config.broker_url)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # -- main loop --------------------------------------------------------

    def _run(self) -> None:
        app = Celery(broker=self.config.broker_url)
        was_connected = False

        while not self._stop.is_set():
            try:
                with app.connection() as conn:
                    if not was_connected:
                        logger.info("Connected to broker")
                        was_connected = True
                    self.connected = True
                    self.last_error = None
                    recv = app.events.Receiver(
                        conn,
                        handlers={
                            "task-received": self._on_received,
                            "task-started": self._on_started,
                            "task-succeeded": self._on_succeeded,
                            "task-failed": self._on_failed,
                            "task-retried": self._on_retried,
                        },
                    )
                    recv.capture(limit=None, timeout=1.0, wakeup=True)
            except (socket.timeout, TimeoutError, Empty):
                # Normal timeout when no events arrive — just loop back
                continue
            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                was_connected = False
                if self._stop.is_set():
                    break
                logger.warning("Broker connection lost — retrying in 5 s (%s)", exc)
                self._stop.wait(5)

    # -- handlers ---------------------------------------------------------

    def _on_received(self, event: dict) -> None:
        queue = event.get("queue") or event.get("routing_key")
        self.store.process_received(
            task_id=event["uuid"],
            task_name=event.get("name", "unknown"),
            ts=event.get("timestamp") or time.time(),
            args=event.get("args"),
            kwargs=event.get("kwargs"),
            queue=queue,
        )

    def _on_started(self, event: dict) -> None:
        self.store.process_started(
            task_id=event["uuid"],
            ts=event.get("timestamp") or time.time(),
            task_name=event.get("name"),
            worker=event.get("hostname"),
        )

    def _on_succeeded(self, event: dict) -> None:
        runtime = event.get("runtime")
        self.store.process_succeeded(
            task_id=event["uuid"],
            ts=event.get("timestamp") or time.time(),
            task_name=event.get("name"),
            runtime_ms=runtime * 1000 if runtime else None,
        )

    def _on_failed(self, event: dict) -> None:
        runtime = event.get("runtime")
        exception_str = event.get("exception", "")
        exc_type = exception_str.split("(")[0] if exception_str else None

        # Try to extract task name from NotRegistered errors
        task_name = event.get("name")
        if not task_name and exc_type == "NotRegistered" and "'" in exception_str:
            task_name = exception_str.split("'")[1]

        self.store.process_failed(
            task_id=event["uuid"],
            ts=event.get("timestamp") or time.time(),
            task_name=task_name,
            runtime_ms=runtime * 1000 if runtime else None,
            exception_type=exc_type,
            exception_message=exception_str,
            traceback_snippet=event.get("traceback"),
        )

    def _on_retried(self, event: dict) -> None:
        exception_str = event.get("exception", "")
        exc_type = exception_str.split("(")[0] if exception_str else None

        self.store.process_retried(
            task_id=event["uuid"],
            ts=event.get("timestamp") or time.time(),
            task_name=event.get("name"),
            exception_type=exc_type,
            exception_message=exception_str,
            traceback_snippet=event.get("traceback"),
        )
