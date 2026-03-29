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
from .workers import WorkerRegistry

logger = logging.getLogger(__name__)

INSPECT_INTERVAL = 60  # seconds


class CeleryEventConsumer:
    def __init__(self, config: Config, store: Store) -> None:
        self.config = config
        self.store = store
        self.registry = WorkerRegistry()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.connected: bool = False
        self.last_error: str | None = None
        self.reconnect_count: int = 0

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
        last_inspect = 0.0

        while not self._stop.is_set():
            try:
                with app.connection() as conn:
                    if not was_connected:
                        logger.info("Connected to broker")
                        was_connected = True
                    self.connected = True
                    self.last_error = None
                    self.reconnect_count = 0

                    # Run inspect on connect and periodically
                    now = time.time()
                    if now - last_inspect > INSPECT_INTERVAL:
                        self.registry.update(app)
                        last_inspect = now

                    recv = app.events.Receiver(
                        conn,
                        handlers={
                            "task-received": self._on_received,
                            "task-started": self._on_started,
                            "task-succeeded": self._on_succeeded,
                            "task-failed": self._on_failed,
                            "task-retried": self._on_retried,
                        },
                        # Only subscribe to task.* events — excludes worker
                        # heartbeats which can burst and overflow Redis
                        # pub/sub output buffers (client-output-buffer-limit).
                        routing_key="task.#",
                    )
                    recv.capture(limit=None, timeout=1.0, wakeup=True)
            except (socket.timeout, TimeoutError, Empty):
                # Normal timeout — check if inspect is due, then loop back
                now = time.time()
                if now - last_inspect > INSPECT_INTERVAL:
                    try:
                        self.registry.update(app)
                        last_inspect = now
                    except Exception:
                        pass
                continue
            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                self.reconnect_count += 1
                was_connected = False
                if self._stop.is_set():
                    break
                delay = min(60, 2 ** min(self.reconnect_count, 6))
                logger.warning(
                    "Broker connection lost (attempt %d) — retrying in %ds (%s)",
                    self.reconnect_count, delay, exc,
                )
                self._stop.wait(delay)

    # -- queue enrichment -------------------------------------------------

    def _resolve_queue(self, event: dict) -> str | None:
        """Best-effort queue: event field → worker inspect data → None."""
        queue = event.get("queue") or event.get("routing_key")
        if queue:
            return queue
        hostname = event.get("hostname")
        if hostname:
            queues = self.registry.queues_for_worker(hostname)
            if len(queues) == 1:
                return queues[0]
        return None

    def _resolve_worker_group(self, hostname: str | None) -> str | None:
        if not hostname:
            return None
        return self.registry.group_for_worker(hostname)

    # -- handlers ---------------------------------------------------------

    def _on_received(self, event: dict) -> None:
        self.store.process_received(
            task_id=event["uuid"],
            task_name=event.get("name", "unknown"),
            ts=event.get("timestamp") or time.time(),
            args=event.get("args"),
            kwargs=event.get("kwargs"),
            queue=self._resolve_queue(event),
        )

    def _on_started(self, event: dict) -> None:
        hostname = event.get("hostname")
        self.store.process_started(
            task_id=event["uuid"],
            ts=event.get("timestamp") or time.time(),
            task_name=event.get("name"),
            worker=hostname,
            worker_group=self._resolve_worker_group(hostname),
            queue=self._resolve_queue(event),
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
