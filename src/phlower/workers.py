"""Worker registry — periodically inspects Celery workers for metadata.

Provides worker→queue mapping and worker group extraction without config.
"""

from __future__ import annotations

import logging
import re
import threading
import time

from celery import Celery

logger = logging.getLogger(__name__)

_NODE_PREFIX = re.compile(r"^node@(?:posthog-worker-django-)?")
# K8s pod names end with replicaset+pod hashes. Formats vary:
#   -f98fbdbbc-54nrz  (with dash)
#   -7c75fdbff6wzs7   (concatenated, no dash between rs and pod hash)
# Match: a dash followed by 10-15 alphanumeric chars at the end
_K8S_HASH_SUFFIX = re.compile(r"-[a-z0-9]{10,15}$")


def extract_worker_group(hostname: str) -> str:
    """Extract the consumer type from a Celery worker hostname.

    node@posthog-worker-django-default-f98fbdbbc-54nrz → default
    node@posthog-worker-django-feature-flags-long-running-7c75fdbff6wzs7 → feature-flags-long-running
    node@posthog-worker-django-session-replay-worker-68c44cbdf6-2668h → session-replay-worker
    node@my-worker → my-worker
    """
    name = _NODE_PREFIX.sub("", hostname)
    # Try stripping the concatenated hash (no dash between rs+pod)
    stripped = _K8S_HASH_SUFFIX.sub("", name)
    # If that didn't change anything, try the two-segment pattern
    if stripped == name:
        stripped = re.sub(r"-[a-f0-9]{7,10}-[a-z0-9]{4,6}$", "", name)
    return stripped or hostname


class WorkerRegistry:
    """Thread-safe registry of worker metadata from celery inspect."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # hostname → list of queue names
        self._worker_queues: dict[str, list[str]] = {}
        # hostname → group label
        self._worker_groups: dict[str, str] = {}
        self.last_inspect_at: float = 0

    def update(self, app: Celery, timeout: float = 5.0) -> None:
        """Run celery inspect and update mappings."""
        try:
            result = app.control.inspect(timeout=timeout).active_queues()
        except Exception:
            logger.debug("Inspect failed", exc_info=True)
            return

        if not result:
            return

        with self._lock:
            for hostname, queues in result.items():
                q_names = [q["name"] for q in queues]
                self._worker_queues[hostname] = q_names
                self._worker_groups[hostname] = extract_worker_group(hostname)
            self.last_inspect_at = time.time()

        with self._lock:
            total_queues = sum(len(qs) for qs in self._worker_queues.values())
        logger.info(
            "Worker inspect: %d workers, %d unique queues, %d unique groups",
            len(result),
            total_queues,
            len(set(self._worker_groups.values())),
        )

    def queues_for_worker(self, hostname: str) -> list[str]:
        with self._lock:
            return self._worker_queues.get(hostname, [])

    def group_for_worker(self, hostname: str) -> str:
        with self._lock:
            cached = self._worker_groups.get(hostname)
            if cached:
                return cached
        # Not seen via inspect yet — derive from hostname directly
        group = extract_worker_group(hostname)
        with self._lock:
            self._worker_groups[hostname] = group
        return group

    def all_groups(self) -> list[str]:
        with self._lock:
            return sorted(set(self._worker_groups.values()))

    def all_queues(self) -> list[str]:
        with self._lock:
            queues: set[str] = set()
            for qs in self._worker_queues.values():
                queues.update(qs)
            return sorted(queues)

    def worker_count(self) -> int:
        with self._lock:
            return sum(1 for k in self._worker_queues if not k.startswith("_seed"))

    def seed(self, queues: list[str], groups: list[str]) -> None:
        """Pre-populate from persisted metadata so pills appear on startup."""
        with self._lock:
            # Use a synthetic hostname so these don't collide with real workers.
            # Real inspect results will overwrite with actual worker→queue mappings.
            if queues and "_seed" not in self._worker_queues:
                self._worker_queues["_seed"] = queues
            for group in groups:
                if group not in self._worker_groups.values():
                    self._worker_groups[f"_seed_{group}"] = group

    def snapshot(self) -> tuple[list[str], list[str]]:
        """Return (queues, groups) for persistence."""
        return self.all_queues(), self.all_groups()
