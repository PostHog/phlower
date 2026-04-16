"""Worker registry — three-tier model for Celery worker topology.

Tracks three distinct entities with independent TTLs:

  Queue    — routing destination, defined in code, long-lived (24h TTL).
  Worker   — logical group derived from hostname, survives deploys (24h TTL).
  Instance — individual Celery worker process, ephemeral (3 min TTL).

Instances are the source of truth.  Queues and workers are derived views
maintained via ``last_seen`` timestamps that get refreshed whenever an
instance belonging to the group/queue is observed — either through
periodic ``celery inspect`` or through live task-event signals.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass, field

from celery import Celery

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TTL defaults (seconds)
# ---------------------------------------------------------------------------

INSTANCE_TTL = 180   # 3 missed inspects (~3 min)
WORKER_TTL = 86400   # 24 h — groups should always be visible
QUEUE_TTL = 86400    # 24 h — queues are defined in code

# ---------------------------------------------------------------------------
# Hostname → worker-group extraction
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Instance dataclass
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class InstanceInfo:
    """A single Celery worker process."""

    hostname: str
    group: str
    queues: list[str] = field(default_factory=list)
    last_seen_at: float = 0.0


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class WorkerRegistry:
    """Thread-safe registry of the three-tier worker topology.

    Instances are the physical source of truth. Worker groups and
    queues are higher-level concepts with their own (longer) TTLs so
    that pill UI elements remain stable across rolling deploys.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

        # Physical layer — hostname → instance info
        self._instances: dict[str, InstanceInfo] = {}

        # Logical layers — name → last_seen_at
        self._worker_last_seen: dict[str, float] = {}
        self._queue_last_seen: dict[str, float] = {}

        self.last_inspect_at: float = 0

    # -- inspect update ---------------------------------------------------

    def update(self, app: Celery, timeout: float = 5.0) -> None:
        """Run ``celery inspect`` and refresh instance/worker/queue state."""
        try:
            result = app.control.inspect(timeout=timeout).active_queues()
        except Exception:
            logger.debug("Inspect failed", exc_info=True)
            return

        if not result:
            return

        now = time.time()
        with self._lock:
            for hostname, queues in result.items():
                q_names = [q["name"] for q in queues]
                group = extract_worker_group(hostname)

                # Upsert instance
                inst = self._instances.get(hostname)
                if inst is None:
                    inst = InstanceInfo(hostname=hostname, group=group)
                    self._instances[hostname] = inst
                inst.queues = q_names
                inst.group = group
                inst.last_seen_at = now

                # Touch worker and queue timestamps
                self._worker_last_seen[group] = now
                for q in q_names:
                    self._queue_last_seen[q] = now

            self.last_inspect_at = now

        live_instances = len(result)
        with self._lock:
            live_queues = len(self._queue_last_seen)
            live_groups = len(self._worker_last_seen)
        logger.info(
            "Worker inspect: %d instances, %d queues, %d groups",
            live_instances, live_queues, live_groups,
        )

    # -- eviction ---------------------------------------------------------

    def evict_stale(self) -> None:
        """Remove instances, workers, and queues that exceed their TTL."""
        now = time.time()
        with self._lock:
            # Instances — short TTL
            stale_instances = [
                h for h, inst in self._instances.items()
                if now - inst.last_seen_at > INSTANCE_TTL
            ]
            for h in stale_instances:
                del self._instances[h]

            # Workers — long TTL
            stale_workers = [
                g for g, ts in self._worker_last_seen.items()
                if now - ts > WORKER_TTL
            ]
            for g in stale_workers:
                del self._worker_last_seen[g]

            # Queues — long TTL
            stale_queues = [
                q for q, ts in self._queue_last_seen.items()
                if now - ts > QUEUE_TTL
            ]
            for q in stale_queues:
                del self._queue_last_seen[q]

        if stale_instances or stale_workers or stale_queues:
            logger.info(
                "Evicted stale: %d instances, %d workers, %d queues",
                len(stale_instances), len(stale_workers), len(stale_queues),
            )

    # -- task-event signals -----------------------------------------------

    def note_queue(self, queue: str) -> None:
        """Mark a queue as seen (called from task-event handlers)."""
        now = time.time()
        with self._lock:
            self._queue_last_seen[queue] = now

    # -- lookups ----------------------------------------------------------

    def queues_for_worker(self, hostname: str) -> list[str]:
        """Return queues consumed by this instance (from last inspect)."""
        with self._lock:
            inst = self._instances.get(hostname)
            return list(inst.queues) if inst else []

    def group_for_worker(self, hostname: str) -> str:
        """Return the worker group for a hostname.

        If the hostname hasn't been seen via inspect yet (task event
        arrived before the next inspect cycle), derive the group from
        the hostname and create a minimal instance entry.
        """
        with self._lock:
            inst = self._instances.get(hostname)
            if inst:
                return inst.group

        # Not seen via inspect — derive from hostname
        group = extract_worker_group(hostname)
        now = time.time()
        with self._lock:
            # Double-check after re-acquiring lock
            inst = self._instances.get(hostname)
            if inst:
                return inst.group
            self._instances[hostname] = InstanceInfo(
                hostname=hostname, group=group, last_seen_at=now,
            )
            self._worker_last_seen[group] = now
        return group

    def all_groups(self) -> list[str]:
        """Sorted list of known (non-expired) worker groups."""
        with self._lock:
            return sorted(self._worker_last_seen.keys())

    def all_queues(self) -> list[str]:
        """Sorted list of known (non-expired) queues."""
        with self._lock:
            return sorted(self._queue_last_seen.keys())

    def worker_count(self) -> int:
        """Total number of live instances."""
        with self._lock:
            return len(self._instances)

    def workers_per_queue(self) -> dict[str, int]:
        """Count of live instances consuming each queue."""
        with self._lock:
            counts: dict[str, int] = {}
            for inst in self._instances.values():
                for q in inst.queues:
                    counts[q] = counts.get(q, 0) + 1
            return counts

    def workers_per_group(self) -> dict[str, int]:
        """Count of live instances in each worker group."""
        with self._lock:
            counts: dict[str, int] = {}
            for inst in self._instances.values():
                counts[inst.group] = counts.get(inst.group, 0) + 1
            return counts

    # -- persistence ------------------------------------------------------

    def seed(self, queues: list[str], groups: list[str]) -> None:
        """Pre-populate from persisted metadata so pills appear on startup.

        Seeds only touch the worker/queue layers (long TTL).  No fake
        instances are created — instance counts will read as zero until
        the first real inspect completes.
        """
        now = time.time()
        with self._lock:
            for q in queues:
                if q not in self._queue_last_seen:
                    self._queue_last_seen[q] = now
            for g in groups:
                if g not in self._worker_last_seen:
                    self._worker_last_seen[g] = now

    def snapshot(self) -> tuple[list[str], list[str]]:
        """Return (queues, groups) for persistence."""
        return self.all_queues(), self.all_groups()
