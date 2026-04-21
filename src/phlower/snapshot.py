"""Serialize / deserialize TaskAggregate state for SQLite snapshot recovery."""

from __future__ import annotations

import base64
import json
import logging
import zlib
from collections import Counter

from fastdigest import TDigest

from .models import HourBucket, MinuteBucket

logger = logging.getLogger(__name__)

SNAPSHOT_VERSION = 1


def _serialize_tdigest(td: TDigest | None) -> str | None:
    if td is None or td.is_empty():
        return None
    return base64.b64encode(td.to_bytes()).decode("ascii")


def _deserialize_tdigest(data: str | None) -> TDigest | None:
    if data is None:
        return None
    return TDigest.from_bytes(base64.b64decode(data))


def _serialize_minute_bucket(b: MinuteBucket) -> dict:
    return {
        "ts": b.timestamp,
        "count": b.count,
        "success": b.success,
        "failure": b.failure,
        "retry": b.retry,
        "digest": _serialize_tdigest(b.digest),
    }


def _deserialize_minute_bucket(d: dict) -> MinuteBucket:
    return MinuteBucket(
        timestamp=d["ts"],
        count=d["count"],
        success=d["success"],
        failure=d["failure"],
        retry=d["retry"],
        digest=_deserialize_tdigest(d.get("digest")),
    )


def _serialize_hour_bucket(hb: HourBucket) -> dict:
    return {
        "ts": hb.timestamp,
        "count": hb.count,
        "success": hb.success,
        "failure": hb.failure,
        "retry": hb.retry,
    }


def _deserialize_hour_bucket(d: dict) -> HourBucket:
    return HourBucket(
        timestamp=d["ts"],
        count=d["count"],
        success=d["success"],
        failure=d["failure"],
        retry=d["retry"],
    )


def _serialize_counter_dict(d: dict[int, Counter[str]]) -> dict[str, dict[str, int]]:
    return {str(ts): dict(counter) for ts, counter in d.items()}


def _deserialize_counter_dict(d: dict[str, dict[str, int]]) -> dict[int, Counter[str]]:
    return {int(ts): Counter(counts) for ts, counts in d.items()}


def serialize_aggregate(agg) -> bytes:
    """Convert a TaskAggregate to a compressed JSON blob."""
    d = {
        "v": SNAPSHOT_VERSION,
        "buckets": {
            str(ts): _serialize_minute_bucket(b)
            for ts, b in agg.buckets.items()
        },
        "hourly_counts": {
            str(ts): _serialize_hour_bucket(hb)
            for ts, hb in agg.hourly_counts.items()
        },
        "hourly_digests": {
            str(ts): _serialize_tdigest(td)
            for ts, td in agg.hourly_digests.items()
        },
        "hourly_exceptions": _serialize_counter_dict(agg.hourly_exceptions),
        "hourly_workers": _serialize_counter_dict(agg.hourly_workers),
        "hourly_queues": _serialize_counter_dict(agg.hourly_queues),
        "runtime_digest": _serialize_tdigest(agg.runtime_digest),
    }
    return zlib.compress(json.dumps(d, separators=(",", ":")).encode())


def deserialize_aggregate(data: bytes, task_name: str):
    """Reconstruct a TaskAggregate from a compressed JSON blob."""
    from .store import TaskAggregate

    d = json.loads(zlib.decompress(data))

    if d.get("v", 0) != SNAPSHOT_VERSION:
        raise ValueError(f"unknown snapshot version {d.get('v')}")

    agg = TaskAggregate(task_name)

    agg.buckets = {
        int(ts): _deserialize_minute_bucket(b)
        for ts, b in d.get("buckets", {}).items()
    }
    agg.hourly_counts = {
        int(ts): _deserialize_hour_bucket(hb)
        for ts, hb in d.get("hourly_counts", {}).items()
    }
    for ts, td in d.get("hourly_digests", {}).items():
        if td is not None:
            digest = _deserialize_tdigest(td)
            if digest is not None:
                agg.hourly_digests[int(ts)] = digest
    agg.hourly_exceptions = _deserialize_counter_dict(d.get("hourly_exceptions", {}))
    agg.hourly_workers = _deserialize_counter_dict(d.get("hourly_workers", {}))
    agg.hourly_queues = _deserialize_counter_dict(d.get("hourly_queues", {}))

    rt = _deserialize_tdigest(d.get("runtime_digest"))
    if rt is not None:
        agg.runtime_digest = rt

    return agg
