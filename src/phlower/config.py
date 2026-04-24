from __future__ import annotations

import os
import re
from dataclasses import dataclass, field


def _parse_list(raw: str) -> list[str]:
    return [t.strip() for t in raw.split(",") if t.strip()]


@dataclass(frozen=True)
class Config:
    broker_url: str = field(
        default_factory=lambda: os.environ.get(
            "CELERY_BROKER_URL", "redis://localhost:6379/0"
        )
    )
    retention_hours: int = field(
        default_factory=lambda: int(os.environ.get("RETENTION_HOURS", "48"))
    )
    aggregate_retention_hours: int = field(
        default_factory=lambda: int(os.environ.get("AGGREGATE_RETENTION_HOURS", "168"))
    )
    sse_invocation_throttle_seconds: float = field(
        default_factory=lambda: float(os.environ.get("SSE_INVOCATION_THROTTLE_SECONDS", "0.6"))
    )
    task_watchlist: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            _parse_list(os.environ.get("TASK_WATCHLIST", ""))
        )
    )
    task_allowlist_regex: re.Pattern[str] = field(
        default_factory=lambda: re.compile(
            os.environ.get("TASK_ALLOWLIST_REGEX", ".*")
        )
    )
    max_args_preview_chars: int = field(
        default_factory=lambda: int(os.environ.get("MAX_ARGS_PREVIEW_CHARS", "500"))
    )
    max_kwargs_preview_chars: int = field(
        default_factory=lambda: int(os.environ.get("MAX_KWARGS_PREVIEW_CHARS", "1000"))
    )
    max_runtime_buffer: int = field(
        default_factory=lambda: int(
            os.environ.get("MAX_RUNTIME_BUFFER_PER_TASK", "10000")
        )
    )
    max_runtimes_per_bucket: int = field(
        default_factory=lambda: int(os.environ.get("MAX_RUNTIMES_PER_BUCKET", "500"))
    )
    sse_throttle_seconds: float = field(
        default_factory=lambda: float(os.environ.get("SSE_THROTTLE_SECONDS", "0.3"))
    )
    sqlite_path: str | None = field(
        default_factory=lambda: os.environ.get("SQLITE_PATH")
    )
    sqlite_recovery_hours: int = field(
        default_factory=lambda: int(os.environ.get("SQLITE_RECOVERY_HOURS", "48"))
    )
    sqlite_detail_hours: int = field(
        default_factory=lambda: int(os.environ.get("SQLITE_DETAIL_HOURS", "60"))
    )
    sqlite_invocation_retention_hours: int = field(
        default_factory=lambda: int(os.environ.get("SQLITE_INVOCATION_RETENTION_HOURS", "120"))
    )
    sqlite_disk_usage_pct_cap: int = field(
        default_factory=lambda: int(os.environ.get("SQLITE_DISK_USAGE_PCT_CAP", "85"))
    )
    snapshot_interval_seconds: int = field(
        default_factory=lambda: int(os.environ.get("SNAPSHOT_INTERVAL_SECONDS", "60"))
    )
