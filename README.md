# Phlower

Phlower is a task-centric Celery monitor. Single container, optional SQLite persistence, live updates via SSE.

Answers questions like:
- Is this task healthy right now?
- Did something regress in the last hours?
- Why did this particular run fail?
- Find the run tied to order #12345

Not a cluster monitoring tool — no worker/queue dashboards, no autoscaling metrics. Just task behavior.

## Quick start

```bash
docker run -p 8100:8100 \
  -e CELERY_BROKER_URL=redis://host.docker.internal:6379/0 \
  ghcr.io/posthog/phlower:latest
```

Phlower turns on task events on the workers by broadcasting `enable_events` every 5 seconds, the same way Flower does. Workers do not need `-E`. To leave events to the workers (`-E` or `worker_send_task_events`), set `ENABLE_WORKER_EVENTS=false`.

Open `http://localhost:8100`.

For persistent history across restarts, mount a volume and set `SQLITE_PATH`:

```bash
docker run -p 8100:8100 \
  -e CELERY_BROKER_URL=redis://host.docker.internal:6379/0 \
  -e SQLITE_PATH=/data/phlower.db \
  -v phlower-data:/data \
  ghcr.io/posthog/phlower:latest
```

## What you get

**Task list** — all observed task types with rate (tasks/min), failure rate, p50/p95/p99 latency, sparklines. Filter by queue or worker group.

**Task detail** — latency + throughput charts, exception/queue/worker distribution, recent invocations.

**Search** — find invocations by task name, status, worker, queue, task ID, or free-text across args/kwargs/errors.

**Invocation detail** — full lifecycle (received -> started -> finished), runtime, worker, queue, args/kwargs preview, exception + traceback.

**Live ticker** — real-time tasks/sec counter. Everything updates via SSE with ~300ms latency.

## Auto-discovery

Phlower tracks three entities with independent lifecycles:

- **Queue** — routing destination (e.g. `celery`, `analytics`). Discovered from `celery inspect` and live task events. Stays visible for 24 hours after last signal.
- **Worker group** — logical group derived from hostnames (e.g. `posthog-worker-django-analytics-queries-abc123` -> `analytics-queries`). Stays visible for 24 hours.
- **Instance** — individual Celery worker process. Evicted after 3 minutes without an inspect response.

Periodic `celery inspect` (every 60s) refreshes state. Task events also feed queue names, so queues appear as soon as a task is routed. Pickup latency (p95 wait time per queue) is shown in filter pills.

No configuration needed.

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `CELERY_BROKER_URL` | `redis://localhost:6379/0` | Broker connection string |
| `PORT` | `8100` | HTTP listen port |
| `ENABLE_WORKER_EVENTS` | `true` | Broadcast `enable_events` to all workers every 5s so they send task events without `-E`. Set to `false` when workers enable events themselves. |
| `RETENTION_HOURS` | `48` | In-memory invocation record retention |
| `AGGREGATE_RETENTION_HOURS` | `168` | Hourly rollup retention (7 days) |
| `TASK_ALLOWLIST_REGEX` | `.*` | Only track matching task names |
| `TASK_WATCHLIST` | unset | Comma-separated task names. Parsed but not used yet. |
| `MAX_ARGS_PREVIEW_CHARS` | `500` | Max characters of task args kept per invocation |
| `MAX_KWARGS_PREVIEW_CHARS` | `1000` | Max characters of task kwargs kept per invocation |
| `MAX_TRACEBACK_CHARS` | `4000` | Max stored traceback length; keeps the tail (last frames + exception line) |
| `MAX_EXCEPTION_CHARS` | `1000` | Max stored exception message length; keeps the head |
| `MAX_RUNTIME_BUFFER_PER_TASK` | `10000` | Parsed but not used yet |
| `MAX_RUNTIMES_PER_BUCKET` | `500` | Parsed but not used yet |
| `SSE_THROTTLE_SECONDS` | `0.3` | SSE push interval |
| `SSE_INVOCATION_THROTTLE_SECONDS` | `0.6` | Push interval for invocation updates |
| `SQLITE_PATH` | unset | Path to SQLite DB. Enables persistence when set. |
| `SQLITE_RECOVERY_HOURS` | `48` | Fallback row-replay window (only used when no snapshots exist) |
| `SQLITE_DETAIL_HOURS` | `60` | Parsed but not used yet. Detail rows currently live and expire with their invocation partition. |
| `SQLITE_INVOCATION_RETENTION_HOURS` | `120` | Invocation history retention in SQLite. Whole day partitions older than this are dropped (hourly check). |
| `SQLITE_DISK_USAGE_PCT_CAP` | `75` | Disk usage % that triggers an emergency purge. Retention is halved per pass until usage drops below the cap. |
| `SQLITE_PURGE_CHUNK_ROWS` | `50000` | Rows deleted per step when purging an expired partition |
| `SQLITE_VACUUM_CHUNK_PAGES` | `10000` | Free pages returned to the OS per vacuum step after a purge |
| `SQLITE_PENDING_BUFFER_CAP` | `200000` | Max completed invocations buffered for the SQLite writer. Oldest are dropped when exceeded. |
| `DETAIL_RATE_THRESHOLD` | `500` | Successful invocations of tasks above this many invocations/min (5-min average) skip storing args/kwargs. Failures and retries always keep their detail. `0` disables the check. |
| `SNAPSHOT_INTERVAL_SECONDS` | `60` | How often to persist aggregate snapshots |

## SQLite persistence

Without `SQLITE_PATH`, Phlower runs entirely in memory. A restart loses all history. This is fine for development and short-lived clusters but not for production.

Setting `SQLITE_PATH` enables a WAL-mode SQLite database that serves two purposes:

**Invocation history** — every completed task invocation (success, failure, retry) is written as an individual row. This powers the search/detail UI and lets you look up historical invocations that have already been evicted from memory. Rows are written in 1.5s batches. Rows live in daily (UTC) partitions, and partitions older than `SQLITE_INVOCATION_RETENTION_HOURS` are dropped by an hourly purge loop, in chunks so the writer is not starved. If disk usage exceeds `SQLITE_DISK_USAGE_PCT_CAP`, the purge halves the retention window until usage is back under the cap. Args and kwargs of successful invocations are skipped for tasks running above `DETAIL_RATE_THRESHOLD` invocations/min to save space. Failures and retries always keep args, kwargs and traceback. Tracebacks and exception messages are truncated when the event is ingested, before they reach memory, the SSE stream or SQLite: tracebacks keep their last `MAX_TRACEBACK_CHARS` characters (the final frames and exception line), messages keep their first `MAX_EXCEPTION_CHARS`. The full payload is not stored anywhere.

**Aggregate snapshots** — every `SNAPSHOT_INTERVAL_SECONDS`, Phlower serializes the in-memory TaskAggregate state (hourly counters, TDigest percentiles, exception/worker/queue distributions) as compressed blobs. On restart, these snapshots are restored directly instead of replaying individual rows. A short gap-replay covers events between the last snapshot and the crash/shutdown.

Why snapshots instead of replaying rows: at high throughput the invocations table can accumulate millions of rows in the recovery window. Replaying them all through TDigest merges at startup causes massive transient memory allocation that fragments glibc's malloc arenas. The RSS spike never comes back down. Snapshots sidestep this entirely — recovery reads ~100 compressed blobs instead of millions of rows.

**First deploy with an existing DB:** Backwards compatible. The new `aggregate_snapshots` table is created automatically. The first restart falls back to full row-replay (no snapshots yet). The snapshot loop starts writing immediately. Every subsequent restart uses snapshots.

**Limits and sizing:**
- The DB grows proportionally to event throughput. At ~75 events/sec, expect ~10 GB/week before purging kicks in.
- WAL mode allows concurrent reads and writes. The WAL file is capped at 64 MB via `journal_size_limit`.
- A VACUUM would reclaim deleted-row space but requires ~2x the DB size in temp disk and blocks writes. In practice, let the purge loop handle lifecycle — don't VACUUM in production.
- Aggregate snapshots are small (~5-100 KB per task compressed). Total snapshot footprint for 120 tasks is ~10-15 MB.

## Running in Kubernetes

Single-pod deployment. See `k8s/` for example manifests.

For persistence, attach a PVC and set `SQLITE_PATH`. The DB is self-managing — schema migrations, WAL checkpoints, and data purging all happen automatically.

Resource sizing: memory usage depends on event throughput and retention settings. In-memory invocation records are bounded by `RETENTION_HOURS`. With default settings and moderate throughput (~50 events/sec), steady-state RSS is ~300-500 MB. Set memory limits accordingly, with headroom for the initial recovery phase.

## Development

Backend (FastAPI):
```bash
uv sync
uv run python -m phlower
```

Frontend (React + Vite):
```bash
cd frontend
pnpm install
pnpm dev
```

Vite dev server proxies API calls to `localhost:8100`.

Use `scripts/fake_tasks.py` to generate test traffic:
```bash
uv run celery -A scripts.fake_tasks worker -l info -c 2
uv run python scripts/fake_tasks.py
```

Docker build (includes frontend):
```bash
docker build -t phlower .
docker run -p 8100:8100 -e CELERY_BROKER_URL=redis://host.docker.internal:6379/0 phlower
```

## Architecture

This section documents the internal design for contributors and agents working on the codebase.

### Process model

Single Python process, single event loop. Celery events are consumed in a daemon thread (`CeleryEventConsumer` in `events.py`) which writes to the thread-safe `Store` (`store.py`). FastAPI request handlers and background async loops read from the same Store.

### Data flow

```
Celery broker
  |
  v
CeleryEventConsumer (daemon thread)
  |
  v
Store (in-memory, thread-safe via threading.Lock)
  |
  +---> SSE push loop (300ms) ---> browser clients
  |
  +---> SQLite flush loop (1.5s) ---> invocations table
  |
  +---> Snapshot loop (60s) ---> aggregate_snapshots table
  |
  +---> Eviction loop (30min) ---> coarsen + evict old data
  |
  +---> API request handlers ---> JSON responses
```

### In-memory data structures

**`TaskAggregate`** (one per observed task name, ~100-120 in a typical cluster):
- `buckets: dict[int, MinuteBucket]` — per-minute counters + TDigest. Only the last 2h (hot window). Older buckets are coarsened into hourly rollups.
- `hourly_counts: dict[int, HourBucket]` — coarsened counts per hour. Kept for `AGGREGATE_RETENTION_HOURS` (7 days).
- `hourly_digests: dict[int, TDigest]` — merged runtime percentiles per hour.
- `hourly_exceptions/workers/queues: dict[int, Counter]` — attribution counters per hour.
- `runtime_digest: TDigest` — global runtime distribution for all-time percentiles.

**`InvocationRecord`** — per-invocation state (task_id, timestamps, runtime, worker, queue, args/kwargs, exception info). With SQLite, completed records leave memory once they are flushed, and older ones are read back from SQLite. Without SQLite, records are evicted after `RETENTION_HOURS`, and the oldest are dropped early once more than 50,000 are held.

**`WorkerRegistry`** — three-tier topology (instances -> groups -> queues) with TTL-based eviction. Updated by periodic `celery inspect` and enriched by live task events.

### Eviction and coarsening

The eviction loop runs every 30 minutes:

1. **Coarsen**: per-minute buckets older than 2h are merged into hourly rollups. TDigests are merged via `merge_inplace`, counts are summed. Minute buckets are then deleted.
2. **Evict hourly data**: rollups older than `AGGREGATE_RETENTION_HOURS` are deleted.
3. **Evict invocations**: records older than `RETENTION_HOURS` are removed from memory.
4. **Release memory**: `gc.collect()` + `malloc_trim(0)` (Linux/glibc only).

The eviction interval matters for memory — running too frequently creates allocation churn that fragments glibc arenas. 30 minutes is the current balance.

### SQLite tables

**`invocations`** — one row per completed task invocation. Primary key is `task_id`. Indexed on `finished_at` and `(task_name, finished_at)`. Stored in daily partitions (`invocations_YYYYMMDD`, with detail fields in matching `invocation_details_YYYYMMDD` tables); a partition is dropped once it is older than `SQLITE_INVOCATION_RETENTION_HOURS`.

**`aggregate_snapshots`** — one row per task name. Contains a zlib-compressed JSON blob with the full TaskAggregate state (counters, TDigest centroids as base64-encoded bytes, attribution counters). Updated every `SNAPSHOT_INTERVAL_SECONDS` for dirty tasks. Used for fast recovery on restart.

**`metadata`** — key-value pairs for persisting queue and worker group lists across restarts.

### Recovery on startup

1. **Try snapshots first**: load all rows from `aggregate_snapshots`, deserialize each into a `TaskAggregate`, populate `store.tasks`. This is near-instant (~100ms for 120 tasks).
2. **Gap replay**: replay invocations from the `invocations` table where `finished_at > min(snapshot_ts)`. This covers the short window between the last snapshot and the shutdown/crash. Typically seconds of data.
3. **Fallback**: if no snapshots exist (first deploy, or table was cleared), fall back to full row-replay from `SQLITE_RECOVERY_HOURS` of invocation history.

### SSE protocol

The `/api/stream` endpoint emits three event types:

- **`task_update`** — changed task summaries + cluster stats. Sent every ~300ms when there's activity.
- **`invocation_update`** — IDs of newly completed invocations (last 20).
- **`sparkline_update`** — per-minute throughput data points, sent every 60s.

### API endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness/readiness probe |
| GET | `/api/meta` | Cluster topology (queues, worker groups, pickup latency) |
| GET | `/api/stats` | Runtime stats (events/sec, uptime, retention, broker status) |
| GET | `/api/tasks` | All tracked tasks with summaries |
| GET | `/api/tasks/{name}/summary` | Single task summary |
| GET | `/api/tasks/{name}/latency` | Time-series latency/throughput data |
| GET | `/api/tasks/{name}/invocations` | Recent invocations for a task |
| GET | `/api/invocations/{task_id}` | Single invocation detail |
| GET | `/api/search/invocations` | Search with filters (task_name, status, worker, queue, free-text) |
| GET | `/api/stream` | SSE event stream |

### Redis pub/sub at scale

Phlower subscribes to task events only (task-received, task-started, task-succeeded, task-failed, task-retried), excluding worker heartbeats. This avoids the pub/sub output buffer overflow that can occur when many workers burst heartbeats simultaneously and exceed Redis/ElastiCache's `client-output-buffer-limit-pubsub`. Reconnects use exponential backoff (2s -> 60s cap).

Celery workers send task events only after `-E`, `worker_send_task_events` or an `enable_events` control command, and a restarted worker starts with events off. Without one of these, the broker connection and `celery inspect` keep working but no task data arrives. Phlower therefore broadcasts `enable_events` every 5 seconds (`ENABLE_WORKER_EVENTS`). The broadcast and the minutely `celery inspect` run on a separate control thread, because `capture()` does not return while events keep arriving and would starve any timer on the event loop.

## Stack

**Backend:** Python 3.14, FastAPI, uvicorn, Celery event API, SSE via sse-starlette, fastdigest (TDigest)

**Frontend:** React 19, TypeScript, Vite, TanStack Query, Chart.js, react-router

**Packaging:** uv (Python), pnpm (frontend), Docker multi-tool build
