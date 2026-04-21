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

Workers must run with `-E` (events enabled).

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
| `RETENTION_HOURS` | `48` | In-memory invocation record retention |
| `AGGREGATE_RETENTION_HOURS` | `168` | Hourly rollup retention (7 days) |
| `MAX_GLOBAL_INVOCATIONS` | `100000` | Total invocation records in memory |
| `MAX_INVOCATIONS_PER_TASK` | `10000` | Per-task invocation cap |
| `TASK_ALLOWLIST_REGEX` | `.*` | Only track matching task names |
| `SSE_THROTTLE_SECONDS` | `0.3` | SSE push interval |
| `SQLITE_PATH` | unset | Path to SQLite DB. Enables persistence when set. |
| `SQLITE_RECOVERY_HOURS` | `48` | Fallback row-replay window (only used when no snapshots exist) |
| `SQLITE_DETAIL_HOURS` | `8` | Hours to keep args/kwargs/traceback in SQLite before thinning |
| `SNAPSHOT_INTERVAL_SECONDS` | `60` | How often to persist aggregate snapshots |

## SQLite persistence

Without `SQLITE_PATH`, Phlower runs entirely in memory. A restart loses all history. This is fine for development and short-lived clusters but not for production.

Setting `SQLITE_PATH` enables a WAL-mode SQLite database that serves two purposes:

**Invocation history** — every completed task invocation (success, failure, retry) is written as an individual row. This powers the search/detail UI and lets you look up historical invocations that have already been evicted from memory. Rows are written in 1.5s batches. Detail fields (args, kwargs, traceback) are thinned after `SQLITE_DETAIL_HOURS` to save space. Rows older than `AGGREGATE_RETENTION_HOURS` are purged entirely.

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

Resource sizing: memory usage depends on event throughput and retention settings. The in-memory store caps at `MAX_GLOBAL_INVOCATIONS` records. With default settings and moderate throughput (~50 events/sec), steady-state RSS is ~300-500 MB. Set memory limits accordingly, with headroom for the initial recovery phase.

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
uv run celery -A scripts.fake_tasks worker -E -l info -c 2
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

**`InvocationRecord`** — per-invocation state (task_id, timestamps, runtime, worker, queue, args/kwargs, exception info). Capped at `MAX_GLOBAL_INVOCATIONS` globally and `MAX_INVOCATIONS_PER_TASK` per task. Oldest records are evicted first.

**`WorkerRegistry`** — three-tier topology (instances -> groups -> queues) with TTL-based eviction. Updated by periodic `celery inspect` and enriched by live task events.

### Eviction and coarsening

The eviction loop runs every 30 minutes:

1. **Coarsen**: per-minute buckets older than 2h are merged into hourly rollups. TDigests are merged via `merge_inplace`, counts are summed. Minute buckets are then deleted.
2. **Evict hourly data**: rollups older than `AGGREGATE_RETENTION_HOURS` are deleted.
3. **Evict invocations**: records older than `RETENTION_HOURS` are removed from memory.
4. **Release memory**: `gc.collect()` + `malloc_trim(0)` (Linux/glibc only).

The eviction interval matters for memory — running too frequently creates allocation churn that fragments glibc arenas. 30 minutes is the current balance.

### SQLite tables

**`invocations`** — one row per completed task invocation. Primary key is `task_id`. Indexed on `finished_at` and `(task_name, finished_at)`. Detail fields (args_preview, kwargs_preview, traceback_snippet) are NULLed after `SQLITE_DETAIL_HOURS` to save space. Rows are deleted after `AGGREGATE_RETENTION_HOURS`.

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

## Stack

**Backend:** Python 3.14, FastAPI, uvicorn, Celery event API, SSE via sse-starlette, fastdigest (TDigest)

**Frontend:** React 19, TypeScript, Vite, TanStack Query, Chart.js, react-router

**Packaging:** uv (Python), pnpm (frontend), Docker multi-tool build
