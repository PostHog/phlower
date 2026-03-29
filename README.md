# Phlower

Task-centric Celery monitoring. Single container, no database, live updates.

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

## What you get

**Task list** — all observed task types with rate (tasks/min), failure rate, p50/p95/p99 latency, sparklines. Filter by queue or worker group. Bookmark tasks you care about.

**Task detail** — metric cards with state-colored accents, latency + throughput charts, exception/queue/worker distribution, recent invocations.

**Search** — find invocations by task name, status, worker, queue, task ID, or free-text across args/kwargs/errors.

**Invocation detail** — full lifecycle (received → started → finished), runtime, worker, queue, args/kwargs preview, exception + traceback.

**Live ticker** — real-time tasks/sec counter and uptime in the nav bar.

Everything updates live via SSE with ~300ms latency. React frontend with TanStack Query for instant cache-driven re-renders.

## Auto-discovery

Phlower periodically runs `celery inspect` to automatically discover:
- **Worker groups** — extracts consumer type from K8s pod hostnames (e.g. `posthog-worker-django-analytics-queries-abc123` → `analytics-queries`)
- **Queue mapping** — which queues each worker consumes
- **Pickup latency** — p95 wait time per queue, shown in filter pills

No configuration needed for any of this.

## Running in Kubernetes

Single-pod deployment. See `k8s/` for manifests. No volumes needed — restart resets state by design.

## Configuration

All via environment variables:

| Variable | Default | What it does |
|----------|---------|-------------|
| `CELERY_BROKER_URL` | `redis://localhost:6379/0` | Broker to connect to |
| `PORT` | `8100` | HTTP port |
| `RETENTION_HOURS` | `24` | How long to keep data |
| `MAX_GLOBAL_INVOCATIONS` | `100000` | Total invocation records cap |
| `MAX_INVOCATIONS_PER_TASK` | `10000` | Per-task invocation cap |
| `SUCCESS_SAMPLE_RATE` | `0.1` | Fraction of successes to store (failures/retries always stored) |
| `TASK_WATCHLIST` | | Comma-separated task names to always store fully |
| `TASK_ALLOWLIST_REGEX` | `.*` | Only track tasks matching this pattern |
| `SSE_THROTTLE_SECONDS` | `0.3` | How often to push SSE updates |

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

## Redis pub/sub at scale

Phlower subscribes only to `task.#` events (via routing key filter), excluding worker heartbeats. This avoids the pub/sub output buffer overflow that can occur when many workers burst heartbeats simultaneously and exceed Redis/ElastiCache's `client-output-buffer-limit-pubsub`. Reconnects use exponential backoff (2s → 60s cap).

## Stack

**Backend:** Python, FastAPI, uvicorn, Celery event API, SSE via sse-starlette

**Frontend:** React 19, TypeScript, Vite, TanStack Query, Chart.js, react-router

**Packaging:** uv (Python), pnpm (frontend), Docker (ghcr.io)
