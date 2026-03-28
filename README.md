# phlower

Task-centric Celery monitoring. Single container, no database, live updates.

Answers questions like:
- Is this task healthy right now?
- Did something regress in the last hours?
- Why did this particular run fail?
- Find the run tied to order #12345

Not a cluster monitoring tool — no worker/queue dashboards, no autoscaling metrics. Just task behavior.

## Quick start

```bash
docker run -p 8100:8100 -e CELERY_BROKER_URL=redis://host.docker.internal:6379/0 phlower
```

Workers must run with `-E` (events enabled).

Open `http://localhost:8100`.

## What you get

**Task list** — all observed task types with throughput, failure rate, p50/p95/p99 latency. Filter by queue or worker. Bookmark tasks you care about.

**Task detail** — metric cards, latency + throughput charts (Chart.js), exception distribution, worker/queue distribution, recent invocations table.

**Search** — find invocations by task name, status, worker, queue, task ID, or free-text across args/kwargs/errors.

**Invocation detail** — full lifecycle (received → started → finished), runtime, worker, queue, args/kwargs preview, exception + traceback.

Everything updates live via SSE. DOM morphing (idiomorph) prevents flicker.

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
| `SSE_THROTTLE_SECONDS` | `1.0` | How often to push SSE updates |

## Development

```bash
uv sync
cd src/phlower/static && pnpm install && cd -
uv run python -m phlower
```

Use `scripts/fake_tasks.py` to generate test traffic:

```bash
uv run celery -A scripts.fake_tasks worker -E -l info -c 2
uv run python scripts/fake_tasks.py
```

## Redis pub/sub at scale

phlower subscribes only to `task.#` events (via routing key filter), excluding worker heartbeats. This prevents the pub/sub output buffer overflow that occurs when many workers (~60+) burst heartbeats simultaneously and exceed Redis/ElastiCache's `client-output-buffer-limit-pubsub`. Reconnects use exponential backoff (2s → 60s cap).
