# Codex Task Prompt — Lightweight Celery Task Debugger (Flower Alternative)

## Goal

Build a **single-container, lightweight Celery task debugging tool** focused on:

* inspecting **specific task types**
* understanding **recent behavior (last 24h)**
* debugging **individual task executions**

This is **not a cluster monitoring tool**.

It should outperform Flower by focusing on:

* task-centric debugging
* fast lookup of specific runs
* real metrics (latency, failure rates)
* minimal infrastructure (no DB, ephemeral state)

---

## Core Requirements

### Must be:

* **Single Docker container**
* **Python-based (FastAPI)**
* **No persistent database**
* **In-memory only (bounded retention)**
* **Easy local run (`docker run`)**
* **Easy Kubernetes deployment (1 pod)**

### Must NOT be:

* multi-service architecture
* heavy runtime (no multiple containers)
* persistent analytics system
* generic monitoring dashboard

---

## Tech Stack

### Backend

* Python 3.11+
* FastAPI
* Uvicorn

### Real-time updates

* **Server-Sent Events (SSE)** (preferred over WebSockets)

### Frontend

* **HTMX + minimal JS**
* no heavy React unless trivial

### Celery integration

* use **Celery event APIs**
* do NOT parse Redis manually

---

## Architecture Overview

Single process:

```
Celery broker → FastAPI app (event consumer + state) → SSE → HTMX UI
```

Inside the app:

* Celery event consumer (background task)
* in-memory stores:

  * aggregates
  * recent invocations
* HTTP API
* SSE stream
* server-rendered UI

---

## Core Features

### 1. Task List Page

Show all known task names:

* total executions (24h)
* failure rate
* active count
* highlight “problematic” tasks

---

### 2. Task Detail Page

For a selected task:

Show:

* total count
* success / failure / retry counts
* **p50 / p95 / p99 runtime**
* latency over time (1-minute buckets)
* failure rate over time
* worker distribution
* exception distribution
* recent invocations table

Must update **live via SSE**

---

### 3. Invocation Search

Support:

* task name
* time range
* task id
* status
* worker
* free-text (`q`)

Use cases:

* “find send_email around 12:00”
* “find failed invoice for order 123”

---

### 4. Invocation Detail View

Show:

* task id
* timestamps (received, started, finished)
* runtime
* worker
* retries
* state transitions
* exception type
* traceback snippet
* args/kwargs preview (truncated)
* extracted identifiers

---

## Data Model (In-Memory Only)

### A. Aggregates (all tasks)

Per task, per minute:

* count
* success count
* failure count
* retry count
* runtime histogram / buckets

Retention:

* 24h default

---

### B. Invocation Records

Store only:

* failures
* retries
* watched tasks
* sampled successes (optional)

Each record:

```json
{
  "task_id": "...",
  "task_name": "...",
  "state": "...",
  "received_at": "...",
  "started_at": "...",
  "finished_at": "...",
  "runtime_ms": 123,
  "worker": "...",
  "args_preview": "...",
  "kwargs_preview": "...",
  "exception_type": "...",
  "exception_snippet": "...",
  "traceback_snippet": "...",
  "correlation_fields": {}
}
```

Retention:

* 24h (bounded by max size)

---

## Celery Integration

* connect via `CELERY_BROKER_URL`
* consume events using Celery APIs
* track:

  * received
  * started
  * succeeded
  * failed
  * retried

Compute runtime from timestamps.

### Important

Document clearly:

* workers must run with events enabled (`-E`)

---

## API Endpoints

### Health

* `GET /healthz`

### Tasks

* `GET /api/tasks`

### Task detail

* `GET /api/tasks/{task_name}/summary`
* `GET /api/tasks/{task_name}/latency`
* `GET /api/tasks/{task_name}/invocations`

### Search

* `GET /api/search/invocations`

### Invocation

* `GET /api/invocations/{task_id}`

### Live stream

* `GET /api/stream` (SSE)

---

## SSE Behavior

* push updates when:

  * new invocation arrives
  * aggregates change

Event types:

* `task_update`
* `invocation_update`

---

## Config (env vars)

```
CELERY_BROKER_URL=
RETENTION_HOURS=24
MAX_GLOBAL_INVOCATIONS=500000
MAX_INVOCATIONS_PER_TASK=50000
TASK_WATCHLIST=send_email,generate_invoice
TASK_ALLOWLIST_REGEX=.*
MAX_ARGS_PREVIEW_CHARS=500
MAX_KWARGS_PREVIEW_CHARS=1000
```

---

## Docker Requirements

Single Dockerfile:

* lightweight image
* runs FastAPI + event loop
* exposes port 8000

Must support:

```bash
docker run -p 8000:8000 \
  -e CELERY_BROKER_URL=redis://host.docker.internal:6379/0 \
  celery-task-viewer
```

---

## Kubernetes Requirements

Provide:

* Deployment (1 replica)
* Service
* ConfigMap (optional)
* Secret example

No volumes required.

---

## UX Principles

* fast
* minimal
* developer-focused
* task-first navigation
* no dashboards for dashboards’ sake

---

## What Makes This Better Than Flower

Must explicitly deliver:

* task-first UX
* latency percentiles (p50/p95/p99)
* failure rate over time
* fast lookup of specific runs
* correlation-based search
* ephemeral, high-performance model

If these are not implemented, the tool is not acceptable.

---

## Acceptance Criteria

* runs as **one container**
* connects to existing Celery broker
* shows live task data
* supports task-level analysis
* supports invocation search
* supports detailed inspection
* no database required
* restart resets state safely

---

## Instruction to Codex

Build a **working v1**, not scaffolding.

Prioritize:

* simplicity
* correctness of event handling
* bounded memory usage
* fast UI
* minimal dependencies

Avoid overengineering.
