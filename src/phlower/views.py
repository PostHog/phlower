"""Server-rendered HTMX pages and partials."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from markupsafe import Markup

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

TEMPLATE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))

router = APIRouter()


# ---------------------------------------------------------------------------
# Jinja2 helpers
# ---------------------------------------------------------------------------


def _fmt_ms(val: float | None) -> str:
    if val is None:
        return "—"
    if val < 1:
        return f"{val:.2f} ms"
    if val < 1000:
        return f"{val:.0f} ms"
    return f"{val / 1000:.2f} s"


def _fmt_rate(val: float) -> str:
    return f"{val * 100:.1f}%"


def _fmt_ts(val: float | None) -> str:
    if val is None:
        return "—"
    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(val, tz=timezone.utc)
    return dt.strftime("%H:%M:%S")


def _fmt_ts_full(val: float | None) -> str:
    if val is None:
        return "—"
    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(val, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _state_class(state: str) -> str:
    return {
        "SUCCESS": "st-success",
        "FAILURE": "st-failure",
        "RETRY": "st-retry",
        "STARTED": "st-active",
        "RECEIVED": "st-pending",
        "REVOKED": "st-revoked",
    }.get(state, "")


def _sparkline_svg(values: list[int], width: int = 80, height: int = 20) -> str:
    """Render an inline SVG sparkline from a list of counts."""
    if not values or max(values) == 0:
        return Markup(f'<svg width="{width}" height="{height}" class="sparkline"></svg>')
    peak = max(values)
    n = len(values)
    points = []
    for i, v in enumerate(values):
        x = round(i / max(n - 1, 1) * width, 1)
        y = round(height - (v / peak * (height - 2)) - 1, 1)
        points.append(f"{x},{y}")
    poly = " ".join(points)
    # Fill area under the line
    fill_points = f"0,{height} {poly} {width},{height}"
    return Markup(
        f'<svg width="{width}" height="{height}" class="sparkline" viewBox="0 0 {width} {height}">'
        f'<polyline points="{fill_points}" fill="var(--blue)" fill-opacity="0.08" stroke="none"/>'
        f'<polyline points="{poly}" fill="none" stroke="var(--blue)" stroke-width="1.2" stroke-linejoin="round"/>'
        f'</svg>'
    )


def _setup_globals(t: Jinja2Templates) -> None:
    t.env.globals["fmt_ms"] = _fmt_ms
    t.env.globals["fmt_rate"] = _fmt_rate
    t.env.globals["fmt_ts"] = _fmt_ts
    t.env.globals["fmt_ts_full"] = _fmt_ts_full
    t.env.globals["state_class"] = _state_class
    t.env.globals["urlencode"] = quote
    t.env.globals["sparkline_svg"] = _sparkline_svg


def _render(request: Request, template: str, ctx: dict | None = None):
    return templates.TemplateResponse(request, template, context=ctx or {})


_setup_globals(templates)


# ---------------------------------------------------------------------------
# Full pages
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def task_list_page(request: Request):
    store = request.app.state.store
    summaries = store.get_task_list()
    return _render(request, "task_list.html", {
        "tasks": summaries,
        "queues": store.get_known_queues(),
        "workers": store.get_known_workers(),
    })


@router.get("/tasks/{task_name}", response_class=HTMLResponse)
async def task_detail_page(task_name: str, request: Request):
    store = request.app.state.store
    summary = store.get_task_summary(task_name)
    invocations = store.get_task_invocations(task_name, limit=50)
    latency = store.get_task_latency(task_name) or []
    return _render(
        request,
        "task_detail.html",
        {
            "summary": summary,
            "invocations": invocations,
            "latency_json": latency,
            "task_name": task_name,
        },
    )


@router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    return _render(request, "search.html", {"results": []})


@router.get("/invocations/{task_id}", response_class=HTMLResponse)
async def invocation_detail_page(task_id: str, request: Request):
    rec = request.app.state.store.get_invocation(task_id)
    return _render(request, "invocation_detail.html", {"inv": rec, "task_id": task_id})


# ---------------------------------------------------------------------------
# HTMX partials
# ---------------------------------------------------------------------------


@router.get("/ui/partials/broker-status", response_class=HTMLResponse)
async def partial_broker_status(request: Request):
    consumer = request.app.state.consumer
    return _render(
        request,
        "partials/broker_status.html",
        {"connected": consumer.connected, "error": consumer.last_error},
    )


@router.get("/ui/partials/task-table", response_class=HTMLResponse)
async def partial_task_table(request: Request):
    summaries = request.app.state.store.get_task_list()
    return _render(request, "partials/task_table.html", {"tasks": summaries})


@router.get("/ui/partials/task-summary/{task_name}", response_class=HTMLResponse)
async def partial_task_summary(task_name: str, request: Request):
    summary = request.app.state.store.get_task_summary(task_name)
    return _render(request, "partials/task_summary.html", {"summary": summary})


@router.get("/ui/partials/task-invocations/{task_name}", response_class=HTMLResponse)
async def partial_task_invocations(
    task_name: str, request: Request, limit: int = 50, offset: int = 0
):
    invocations = request.app.state.store.get_task_invocations(
        task_name, limit=limit, offset=offset
    )
    return _render(
        request, "partials/invocation_table.html", {"invocations": invocations}
    )


@router.get("/ui/partials/search-results", response_class=HTMLResponse)
async def partial_search_results(
    request: Request,
    task_name: str | None = None,
    status: str | None = None,
    worker: str | None = None,
    queue: str | None = None,
    task_id: str | None = None,
    q: str | None = None,
    time_from: float | None = None,
    time_to: float | None = None,
    limit: int = 50,
    offset: int = 0,
):
    results = request.app.state.store.search_invocations(
        task_name=task_name,
        status=status,
        worker=worker,
        queue=queue,
        task_id=task_id,
        q=q,
        time_from=time_from,
        time_to=time_to,
        limit=limit,
        offset=offset,
    )
    return _render(request, "partials/search_results.html", {"results": results})
