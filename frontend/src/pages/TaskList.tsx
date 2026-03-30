import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type TaskSummary } from "../api/client";
import { useBookmarks } from "../hooks/useBookmarks";
import { BookmarkButton } from "../components/BookmarkButton";
import { Sparkline } from "../components/Sparkline";
import { fmtMs, fmtRate, fmtPerMin, shortTaskName } from "../util";

export function TaskList() {
  const { data: tasks = [] } = useQuery({
    queryKey: ["tasks"],
    queryFn: api.tasks,
    // No polling — SSE task_update events trigger invalidation
  });

  const { data: meta } = useQuery({
    queryKey: ["meta"],
    queryFn: api.meta,
    refetchInterval: 30000,
  });

  const { isBookmarked } = useBookmarks();
  const [queueFilter, setQueueFilter] = useState("");
  const [groupFilter, setGroupFilter] = useState("");

  // Sort: bookmarked first, then alphabetical
  const sorted = [...tasks].sort((a, b) => {
    const ab = isBookmarked(a.task_name) ? 0 : 1;
    const bb = isBookmarked(b.task_name) ? 0 : 1;
    if (ab !== bb) return ab - bb;
    return a.task_name.localeCompare(b.task_name);
  });

  // Filter by queue or worker group
  const filtered = sorted.filter((t) => {
    if (queueFilter && !t.top_queues.some((q) => q.queue === queueFilter))
      return false;
    if (groupFilter && !t.top_workers.some((w) => w.worker.includes(groupFilter)))
      return false;
    return true;
  });

  // Compute per-queue sparklines for the filter pills
  const queueSparklines = computeQueueSparklines(tasks, meta?.queues || []);

  return (
    <>
      <div className="page-header">
        <h1>Tasks</h1>
        <span className="badge">{tasks.length} tracked</span>
      </div>

      {(meta?.queues?.length || meta?.worker_groups?.length) ? (
        <div className="filter-bar">
          {meta?.queues && meta.queues.length > 0 && (
            <div className="filter-group">
              <span className="filter-label">Queue</span>
              <FilterPill
                label="All"
                active={queueFilter === ""}
                onClick={() => setQueueFilter("")}
              />
              {meta.queues.map((q) => (
                <FilterPill
                  key={q}
                  label={q}
                  active={queueFilter === q}
                  onClick={() => setQueueFilter(q)}
                  sparkline={queueSparklines[q]}
                  waitMs={meta.pickup_latency_p95?.[q]}
                />
              ))}
            </div>
          )}
          {meta?.worker_groups && meta.worker_groups.length > 0 && (
            <div className="filter-group">
              <span className="filter-label">Worker</span>
              <FilterPill
                label="All"
                active={groupFilter === ""}
                onClick={() => setGroupFilter("")}
              />
              {meta.worker_groups.map((g) => (
                <FilterPill
                  key={g}
                  label={g}
                  active={groupFilter === g}
                  onClick={() => setGroupFilter(g)}
                />
              ))}
            </div>
          )}
        </div>
      ) : null}

      {filtered.length > 0 ? (
        <table className="data-table" id="task-table">
          <thead>
            <tr>
              <th className="col-bm" />
              <th>Task</th>
              <th>1 h</th>
              <th className="r">Rate</th>
              <th className="r">Active</th>
              <th className="r">Fail rate</th>
              <th className="r">p50</th>
              <th className="r">p95</th>
              <th className="r">p99</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((t) => (
              <TaskRow key={t.task_name} task={t} />
            ))}
          </tbody>
        </table>
      ) : (
        <div className="empty-state">
          <p>No tasks observed yet.</p>
          <p className="hint">
            Make sure workers run with <code>-E</code> (events enabled) and
            the broker URL is correct.
          </p>
        </div>
      )}
    </>
  );
}

function TaskRow({ task: t }: { task: TaskSummary }) {
  const { isBookmarked } = useBookmarks();
  const bm = isBookmarked(t.task_name);

  const rowClass = [
    t.failure_rate > 0.25
      ? "row-crit"
      : t.failure_rate > 0.1
        ? "row-warn"
        : "",
    bm ? "bm-row" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <tr className={rowClass}>
      <td className="col-bm">
        <BookmarkButton taskName={t.task_name} />
      </td>
      <td>
        <Link to={`/tasks/${encodeURIComponent(t.task_name)}`} className="mono truncate" title={t.task_name}>
          {shortTaskName(t.task_name)}
        </Link>
      </td>
      <td className="col-spark">
        <Sparkline values={t.sparkline} />
      </td>
      <td className="r num">{fmtPerMin(t.rate_per_min)}</td>
      <td className="r num">{t.active_count}</td>
      <td className={`r num ${t.failure_rate > 0.05 ? "txt-fail" : ""}`}>
        {fmtRate(t.failure_rate)}
      </td>
      <td className="r num">{fmtMs(t.p50_ms)}</td>
      <td className="r num">{fmtMs(t.p95_ms)}</td>
      <td className="r num">{fmtMs(t.p99_ms)}</td>
    </tr>
  );
}

function FilterPill({
  label,
  active,
  onClick,
  sparkline,
  waitMs,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
  sparkline?: number[];
  waitMs?: number | null;
}) {
  const waitClass =
    waitMs != null && waitMs > 5000
      ? "pill-wait-red"
      : waitMs != null && waitMs > 1000
        ? "pill-wait-yellow"
        : "";

  return (
    <button
      className={`filter-pill${active ? " active" : ""}${sparkline ? " with-spark" : ""} ${waitClass}`}
      onClick={onClick}
    >
      {sparkline && (
        <Sparkline values={sparkline} width={40} height={14} />
      )}
      <span>{label}</span>
      {waitMs != null && (
        <span className="pill-wait">{fmtMs(waitMs)}</span>
      )}
    </button>
  );
}

/** Aggregate sparklines per queue from task data. */
function computeQueueSparklines(
  tasks: TaskSummary[],
  queues: string[]
): Record<string, number[]> {
  const result: Record<string, number[]> = {};
  for (const q of queues) {
    const matching = tasks.filter((t) =>
      t.top_queues.some((tq) => tq.queue === q)
    );
    if (matching.length === 0) continue;
    const len = matching[0]?.sparkline.length || 60;
    const agg = new Array(len).fill(0);
    for (const t of matching) {
      for (let i = 0; i < Math.min(t.sparkline.length, len); i++) {
        agg[i] += t.sparkline[i];
      }
    }
    result[q] = agg;
  }
  return result;
}
