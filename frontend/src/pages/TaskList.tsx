import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type TaskSummary } from "../api/client";
import { useBookmarks } from "../hooks/useBookmarks";
import { BookmarkButton } from "../components/BookmarkButton";
import { Sparkline } from "../components/Sparkline";
import { fmtMs, fmtRate, fmtPerMin } from "../util";

export function TaskList() {
  const { data: tasks = [] } = useQuery({
    queryKey: ["tasks"],
    queryFn: api.tasks,
    refetchInterval: 2000,
  });

  const { isBookmarked } = useBookmarks();
  const [workerFilter, setWorkerFilter] = useState("");

  // Collect known workers from data
  const workers = [
    ...new Set(tasks.flatMap((t) => t.top_workers.map((w) => w.worker))),
  ].sort();

  // Sort: bookmarked first, then alphabetical
  const sorted = [...tasks].sort((a, b) => {
    const ab = isBookmarked(a.task_name) ? 0 : 1;
    const bb = isBookmarked(b.task_name) ? 0 : 1;
    if (ab !== bb) return ab - bb;
    return a.task_name.localeCompare(b.task_name);
  });

  // Filter by worker
  const filtered = workerFilter
    ? sorted.filter((t) => t.top_workers.some((w) => w.worker === workerFilter))
    : sorted;

  return (
    <>
      <div className="page-header">
        <h1>Tasks</h1>
        <span className="badge">{tasks.length} tracked</span>
      </div>

      {workers.length > 0 && (
        <div className="filter-bar">
          <div className="filter-group">
            <span className="filter-label">Worker</span>
            <FilterPill
              label="All"
              active={workerFilter === ""}
              onClick={() => setWorkerFilter("")}
            />
            {workers.map((w) => (
              <FilterPill
                key={w}
                label={w}
                active={workerFilter === w}
                onClick={() => setWorkerFilter(w)}
              />
            ))}
          </div>
        </div>
      )}

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
        <Link to={`/tasks/${encodeURIComponent(t.task_name)}`} className="mono">
          {t.task_name}
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
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      className={`filter-pill${active ? " active" : ""}`}
      onClick={onClick}
    >
      {label}
    </button>
  );
}
