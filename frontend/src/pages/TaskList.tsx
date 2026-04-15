import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { type ColumnDef } from "@tanstack/react-table";
import type { TaskSummary } from "../api/client";
import { listTasksOptions, metaOptions } from "../api/generated/@tanstack/react-query.gen";
import { useBookmarks } from "../hooks/useBookmarks";
import { BookmarkButton } from "../components/BookmarkButton";
import { Sparkline } from "../components/Sparkline";
import { DataTable } from "../components/DataTable";
import { fmtMs, fmtNum, fmtRate, fmtPerMin, shortTaskName } from "../util";

export function TaskList() {
  const { data: tasks = [] } = useQuery({ ...listTasksOptions() });

  const { data: meta } = useQuery({
    ...metaOptions(),
    refetchInterval: 30000,
  });

  const { isBookmarked } = useBookmarks();
  const [queueFilter, setQueueFilter] = useState("");
  const [groupFilter, setGroupFilter] = useState("");

  // Aggregate sparklines per queue from task data
  const queueSparklines = useMemo(
    () => computeQueueSparklines(tasks, meta?.queues || []),
    [tasks, meta?.queues]
  );

  const filtered = useMemo(() => {
    return tasks.filter((t) => {
      if (queueFilter && !t.top_queues.some((q) => q.queue === queueFilter))
        return false;
      if (groupFilter && !t.top_workers.some((w) => w.worker.includes(groupFilter)))
        return false;
      return true;
    });
  }, [tasks, queueFilter, groupFilter]);

  const columns = useMemo<ColumnDef<TaskSummary, unknown>[]>(
    () => [
      {
        id: "bookmark",
        accessorFn: (row) => (isBookmarked(row.task_name) ? 1 : 0),
        header: "",
        size: 36,
        meta: { className: "col-bm" },
        cell: ({ row }) => <BookmarkButton taskName={row.original.task_name} />,
      },
      {
        accessorKey: "task_name",
        header: "Task",
        cell: ({ row }) => (
          <Link
            to={`/tasks/${encodeURIComponent(row.original.task_name)}`}
            className="mono truncate"
            title={row.original.task_name}
          >
            {shortTaskName(row.original.task_name)}
          </Link>
        ),
      },
      {
        id: "sparkline",
        accessorFn: (row) => row.sparkline.reduce((a, b) => a + b, 0),
        header: "1 h",
        meta: { className: "col-spark" },
        cell: ({ row }) => <Sparkline values={row.original.sparkline} />,
      },
      {
        accessorKey: "rate_per_min",
        header: "Rate",
        meta: { className: "r num" },
        cell: ({ row }) => fmtPerMin(row.original.rate_per_min),
      },
      {
        accessorKey: "active_count",
        header: "Active",
        meta: { className: "r num" },
      },
      {
        accessorKey: "failure_rate",
        header: "Fail / Retry",
        meta: { className: "r num" },
        cell: ({ row }) => {
          const { failure_rate, retry_count } = row.original;
          return (
            <span>
              <span className={failure_rate > 0.05 ? "txt-fail" : ""}>
                {fmtRate(failure_rate)}
              </span>
              {retry_count > 0 && (
                <span className="txt-retry" title={`${retry_count} retries`}>
                  {" "}/ {fmtNum(retry_count)}
                </span>
              )}
            </span>
          );
        },
      },
      {
        accessorKey: "p50_ms",
        header: "p50",
        meta: { className: "r num" },
        cell: ({ row }) => fmtMs(row.original.p50_ms),
      },
      {
        accessorKey: "p95_ms",
        header: "p95",
        meta: { className: "r num" },
        cell: ({ row }) => fmtMs(row.original.p95_ms),
      },
      {
        accessorKey: "p99_ms",
        header: "p99",
        meta: { className: "r num" },
        cell: ({ row }) => fmtMs(row.original.p99_ms),
      },
    ],
    [isBookmarked]
  );

  const getRowClassName = (t: TaskSummary) => {
    const classes: string[] = [];
    if (t.failure_rate > 0.25) classes.push("row-crit");
    else if (t.failure_rate > 0.1) classes.push("row-warn");
    else if (t.retry_count > 0 && t.retry_count / Math.max(t.total_count, 1) > 0.1) classes.push("row-warn");
    if (isBookmarked(t.task_name)) classes.push("bm-row");
    return classes.join(" ");
  };

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
              <FilterPill label="All" active={queueFilter === ""} onClick={() => setQueueFilter("")} />
              {meta.queues.map((q) => (
                <FilterPill
                  key={q}
                  label={q}
                  active={queueFilter === q}
                  onClick={() => setQueueFilter(q)}
                  sparkline={queueSparklines[q]}
                  waitMs={meta.pickup_latency_p95?.[q]}
                  workerCount={meta.workers_per_queue?.[q]}
                />
              ))}
            </div>
          )}
          {meta?.worker_groups && meta.worker_groups.length > 0 && (
            <div className="filter-group">
              <span className="filter-label">Worker</span>
              <FilterPill label="All" active={groupFilter === ""} onClick={() => setGroupFilter("")} />
              {meta.worker_groups.map((g) => (
                <FilterPill key={g} label={g} active={groupFilter === g} onClick={() => setGroupFilter(g)} workerCount={meta.workers_per_group?.[g]} />
              ))}
            </div>
          )}
        </div>
      ) : null}

      {filtered.length > 0 ? (
        <DataTable
          data={filtered}
          columns={columns}
          getRowClassName={getRowClassName}
          initialSorting={[{ id: "bookmark", desc: true }, { id: "task_name", desc: false }]}
        />
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

function FilterPill({
  label,
  active,
  onClick,
  sparkline,
  waitMs,
  workerCount,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
  sparkline?: number[];
  waitMs?: number | null;
  workerCount?: number;
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
      {workerCount != null && workerCount > 0 && (
        <span className="pill-worker-count" title={`${workerCount} worker${workerCount !== 1 ? "s" : ""}`}>{workerCount}</span>
      )}
      {waitMs != null && (
        <span className="pill-wait">{fmtMs(waitMs)}</span>
      )}
    </button>
  );
}

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
