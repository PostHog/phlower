import { useParams, Link } from "react-router-dom";
import { useQuery, useInfiniteQuery } from "@tanstack/react-query";
import { useCallback, useEffect } from "react";
import { type ColumnDef } from "@tanstack/react-table";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  TimeScale,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import "chartjs-adapter-date-fns";
import { Line, Bar } from "react-chartjs-2";
import { api, type InvocationRecord } from "../api/client";
import { BookmarkButton } from "../components/BookmarkButton";
import { Badge } from "../components/Badge";
import { DataTable } from "../components/DataTable";
import { fmtMs, fmtRate, fmtTs, fmtNum, fmtPerMin } from "../util";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  TimeScale,
  Tooltip,
  Legend,
  Filler
);

const invocationColumns: ColumnDef<InvocationRecord, unknown>[] = [
  {
    accessorKey: "task_id",
    header: "Task ID",
    size: 240,
    cell: ({ row }) => (
      <Link to={`/invocations/${row.original.task_id}`} className="mono small truncate-id" title={row.original.task_id}>
        {row.original.task_id}
      </Link>
    ),
  },
  {
    accessorKey: "state",
    header: "State",
    size: 100,
    cell: ({ row }) => <Badge state={row.original.state} small />,
  },
  {
    accessorKey: "worker",
    header: "Worker",
    meta: { className: "mono small" },
    cell: ({ row }) => row.original.worker || "\u2014",
  },
  {
    accessorKey: "runtime_ms",
    header: "Runtime",
    meta: { className: "r num" },
    size: 100,
    cell: ({ row }) => fmtMs(row.original.runtime_ms),
  },
  {
    accessorKey: "received_at",
    header: "Received",
    meta: { className: "small" },
    size: 100,
    cell: ({ row }) => fmtTs(row.original.received_at),
  },
  {
    accessorKey: "exception_type",
    header: "Error",
    meta: { className: "mono small txt-fail" },
    cell: ({ row }) => row.original.exception_type || "",
  },
];

export function TaskDetail() {
  const { taskName } = useParams<{ taskName: string }>();
  const name = taskName!;

  const { data: summary } = useQuery({
    queryKey: ["tasks", name, "summary"],
    queryFn: () => api.taskSummary(name),
    refetchInterval: 5000, // SSE updates this cache too, but fallback poll for safety
  });

  const { data: latency = [] } = useQuery({
    queryKey: ["tasks", name, "latency"],
    queryFn: () => api.taskLatency(name),
    refetchInterval: 30000, // per-minute data, no need to refresh faster
  });

  // Bidirectional infinite query:
  // - fetchNextPage: load older records (scroll down / "Load older" button)
  // - fetchPreviousPage: load newer records (SSE invocation_update signal)
  const {
    data: invPages,
    fetchNextPage,
    fetchPreviousPage,
    hasNextPage,
    isFetchingNextPage,
  } = useInfiniteQuery({
    queryKey: ["tasks", name, "invocations"],
    queryFn: ({ pageParam }) => {
      if (!pageParam) {
        // Initial fetch — no cursor
        return api.taskInvocations(name, { limit: 100 });
      }
      if (pageParam.direction === "older") {
        return api.taskInvocations(name, { limit: 100, before_ts: pageParam.ts });
      }
      // "newer" — prepend
      return api.taskInvocations(name, { limit: 100, after_ts: pageParam.ts });
    },
    initialPageParam: undefined as { direction: "older" | "newer"; ts: number } | undefined,
    getNextPageParam: (lastPage) => {
      if (lastPage.length === 0) return undefined;
      const oldest = lastPage[lastPage.length - 1];
      return oldest.received_at ? { direction: "older" as const, ts: oldest.received_at } : undefined;
    },
    getPreviousPageParam: (firstPage) => {
      if (firstPage.length === 0) return undefined;
      return firstPage[0].received_at ? { direction: "newer" as const, ts: firstPage[0].received_at } : undefined;
    },
    maxPages: 20,
  });

  // Flatten all pages, deduplicate by task_id
  const allInvocations = (invPages?.pages ?? []).flat();
  const seen = new Set<string>();
  const invocations = allInvocations.filter((inv) => {
    if (seen.has(inv.task_id)) return false;
    seen.add(inv.task_id);
    return true;
  });

  // SSE invocation_update triggers fetchPreviousPage to prepend new records
  useEffect(() => {
    const handler = () => {
      if (invocations.length > 0) {
        fetchPreviousPage();
      }
    };
    // Listen for custom event dispatched by SSE hook
    window.addEventListener("phlower:invocation_update", handler);
    return () => window.removeEventListener("phlower:invocation_update", handler);
  }, [fetchPreviousPage, invocations.length]);

  const loadMore = useCallback(() => {
    if (hasNextPage && !isFetchingNextPage) fetchNextPage();
  }, [hasNextPage, isFetchingNextPage, fetchNextPage]);

  if (!summary) return null;

  const labels = latency.map((d) => new Date(d.t * 1000));

  return (
    <>
      <div className="page-header">
        <Link to="/" className="back">&larr; Tasks</Link>
        <h1 className="mono">{name}</h1>
        <BookmarkButton taskName={name} size={18} />
      </div>

      {/* Metrics */}
      <div className="metric-grid">
        <Metric label="Rate" value={fmtPerMin(summary.rate_per_min)} />
        <Metric label="Total (24 h)" value={fmtNum(summary.total_count)} />
        <Metric label="Success" value={fmtNum(summary.success_count)} cls="st-success" />
        <Metric label="Failures" value={fmtNum(summary.failure_count)} cls="st-failure" />
        <Metric label="Retries" value={fmtNum(summary.retry_count)} cls="st-retry" />
        <Metric label="Active" value={summary.active_count} cls="st-active" />
        <Metric
          label="Failure rate"
          value={fmtRate(summary.failure_rate)}
          cls={summary.failure_rate > 0.05 ? "txt-fail" : ""}
        />
        <Metric label="p50" value={fmtMs(summary.p50_ms)} />
        <Metric label="p95" value={fmtMs(summary.p95_ms)} />
        <Metric label="p99" value={fmtMs(summary.p99_ms)} />
      </div>

      {/* Distributions */}
      {summary.top_exceptions.length > 0 && (
        <div className="sub-section">
          <h3>Top exceptions</h3>
          <table className="data-table compact">
            <tbody>
              {summary.top_exceptions.map((e) => (
                <tr key={e.type}>
                  <td className="mono txt-fail">{e.type}</td>
                  <td className="r num">{e.count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {summary.top_queues.length > 0 && (
        <div className="sub-section">
          <h3>Queue distribution</h3>
          <table className="data-table compact">
            <tbody>
              {summary.top_queues.map((q) => (
                <tr key={q.queue}>
                  <td className="mono">{q.queue}</td>
                  <td className="r num">{q.count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {summary.top_workers.length > 0 && (
        <div className="sub-section">
          <h3>Worker distribution</h3>
          <table className="data-table compact">
            <tbody>
              {summary.top_workers.map((w) => (
                <tr key={w.worker}>
                  <td className="mono">{w.worker}</td>
                  <td className="r num">{w.count}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Charts */}
      <div className="chart-row">
        <div className="chart-box">
          <h3>Latency (ms)</h3>
          <Line
            data={{
              labels,
              datasets: [
                {
                  label: "p50",
                  data: latency.map((d) => d.p50),
                  borderColor: "#1d4aff",
                  backgroundColor: "#1d4aff10",
                  borderWidth: 1.5,
                  pointRadius: 0,
                  fill: true,
                  tension: 0.3,
                },
                {
                  label: "p95",
                  data: latency.map((d) => d.p95),
                  borderColor: "#7B61FF",
                  borderWidth: 1.5,
                  borderDash: [4, 2],
                  pointRadius: 0,
                  fill: false,
                  tension: 0.3,
                },
                {
                  label: "p99",
                  data: latency.map((d) => d.p99),
                  borderColor: "#f54e00",
                  borderWidth: 1.5,
                  borderDash: [2, 2],
                  pointRadius: 0,
                  fill: false,
                  tension: 0.3,
                },
              ],
            }}
            options={latencyOptions}
          />
        </div>
        <div className="chart-box">
          <h3>Throughput & failures</h3>
          <Bar
            data={{
              labels,
              datasets: [
                {
                  label: "Success",
                  data: latency.map((d) => d.success),
                  backgroundColor: "#77B96Cb0",
                  borderRadius: 1,
                },
                {
                  label: "Failure",
                  data: latency.map((d) => d.failure),
                  backgroundColor: "#f54e00cc",
                  borderRadius: 1,
                },
                {
                  label: "Retry",
                  data: latency.map((d) => d.retry),
                  backgroundColor: "#F1A82Cb0",
                  borderRadius: 1,
                },
              ],
            }}
            options={throughputOptions}
          />
        </div>
      </div>

      {/* Recent invocations — virtualized */}
      <h2>Recent invocations</h2>
      {invocations.length > 0 ? (
        <>
          <DataTable
            data={invocations}
            columns={invocationColumns}
            virtualize
            estimateSize={38}
            maxHeight={500}
            initialSorting={[{ id: "received_at", desc: true }]}
            getRowClassName={(inv) =>
              inv.received_at != null && Date.now() / 1000 - inv.received_at < 5
                ? "row-new"
                : ""
            }
          />
          {hasNextPage && (
            <button className="load-more" onClick={loadMore} disabled={isFetchingNextPage}>
              {isFetchingNextPage ? "Loading..." : "Load older"}
            </button>
          )}
        </>
      ) : (
        <div className="empty-state"><p>No invocations recorded.</p></div>
      )}
    </>
  );
}

function Metric({
  label,
  value,
  cls = "",
}: {
  label: string;
  value: string | number;
  cls?: string;
}) {
  return (
    <div className="metric-card">
      <span className={`metric-val ${cls}`}>{value}</span>
      <span className="metric-label">{label}</span>
    </div>
  );
}

const chartBase = {
  responsive: true,
  maintainAspectRatio: false,
  animation: { duration: 200 } as const,
  interaction: { mode: "index" as const, intersect: false },
  plugins: {
    legend: {
      position: "bottom" as const,
      labels: { boxWidth: 8, font: { size: 11 }, usePointStyle: true },
    },
  },
  scales: {
    x: {
      type: "time" as const,
      time: { unit: "minute" as const, displayFormats: { minute: "HH:mm" } },
      grid: { display: false },
      ticks: { font: { size: 10 } },
    },
  },
};

const latencyOptions = {
  ...chartBase,
  scales: {
    ...chartBase.scales,
    y: { beginAtZero: true, ticks: { font: { size: 10 } }, grid: { color: "#e5e5e520" } },
  },
};

const throughputOptions = {
  ...chartBase,
  scales: {
    ...chartBase.scales,
    x: { ...chartBase.scales.x, stacked: true },
    y: {
      stacked: true,
      beginAtZero: true,
      ticks: { font: { size: 10 }, stepSize: 1 },
      grid: { color: "#e5e5e520" },
    },
  },
};
