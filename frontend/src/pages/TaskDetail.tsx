import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
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
import { taskSummaryOptions, taskLatencyOptions } from "../api/generated/@tanstack/react-query.gen";
import { BookmarkButton } from "../components/BookmarkButton";
import { Badge } from "../components/Badge";
import { DataTable } from "../components/DataTable";
import { Sparkline } from "../components/Sparkline";
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
    cell: ({ row }) => (
      <>
        <Badge state={row.original.state} small />
        {row.original.retries > 0 && (
          <span className="txt-retry" title={`${row.original.retries} retries`}> ×{row.original.retries}</span>
        )}
      </>
    ),
  },
  {
    accessorKey: "worker_group",
    header: "Group",
    meta: { className: "mono small" },
    cell: ({ row }) => row.original.worker_group || "\u2014",
  },
  {
    accessorKey: "worker",
    header: "Instance",
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
    ...taskSummaryOptions({ path: { task_name: name } }),
    refetchInterval: 5000,
  });

  const { data: latency = [] } = useQuery({
    ...taskLatencyOptions({ path: { task_name: name } }),
    refetchInterval: 30000,
  });

  // Log-style invocations: oldest first, new records append at bottom.
  // Initial load fetches the latest batch, then SSE signals trigger
  // incremental fetches via after_ts to append only new records.
  const [invocations, setInvocations] = useState<InvocationRecord[]>([]);
  const [hasOlder, setHasOlder] = useState(true);
  const [pulse, setPulse] = useState(false);
  const latestTs = useRef<number>(0);

  // Initial load
  useEffect(() => {
    api.taskInvocations(name, { limit: 100 }).then((records) => {
      const sorted = [...records].sort((a, b) => (a.updated_at ?? 0) - (b.updated_at ?? 0));
      setInvocations(sorted);
      setHasOlder(records.length >= 100);
      if (sorted.length > 0) {
        latestTs.current = Math.max(...sorted.map((r) => r.updated_at ?? 0));
      }
    });
    return () => { setInvocations([]); latestTs.current = 0; };
  }, [name]);

  // SSE-triggered incremental fetch — uses updated_at cursor to catch
  // both new records and state transitions (RECEIVED→SUCCESS)
  useEffect(() => {
    const handler = () => {
      if (!latestTs.current && invocations.length === 0) return;
      api.taskInvocations(name, { limit: 500, after_ts: latestTs.current }).then((records) => {
        if (records.length === 0) return;
        setInvocations((prev) => {
          const byId = new Map(prev.map((r) => [r.task_id, r]));
          let changed = false;
          for (const rec of records) {
            const existing = byId.get(rec.task_id);
            if (!existing || existing.state !== rec.state) {
              byId.set(rec.task_id, rec);
              changed = true;
            }
          }
          if (!changed) return prev;
          return [...byId.values()].sort((a, b) => (a.updated_at ?? 0) - (b.updated_at ?? 0));
        });
        const maxTs = Math.max(...records.map((r) => r.updated_at ?? 0));
        if (maxTs > latestTs.current) latestTs.current = maxTs;
        setPulse(true);
        setTimeout(() => setPulse(false), 400);
      });
    };

    window.addEventListener("phlower:invocation_update", handler);
    return () => window.removeEventListener("phlower:invocation_update", handler);
  }, [name, invocations.length]);

  const loadOlder = useCallback(() => {
    if (invocations.length === 0) return;
    const oldestTs = invocations[0].updated_at ?? invocations[0].received_at ?? 0;
    api.taskInvocations(name, { limit: 100, before_ts: oldestTs }).then((records) => {
      if (records.length === 0) { setHasOlder(false); return; }
      const sorted = [...records].sort((a, b) => (a.updated_at ?? 0) - (b.updated_at ?? 0));
      setInvocations((prev) => {
        const seen = new Set(prev.map((r) => r.task_id));
        const fresh = sorted.filter((r) => !seen.has(r.task_id));
        return fresh.length > 0 ? [...fresh, ...prev] : prev;
      });
      setHasOlder(records.length >= 100);
    });
  }, [name, invocations]);

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
        <div className="metric-card">
          <Sparkline values={summary.sparkline} width={120} height={28} />
          <span className="metric-label">1 h</span>
        </div>
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
        <Metric label="Mean" value={fmtMs(summary.mean_ms)} />
        <Metric label="Min" value={fmtMs(summary.min_ms)} />
        <Metric label="Max" value={fmtMs(summary.max_ms)} />
        <Metric label="Std dev" value={fmtMs(summary.std_ms)} />
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

      {/* Live invocations — log style, newest at bottom */}
      <h2>
        Live invocations
        <span className={`pulse-dot${pulse ? " active" : ""}`} />
      </h2>
      {hasOlder && invocations.length > 0 && (
        <button className="load-more load-older" onClick={loadOlder}>
          ↑ Load older
        </button>
      )}
      {invocations.length > 0 ? (
        <DataTable
          data={invocations}
          columns={invocationColumns}
          virtualize
          autoScroll
          estimateSize={38}
          maxHeight={2100}
          getRowClassName={(inv) => {
            const ts = inv.updated_at ?? inv.received_at ?? 0;
            return ts > 0 && Date.now() / 1000 - ts < 5 ? "row-new" : "";
          }}
        />
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
