import { useParams, Link, useNavigate } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
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
import { fmtMs, fmtRate, fmtTs, fmtNum, fmtPerMin } from "../util";

ChartJS.register(
  CategoryScale, LinearScale, PointElement, LineElement,
  BarElement, TimeScale, Tooltip, Legend, Filler
);

export function TaskDetail() {
  const { taskName } = useParams<{ taskName: string }>();
  const name = taskName!;
  const navigate = useNavigate();

  const { data: summary } = useQuery({
    ...taskSummaryOptions({ path: { task_name: name } }),
    refetchInterval: 5000,
  });

  const { data: latency = [] } = useQuery({
    ...taskLatencyOptions({ path: { task_name: name } }),
    refetchInterval: 30000,
  });

  // ── Live invocations (SSE-driven, same logic as before) ──
  const [invocations, setInvocations] = useState<InvocationRecord[]>([]);
  const [hasOlder, setHasOlder] = useState(true);
  const latestTs = useRef<number>(0);

  useEffect(() => {
    api.taskInvocations(name, { limit: 100 }).then((records) => {
      const sorted = [...records].sort((a, b) => (a.updated_at ?? 0) - (b.updated_at ?? 0));
      setInvocations(sorted);
      setHasOlder(records.length >= 100);
      if (sorted.length > 0) latestTs.current = Math.max(...sorted.map((r) => r.updated_at ?? 0));
    });
    return () => { setInvocations([]); latestTs.current = 0; };
  }, [name]);

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

  // ── Failure banner state ──
  const [failureBannerOpen, setFailureBannerOpen] = useState(false);

  if (!summary) return null;

  const labels = latency.map((d) => new Date(d.t * 1000));
  const topFailure = summary.top_exceptions[0];

  // Sort invocations newest-first for the runs ledger display
  const runsNewestFirst = [...invocations].reverse();

  return (
    <div className="main-content" style={{ display: "flex", flexDirection: "column" }}>
      {/* Header — fixed, not scrolling */}
      <div className="detail-header">
          <Link to="/" className="back-btn" style={{ textDecoration: "none" }}>← Tasks</Link>
          <h1>{name}</h1>
          <BookmarkButton taskName={name} size={12} />
          <div style={{ flex: 1 }} />
          <span className="detail-header-meta">
            {summary.top_queues[0] && `queue=${summary.top_queues[0].queue}`}
            {summary.top_workers.length > 0 && ` · ${summary.top_workers.length} workers`}
            {" · 24h"}
          </span>
        </div>

      {/* Scrollable area for failure banner + bloomberg grid */}
      <div className="detail-scroll">
        {/* Failure banner */}
        {topFailure && (
          <>
            <div className="failure-banner" onClick={() => setFailureBannerOpen(!failureBannerOpen)}>
              <span className="failure-banner-label">Last failure</span>
              <span className="failure-banner-msg">{topFailure.type} ({topFailure.count})</span>
              <span className="failure-banner-toggle">{failureBannerOpen ? "−" : "+"}</span>
            </div>
            {failureBannerOpen && (
              <div className="failure-banner-detail">
                <div className="failure-banner-meta">
                  <span>count={topFailure.count}</span>
                </div>
              </div>
            )}
          </>
        )}

        {/* Bloomberg 12-col grid */}
        <div className="bloomberg-grid">
          {/* Row 1: 6 number panes */}
          <NumPane span={2} label="Rate" value={fmtPerMin(summary.rate_per_min)} />
          <NumPane span={2} label="Total 24h" value={fmtNum(summary.total_count)} />
          <NumPane span={2} label="Success" value={fmtNum(summary.success_count)} accent="var(--success)" />
          <NumPane span={2} label="Fail rate" value={fmtRate(summary.failure_rate)} accent={summary.failure_rate > 0 ? "var(--bad)" : undefined} />
          <NumPane span={2} label="Active" value={String(summary.active_count)} accent={summary.active_count > 0 ? "var(--active-blue)" : undefined} />
          <NumPane span={2} label="Retries" value={fmtNum(summary.retry_count)} />

          {/* Row 2: latency panes */}
          <NumPane span={2} label="p50" value={fmtMs(summary.p50_ms)} />
          <NumPane span={2} label="p95" value={fmtMs(summary.p95_ms)} />
          <NumPane span={2} label="p99" value={fmtMs(summary.p99_ms)} />
          <NumPane span={2} label="Mean" value={fmtMs(summary.mean_ms)} />
          <NumPane span={2} label="Min" value={fmtMs(summary.min_ms)} />
          <NumPane span={2} label="Max" value={fmtMs(summary.max_ms)} />

          {/* Row 3: charts */}
          <div className="pane" style={{ gridColumn: "span 6" }}>
            <div className="pane-head">
              <span>Latency</span>
              <span className="pane-head-right">ms · 24h</span>
            </div>
            <div className="chart-pane-body">
              {labels.length > 0 && (
                <Line data={{
                  labels,
                  datasets: [
                    { label: "p50", data: latency.map((d) => d.p50), borderColor: "#F54E00", borderWidth: 1.4, pointRadius: 0, fill: false, tension: 0.3 },
                    { label: "p95", data: latency.map((d) => d.p95), borderColor: "#1D4AFF", borderWidth: 1.2, pointRadius: 0, fill: false, tension: 0.3 },
                    { label: "p99", data: latency.map((d) => d.p99), borderColor: "#F5A623", borderWidth: 1.1, borderDash: [2, 2], pointRadius: 0, fill: false, tension: 0.3 },
                  ],
                }} options={latencyChartOptions} />
              )}
            </div>
          </div>
          <div className="pane" style={{ gridColumn: "span 6" }}>
            <div className="pane-head">
              <span>Throughput</span>
              <span className="pane-head-right">24h</span>
            </div>
            <div className="chart-pane-body">
              {labels.length > 0 && (
                <Bar data={{
                  labels,
                  datasets: [
                    { label: "Success", data: latency.map((d) => d.success), backgroundColor: "#2FBF71b0", borderRadius: 1 },
                    { label: "Failure", data: latency.map((d) => d.failure), backgroundColor: "#E5484Dcc", borderRadius: 1 },
                    { label: "Retry", data: latency.map((d) => d.retry), backgroundColor: "#F5A623b0", borderRadius: 1 },
                  ],
                }} options={throughputChartOptions} />
              )}
            </div>
          </div>

          {/* Row 4: workers + exceptions */}
          {summary.top_workers.length > 0 && (
            <div className="pane" style={{ gridColumn: `span ${summary.top_exceptions.length > 0 ? 5 : 12}` }}>
              <div className="pane-head">
                <span>Workers</span>
                <span className="pane-head-right">{summary.top_workers.length}</span>
              </div>
              <div>
                {summary.top_workers.slice(0, 8).map((w) => {
                  const maxN = Math.max(...summary.top_workers.map((x) => x.count));
                  const pct = Math.max(3, (w.count / maxN) * 100);
                  return (
                    <div key={w.worker} className="worker-bar">
                      <div className="worker-bar-fill" style={{ width: `${pct}%` }} />
                      <span className="worker-bar-label">{w.worker.split("-").slice(-2).join("-")}</span>
                      <span className="worker-bar-count">{w.count}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {summary.top_exceptions.length > 0 && (
            <div className="pane" style={{ gridColumn: `span ${summary.top_workers.length > 0 ? 7 : 12}` }}>
              <div className="pane-head">
                <span>Failures by class</span>
                <span className="pane-head-right">24h</span>
              </div>
              <div>
                {summary.top_exceptions.map((e) => (
                  <div key={e.type} className="failure-group">
                    <div className="failure-group-info">
                      <div className="failure-group-cls">{e.type}</div>
                    </div>
                    <span className="failure-group-count">{e.count}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

        </div>
      </div>

      {/* Recent invocations — fills remaining viewport height */}
      <div className="runs-section">
        <div className="pane-head" style={{ borderTop: "1px solid var(--border-subtle)" }}>
          <span>Recent invocations</span>
          <span className="pane-head-right">{invocations.length} · 24h</span>
        </div>
        <RunsLedger
          runs={runsNewestFirst}
          hasOlder={hasOlder}
          onLoadOlder={loadOlder}
          onOpenRun={(inv) => navigate(`/invocations/${inv.task_id}`)}
        />
      </div>
    </div>
  );
}

function NumPane({ span, label, value, accent }: { span: number; label: string; value: string; accent?: string }) {
  return (
    <div className="pane" style={{ gridColumn: `span ${span}` }}>
      <div className="pane-head"><span>{label}</span></div>
      <div className="num-pane-value" style={accent ? { color: accent } : undefined}>{value}</div>
    </div>
  );
}

function RunsLedger({
  runs,
  hasOlder,
  onLoadOlder,
  onOpenRun,
}: {
  runs: InvocationRecord[];
  hasOlder: boolean;
  onLoadOlder: () => void;
  onOpenRun: (inv: InvocationRecord) => void;
}) {
  const [query, setQuery] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);

  const filtered = query
    ? runs.filter((r) => {
        const hay = `${r.task_id} ${r.args_preview ?? ""} ${r.exception_type ?? ""}`.toLowerCase();
        return hay.includes(query.toLowerCase());
      })
    : runs;

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 26,
    overscan: 20,
  });

  return (
    <div className="runs-ledger-container">
      <div className="runs-toolbar">
        <div className="runs-search-box">
          <svg width="11" height="11" viewBox="0 0 12 12" fill="none" stroke="var(--fg-muted)" strokeWidth="1.2">
            <circle cx="5" cy="5" r="3.5" /><line x1="8" y1="8" x2="11" y2="11" />
          </svg>
          <input
            className="runs-search-input"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Find by id, arg, error…"
          />
          {query && (
            <button
              onClick={() => setQuery("")}
              style={{ appearance: "none", background: "transparent", border: 0, color: "var(--fg-muted)", cursor: "pointer", padding: 0, fontSize: 12 }}
            >×</button>
          )}
        </div>
        <span className="runs-meta">showing {filtered.length} of {runs.length}</span>
        {hasOlder && runs.length > 0 && (
          <button
            onClick={onLoadOlder}
            style={{ appearance: "none", background: "transparent", border: "1px solid var(--border)", color: "var(--fg-muted)", cursor: "pointer", fontFamily: "var(--sans)", fontSize: 10.5, padding: "2px 8px" }}
          >↑ older</button>
        )}
      </div>
      <div className="runs-header">
        <div style={{ flex: "1.4 1 0" }}>Task ID</div>
        <div style={{ width: 68 }}>State</div>
        <div style={{ flex: "2 1 0" }}>Instance</div>
        <div style={{ width: 70, textAlign: "right" }}>Runtime</div>
        <div style={{ width: 80, textAlign: "right" }}>Received ↓</div>
      </div>
      <div ref={scrollRef} className="runs-virtual-scroll">
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {virtualizer.getVirtualItems().map((virtualRow) => {
            const r = filtered[virtualRow.index];
            const stateClass = r.state === "SUCCESS" ? "success" : r.state === "FAILURE" ? "failure" : r.state === "RETRY" ? "retry" : "received";
            return (
              <div
                key={r.task_id}
                style={{ position: "absolute", top: 0, left: 0, width: "100%", transform: `translateY(${virtualRow.start}px)` }}
              >
                <div className="runs-row" onClick={() => onOpenRun(r)}>
                  <div style={{ flex: "1.4 1 0", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.task_id}</div>
                  <div style={{ width: 68 }}>
                    <span className={`state-label ${stateClass}`}>{r.state}</span>
                  </div>
                  <div style={{ flex: "2 1 0", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "var(--fg-muted)" }}>{r.worker ?? "—"}</div>
                  <div style={{ width: 70, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>{fmtMs(r.runtime_ms)}</div>
                  <div style={{ width: 80, textAlign: "right", color: "var(--fg-muted)", fontVariantNumeric: "tabular-nums" }}>{fmtTs(r.received_at)}</div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
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
      labels: { boxWidth: 8, font: { size: 10, family: "Inter" }, usePointStyle: true },
    },
  },
  scales: {
    x: {
      type: "time" as const,
      time: { unit: "minute" as const, displayFormats: { minute: "HH:mm" } },
      grid: { display: false },
      ticks: { font: { size: 9, family: "JetBrains Mono" } },
      border: { display: false },
    },
  },
};

const latencyChartOptions = {
  ...chartBase,
  scales: {
    ...chartBase.scales,
    y: { beginAtZero: true, ticks: { font: { size: 9, family: "JetBrains Mono" } }, grid: { color: "#E8E4DB" }, border: { display: false } },
  },
};

const throughputChartOptions = {
  ...chartBase,
  scales: {
    ...chartBase.scales,
    x: { ...chartBase.scales.x, stacked: true },
    y: { stacked: true, beginAtZero: true, ticks: { font: { size: 9, family: "JetBrains Mono" }, stepSize: 1 }, grid: { color: "#E8E4DB" }, border: { display: false } },
  },
};
