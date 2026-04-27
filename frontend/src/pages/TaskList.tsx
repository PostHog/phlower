import { useState, useMemo, useCallback } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import type { TaskSummary } from "../api/client";
import { listTasksOptions, metaOptions } from "../api/generated/@tanstack/react-query.gen";
import { useBookmarks } from "../hooks/useBookmarks";
import { BookmarkButton } from "../components/BookmarkButton";
import { Sparkline } from "../components/Sparkline";
import { StatusDot } from "../components/StatusDot";
import { fmtMs, fmtPerMin, fmtRate, shortTaskName } from "../util";

type SortKey = "task_name" | "sparkline_sum" | "rate_per_min" | "active_count" | "failure_rate" | "p50_ms" | "p95_ms" | "p99_ms" | "ovhd" | "bneck" | "fimp";
type SortDir = "asc" | "desc";

function logNorm(value: number, min: number, max: number): number {
  if (value <= 0 || min <= 0 || max <= min) return 0;
  const logVal = Math.log10(value);
  const logMin = Math.log10(min);
  const logMax = Math.log10(max);
  if (logMax === logMin) return 0;
  return Math.max(0, Math.min(1, (logVal - logMin) / (logMax - logMin)));
}

function ovhdPenalty(p50Ms: number): number {
  return 1 / (1 + (p50Ms / 75) ** 3);
}

function computeScores(tasks: TaskSummary[]) {
  const rates = tasks.map((t) => t.rate_per_min).filter((r) => r > 0);
  const p95s = tasks.map((t) => t.p95_ms ?? 0).filter((v) => v > 0);
  if (rates.length === 0) return new Map();

  const minRate = Math.min(...rates);
  const maxRate = Math.max(...rates);
  const minP95 = Math.min(...p95s);
  const maxP95 = Math.max(...p95s);

  return new Map(
    tasks.map((t) => {
      const rateLog = logNorm(t.rate_per_min, minRate, maxRate);
      const p95Log = logNorm(t.p95_ms ?? 0, minP95, maxP95);
      return [
        t.task_name,
        {
          ovhd: Math.round(rateLog * ovhdPenalty(t.p50_ms ?? 0) * 100),
          bneck: Math.round(rateLog * p95Log * 100),
          fimp: Math.round(t.failure_rate * rateLog * 100),
        },
      ] as const;
    })
  );
}

export function TaskList() {
  const { data: tasks = [] } = useQuery({ ...listTasksOptions() });
  const { data: meta } = useQuery({ ...metaOptions(), refetchInterval: 30000 });
  const { isBookmarked } = useBookmarks();
  const navigate = useNavigate();

  const [queueFilter, setQueueFilter] = useState("__all");
  const [workerFilter, setWorkerFilter] = useState("__all");
  const [sortKey, setSortKey] = useState<SortKey>("task_name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");

  const scores = useMemo(() => computeScores(tasks), [tasks]);

  const queueSparklines = useMemo(
    () => computeQueueSparklines(tasks, meta?.queues || []),
    [tasks, meta?.queues]
  );

  const filtered = useMemo(() => {
    return tasks.filter((t) => {
      if (queueFilter !== "__all" && !t.top_queues.some((q) => q.queue === queueFilter)) return false;
      if (workerFilter !== "__all" && !t.top_workers.some((w) => w.worker.includes(workerFilter))) return false;
      return true;
    });
  }, [tasks, queueFilter, workerFilter]);

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      const aB = isBookmarked(a.task_name) ? 1 : 0;
      const bB = isBookmarked(b.task_name) ? 1 : 0;
      if (aB !== bB) return bB - aB;

      let cmp = 0;
      if (sortKey === "task_name") {
        cmp = a.task_name.localeCompare(b.task_name);
      } else if (sortKey === "sparkline_sum") {
        cmp = a.sparkline.reduce((s, v) => s + v, 0) - b.sparkline.reduce((s, v) => s + v, 0);
      } else if (sortKey === "ovhd" || sortKey === "bneck" || sortKey === "fimp") {
        const aS = scores.get(a.task_name)?.[sortKey] ?? 0;
        const bS = scores.get(b.task_name)?.[sortKey] ?? 0;
        cmp = aS - bS;
      } else {
        cmp = ((a[sortKey] as number) ?? 0) - ((b[sortKey] as number) ?? 0);
      }
      return sortDir === "desc" ? -cmp : cmp;
    });
  }, [filtered, isBookmarked, sortKey, sortDir, scores]);

  const toggleSort = useCallback((key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "task_name" ? "asc" : "desc");
    }
  }, [sortKey]);

  const queues = meta?.queues || [];
  const workerGroups = meta?.worker_groups || [];
  const filterActive = queueFilter !== "__all" || workerFilter !== "__all";

  return (
    <>
      <aside className="sidebar">
        <div className="sidebar-scroll">
          <div className="section-label">
            Queue
            <span className="section-label-right">{queues.length}</span>
          </div>
          <FacetRow
            label="All"
            count={tasks.length}
            active={queueFilter === "__all"}
            onClick={() => setQueueFilter("__all")}
          />
          {queues.map((q) => {
            const count = tasks.filter((t) => t.top_queues.some((tq) => tq.queue === q)).length;
            return (
              <FacetRow
                key={q}
                label={q}
                count={count}
                active={queueFilter === q}
                onClick={() => setQueueFilter(q)}
                sparkline={queueSparklines[q]}
                waitMs={meta?.pickup_latency_p95?.[q]}
                workerCount={meta?.workers_per_queue?.[q]}
              />
            );
          })}

          <div className="section-label">
            Worker
            <span className="section-label-right">{workerGroups.length}</span>
          </div>
          <FacetRow
            label="All"
            count={workerGroups.length}
            active={workerFilter === "__all"}
            onClick={() => setWorkerFilter("__all")}
          />
          {workerGroups.map((g) => (
            <FacetRow
              key={g}
              label={g}
              count={meta?.workers_per_group?.[g] ?? 0}
              active={workerFilter === g}
              onClick={() => setWorkerFilter(g)}
            />
          ))}
        </div>
      </aside>

      <div className="main-content">
        <div className="tasks-header">
          <h1>Tasks</h1>
          <span className="tracked">
            {filtered.length === tasks.length
              ? `${tasks.length} tracked`
              : `${filtered.length} of ${tasks.length}`}
          </span>
          {filterActive && (
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginLeft: 4 }}>
              {queueFilter !== "__all" && (
                <span className="filter-chip">
                  queue = {queueFilter}
                  <button onClick={() => setQueueFilter("__all")}>×</button>
                </span>
              )}
              {workerFilter !== "__all" && (
                <span className="filter-chip">
                  worker = {workerFilter}
                  <button onClick={() => setWorkerFilter("__all")}>×</button>
                </span>
              )}
            </div>
          )}
          <div style={{ flex: 1 }} />
        </div>

        <div className="main-scroll">
          {sorted.length > 0 ? (
            <div className="task-table">
              <div className="task-table-header">
                <div className="col col-bm" />
                <SortHeader className="col col-name" sortKey="task_name" current={sortKey} dir={sortDir} onClick={toggleSort}>Task</SortHeader>
                <SortHeader className="col col-spark" sortKey="sparkline_sum" current={sortKey} dir={sortDir} onClick={toggleSort}>1h</SortHeader>
                <SortHeader className="col col-rate" sortKey="rate_per_min" current={sortKey} dir={sortDir} onClick={toggleSort}>Rate</SortHeader>
                <SortHeader className="col col-active" sortKey="active_count" current={sortKey} dir={sortDir} onClick={toggleSort}>Active</SortHeader>
                <SortHeader className="col col-fail" sortKey="failure_rate" current={sortKey} dir={sortDir} onClick={toggleSort}>Fail / Retry</SortHeader>
                <SortHeader className="col col-p50" sortKey="p50_ms" current={sortKey} dir={sortDir} onClick={toggleSort}>p50</SortHeader>
                <SortHeader className="col col-p95" sortKey="p95_ms" current={sortKey} dir={sortDir} onClick={toggleSort}>p95</SortHeader>
                <SortHeader className="col col-p99" sortKey="p99_ms" current={sortKey} dir={sortDir} onClick={toggleSort}>p99</SortHeader>
                <SortHeader className="col col-score" sortKey="ovhd" current={sortKey} dir={sortDir} onClick={toggleSort} title="High rate + sub-100ms p50 — async overhead, could be a function call">Ovhd</SortHeader>
                <SortHeader className="col col-score" sortKey="bneck" current={sortKey} dir={sortDir} onClick={toggleSort} title="High rate + high p95 — worker time bottleneck">Bneck</SortHeader>
                <SortHeader className="col col-score" sortKey="fimp" current={sortKey} dir={sortDir} onClick={toggleSort} title="High failure rate × high volume — failure impact">FImp</SortHeader>
              </div>
              {sorted.map((t) => (
                <TaskRow
                  key={t.task_name}
                  task={t}
                  scores={scores.get(t.task_name)}
                  onClick={() => navigate(`/tasks/${encodeURIComponent(t.task_name)}`)}
                />
              ))}
            </div>
          ) : (
            <div className="empty-state">
              <p>No tasks match the current filter.</p>
              <p style={{ marginTop: 8 }}>
                Make sure workers run with <code>-E</code> (events enabled) and the broker URL is correct.
              </p>
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function SortHeader({
  className,
  sortKey,
  current,
  dir,
  onClick,
  children,
  title,
}: {
  className: string;
  sortKey: SortKey;
  current: SortKey;
  dir: SortDir;
  onClick: (key: SortKey) => void;
  children: React.ReactNode;
  title?: string;
}) {
  const indicator = current === sortKey ? (dir === "asc" ? " ↑" : " ↓") : "";
  return (
    <div
      className={className}
      style={{ cursor: "pointer" }}
      title={title}
      onClick={() => onClick(sortKey)}
    >
      {children}{indicator}
    </div>
  );
}

function ScoreCell({ value }: { value: number }) {
  if (value === 0) return <span className="score-cell score-zero">—</span>;
  const cls = value >= 70 ? "score-high" : value >= 30 ? "score-mid" : "score-low";
  return <span className={`score-cell ${cls}`}>{value}</span>;
}

function TaskRow({
  task: t,
  scores,
  onClick,
}: {
  task: TaskSummary;
  scores?: { ovhd: number; bneck: number; fimp: number };
  onClick: () => void;
}) {
  const failColor =
    t.failure_rate >= 0.01 ? "txt-bad" :
    t.failure_rate >= 0.003 ? "txt-warn" :
    "txt-muted";

  return (
    <div className="task-row" onClick={onClick}>
      <div className="col col-bm">
        <BookmarkButton taskName={t.task_name} />
      </div>
      <div className="col col-name">
        <StatusDot failRate={t.failure_rate} retryCount={t.retry_count} />
        <span className="task-name" title={t.task_name}>{shortTaskName(t.task_name)}</span>
      </div>
      <div className="col col-spark">
        <Sparkline values={t.sparkline} />
      </div>
      <div className="col col-rate">{fmtPerMin(t.rate_per_min)}</div>
      <div className="col col-active" style={{ color: t.active_count > 0 ? undefined : "var(--fg-dim)" }}>
        {t.active_count}
      </div>
      <div className={`col col-fail ${failColor}`}>
        {fmtRate(t.failure_rate)}
        {t.retry_count > 0 && <span className="txt-warn"> / {t.retry_count}</span>}
      </div>
      <div className="col col-p50">{fmtMs(t.p50_ms)}</div>
      <div className="col col-p95">{fmtMs(t.p95_ms)}</div>
      <div className="col col-p99">{fmtMs(t.p99_ms)}</div>
      <div className="col col-score"><ScoreCell value={scores?.ovhd ?? 0} /></div>
      <div className="col col-score"><ScoreCell value={scores?.bneck ?? 0} /></div>
      <div className="col col-score"><ScoreCell value={scores?.fimp ?? 0} /></div>
    </div>
  );
}

function FacetRow({
  label,
  count,
  active,
  onClick,
  sparkline,
  waitMs,
  workerCount,
}: {
  label: string;
  count: number;
  active: boolean;
  onClick: () => void;
  sparkline?: number[];
  waitMs?: number | null;
  workerCount?: number;
}) {
  const waitColor = waitMs != null && waitMs > 5000 ? "var(--bad)" : waitMs != null && waitMs > 1000 ? "var(--warn)" : "var(--fg-dim)";

  return (
    <div className={`facet-row${active ? " active" : ""}`} onClick={onClick}>
      <span className="facet-label">{label}</span>
      {workerCount != null && workerCount > 0 && (
        <span className="facet-workers" title={`${workerCount} worker${workerCount !== 1 ? "s" : ""}`}>{workerCount}w</span>
      )}
      {waitMs != null && (
        <span className="facet-meta" style={{ color: waitColor }}>{fmtMs(waitMs)}</span>
      )}
      {sparkline && sparkline.length > 0 && (
        <Sparkline
          values={sparkline}
          width={36}
          height={12}
          color={active ? "var(--accent)" : "var(--fg-dim)"}
        />
      )}
      <span className="facet-count">{count}</span>
    </div>
  );
}

function computeQueueSparklines(
  tasks: TaskSummary[],
  queues: string[]
): Record<string, number[]> {
  const result: Record<string, number[]> = {};
  for (const q of queues) {
    const matching = tasks.filter((t) => t.top_queues.some((tq) => tq.queue === q));
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
