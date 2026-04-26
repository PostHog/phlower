import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";
import { api, type InvocationRecord } from "../api/client";
import { metaOptions } from "../api/generated/@tanstack/react-query.gen";
import { StateLabel } from "../components/Badge";
import { fmtMs, fmtTs, shortTaskName } from "../util";

export function Search() {
  const navigate = useNavigate();
  const { data: meta } = useQuery({ ...metaOptions(), refetchInterval: 30000 });

  const [q, setQ] = useState("");
  const [states, setStates] = useState<Record<string, boolean>>({ SUCCESS: true, FAILURE: true, RETRY: true });
  const [queueFilter, setQueueFilter] = useState<Record<string, boolean>>({});
  const [submitted, setSubmitted] = useState<Record<string, string>>({});

  const { data: results, isFetching } = useQuery({
    queryKey: ["search", submitted],
    queryFn: () => api.search(submitted),
    enabled: true,
  });

  // Build search params from facets
  function doSearch() {
    const params: Record<string, string> = {};
    if (q) params.q = q;

    const activeStates = Object.entries(states).filter(([, v]) => v).map(([k]) => k);
    if (activeStates.length > 0 && activeStates.length < 3) {
      params.status = activeStates[0];
    }

    const activeQueues = Object.entries(queueFilter).filter(([, v]) => v).map(([k]) => k);
    if (activeQueues.length === 1) params.queue = activeQueues[0];

    setSubmitted(params);
  }

  // Auto-search on facet change
  const filtered = useMemo(() => {
    if (!results) return [];
    return results.filter((inv) => {
      if (!states[inv.state]) return false;
      const anyQueue = Object.values(queueFilter).some(Boolean);
      if (anyQueue && inv.queue && !queueFilter[inv.queue]) return false;
      return true;
    });
  }, [results, states, queueFilter]);

  const queues = meta?.queues || [];

  // Count states in results
  const sCounts = useMemo(() => {
    const c: Record<string, number> = { SUCCESS: 0, FAILURE: 0, RETRY: 0 };
    for (const inv of results || []) c[inv.state] = (c[inv.state] || 0) + 1;
    return c;
  }, [results]);

  return (
    <>
      {/* Left facet rail */}
      <div className="search-rail">
        <div className="section-label">State</div>
        <FacetCheck label="SUCCESS" on={states.SUCCESS} count={sCounts.SUCCESS} dot="var(--success)" onChange={() => setStates({ ...states, SUCCESS: !states.SUCCESS })} />
        <FacetCheck label="FAILURE" on={states.FAILURE} count={sCounts.FAILURE} dot="var(--bad)" onChange={() => setStates({ ...states, FAILURE: !states.FAILURE })} />
        <FacetCheck label="RETRY" on={states.RETRY} count={sCounts.RETRY} dot="var(--warn)" onChange={() => setStates({ ...states, RETRY: !states.RETRY })} />

        <div className="section-label">Queue</div>
        {queues.slice(0, 12).map((qName) => (
          <FacetCheck
            key={qName}
            label={qName}
            on={!!queueFilter[qName]}
            onChange={() => setQueueFilter({ ...queueFilter, [qName]: !queueFilter[qName] })}
          />
        ))}
      </div>

      {/* Main results */}
      <div className="main-content">
        <div className="search-bar">
          <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="var(--fg-muted)" strokeWidth="1.2">
            <circle cx="5" cy="5" r="3.5" /><line x1="8" y1="8" x2="11" y2="11" />
          </svg>
          <input
            className="search-input"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") doSearch(); }}
            placeholder="Free-text: args, kwargs, error text, id…"
          />
          {q && <button className="search-clear" onClick={() => { setQ(""); }}>×</button>}
          <button
            onClick={doSearch}
            style={{ appearance: "none", background: "transparent", border: "1px solid var(--border)", color: "var(--fg-muted)", cursor: "pointer", fontFamily: "var(--sans)", fontSize: 11, padding: "3px 12px" }}
          >
            {isFetching ? "…" : "Search"}
          </button>
        </div>

        <div className="search-stats">
          <span><strong>{filtered.length}</strong> results</span>
        </div>

        <div className="main-scroll">
          <div className="search-results-header">
            <div style={{ width: 74 }}>Received</div>
            <div style={{ width: 74 }}>State</div>
            <div style={{ flex: "2 1 0" }}>Task</div>
            <div style={{ flex: "1.6 1 0" }}>Args</div>
            <div style={{ width: 100 }}>Queue</div>
            <div style={{ width: 70, textAlign: "right" }}>Runtime</div>
            <div style={{ width: 120, textAlign: "right" }}>Task ID</div>
          </div>
          {filtered.length > 0 ? (
            filtered.map((inv) => (
              <SearchResultRow
                key={inv.task_id}
                inv={inv}
                onClick={() => navigate(`/invocations/${inv.task_id}`)}
              />
            ))
          ) : results ? (
            <div className="empty-state">No results. Try adjusting your filters.</div>
          ) : (
            <div className="empty-state">Enter a search to find invocations.</div>
          )}
        </div>
      </div>
    </>
  );
}

function SearchResultRow({ inv, onClick }: { inv: InvocationRecord; onClick: () => void }) {
  return (
    <div className="search-result-row" onClick={onClick}>
      <div style={{ width: 74, color: "var(--fg-muted)", fontVariantNumeric: "tabular-nums" }}>
        {fmtTs(inv.received_at)}
      </div>
      <div style={{ width: 74 }}>
        <StateLabel state={inv.state} />
      </div>
      <div style={{ flex: "2 1 0", minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {shortTaskName(inv.task_name)}
      </div>
      <div style={{ flex: "1.6 1 0", minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", color: "var(--fg-muted)" }}>
        {inv.args_preview ?? ""}
      </div>
      <div style={{ width: 100, color: "var(--fg-muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {inv.queue ?? ""}
      </div>
      <div style={{ width: 70, textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
        {fmtMs(inv.runtime_ms)}
      </div>
      <div style={{ width: 120, textAlign: "right", color: "var(--fg-muted)", fontSize: 10, overflow: "hidden", textOverflow: "ellipsis" }}>
        {inv.task_id.slice(0, 18)}…
      </div>
    </div>
  );
}

function FacetCheck({
  label,
  on,
  count,
  dot,
  onChange,
}: {
  label: string;
  on: boolean;
  count?: number;
  dot?: string;
  onChange: () => void;
}) {
  return (
    <label className="facet-check">
      <input type="checkbox" checked={on} onChange={onChange} />
      {dot && <span className="facet-dot" style={{ background: dot }} />}
      <span className="facet-check-label">{label}</span>
      {count !== undefined && <span className="facet-check-count">{count}</span>}
    </label>
  );
}
