import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type InvocationRecord } from "../api/client";
import { metaOptions } from "../api/generated/@tanstack/react-query.gen";
import { StateLabel } from "../components/Badge";
import { fmtMs, fmtTs, shortTaskName } from "../util";

const ALL_STATES = ["SUCCESS", "FAILURE", "RETRY"];

type SearchRequest = { params: Record<string, string>; statuses: string[] };

/**
 * The API filters by one status and limits results after that. For a partial
 * set of states, ask once per state so newer successes can't push the
 * failures and retries out of a mixed result.
 */
async function searchByStates({ params, statuses }: SearchRequest): Promise<InvocationRecord[]> {
  if (statuses.length === 0) return api.search(params);
  const lists = await Promise.all(statuses.map((status) => api.search({ ...params, status })));
  return lists.flat().sort((a, b) => (b.finished_at ?? 0) - (a.finished_at ?? 0));
}

export function Search() {
  const { data: meta } = useQuery({ ...metaOptions(), refetchInterval: 30000 });

  const [q, setQ] = useState("");
  const [states, setStates] = useState<Record<string, boolean>>({ SUCCESS: true, FAILURE: true, RETRY: true });
  const [queueFilter, setQueueFilter] = useState<Record<string, boolean>>({});
  const [submitted, setSubmitted] = useState<SearchRequest>({ params: {}, statuses: [] });

  const { data: results, isFetching } = useQuery({
    queryKey: ["search", submitted],
    queryFn: () => searchByStates(submitted),
    enabled: true,
  });

  // Build search params from facets
  function doSearch() {
    const params: Record<string, string> = {};
    if (q) params.q = q;

    // All states checked is the same as no status filter.
    const activeStates = Object.entries(states).filter(([, v]) => v).map(([k]) => k);
    const statuses = activeStates.length === ALL_STATES.length ? [] : activeStates;

    const activeQueues = Object.entries(queueFilter).filter(([, v]) => v).map(([k]) => k);
    if (activeQueues.length === 1) params.queue = activeQueues[0];

    setSubmitted({ params, statuses });
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
            aria-label="Search invocations"
          />
          {q && <button className="search-clear" aria-label="Clear search" onClick={() => { setQ(""); }}>×</button>}
          <button
            className="text-btn"
            onClick={doSearch}
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
            <div style={{ flex: "1.6 1 0" }}>Args / kwargs</div>
            <div style={{ width: 100 }}>Queue</div>
            <div style={{ width: 70, textAlign: "right" }}>Runtime</div>
            <div style={{ width: 120, textAlign: "right" }}>Task ID</div>
          </div>
          {filtered.length > 0 ? (
            filtered.map((inv) => (
              <SearchResultRow key={inv.task_id} inv={inv} />
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

/** Tasks called with kwargs only show "()" as args; show whichever carries data. */
function callPreview(inv: InvocationRecord): string {
  const args = inv.args_preview && inv.args_preview !== "()" ? inv.args_preview : null;
  const kwargs = inv.kwargs_preview && inv.kwargs_preview !== "{}" ? inv.kwargs_preview : null;
  return [args, kwargs].filter(Boolean).join(" ");
}

function SearchResultRow({ inv }: { inv: InvocationRecord }) {
  return (
    <Link className="search-result-row" to={`/invocations/${inv.task_id}`}>
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
        {callPreview(inv)}
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
    </Link>
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
