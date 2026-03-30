import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api, type InvocationRecord } from "../api/client";
import { Badge } from "../components/Badge";
import { fmtMs, fmtTs, shortTaskName } from "../util";

export function Search() {
  const [params, setParams] = useState<Record<string, string>>({});
  const [submitted, setSubmitted] = useState<Record<string, string>>({});

  const { data: results, isFetching } = useQuery({
    queryKey: ["search", submitted],
    queryFn: () => api.search(submitted),
    enabled: Object.values(submitted).some(Boolean),
  });

  function handleSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setSubmitted({ ...params });
  }

  function update(key: string, value: string) {
    setParams((p) => ({ ...p, [key]: value }));
  }

  return (
    <>
      <div className="page-header">
        <h1>Search invocations</h1>
      </div>

      <form className="search-form" onSubmit={handleSubmit}>
        <div className="search-row">
          <input
            name="task_name"
            placeholder="Task name"
            onChange={(e) => update("task_name", e.target.value)}
          />
          <input
            name="task_id"
            placeholder="Task ID"
            onChange={(e) => update("task_id", e.target.value)}
          />
          <select
            name="status"
            onChange={(e) => update("status", e.target.value)}
          >
            <option value="">Any status</option>
            <option value="SUCCESS">Success</option>
            <option value="FAILURE">Failure</option>
            <option value="RETRY">Retry</option>
            <option value="STARTED">Started</option>
          </select>
          <input
            name="worker"
            placeholder="Worker"
            onChange={(e) => update("worker", e.target.value)}
          />
          <input
            name="queue"
            placeholder="Queue"
            onChange={(e) => update("queue", e.target.value)}
          />
        </div>
        <div className="search-row">
          <input
            name="q"
            placeholder="Free-text search (args, kwargs, errors\u2026)"
            className="wide"
            onChange={(e) => update("q", e.target.value)}
          />
          <button type="submit">Search</button>
          {isFetching && <span style={{ color: "var(--ink-3)", fontSize: 12 }}>searching&hellip;</span>}
        </div>
      </form>

      <div>
        {results && results.length > 0 ? (
          <>
            <p className="result-count">
              {results.length} result{results.length !== 1 ? "s" : ""}
            </p>
            <ResultsTable results={results} />
          </>
        ) : results ? (
          <div className="empty-state">
            <p>No results. Try adjusting your filters.</p>
          </div>
        ) : null}
      </div>
    </>
  );
}

function ResultsTable({ results }: { results: InvocationRecord[] }) {
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>Task ID</th>
          <th>Task name</th>
          <th>State</th>
          <th>Worker</th>
          <th className="r">Runtime</th>
          <th>Received</th>
          <th>Error</th>
        </tr>
      </thead>
      <tbody>
        {results.map((inv) => (
          <tr key={inv.task_id}>
            <td>
              <Link to={`/invocations/${inv.task_id}`} className="mono small truncate-id" title={inv.task_id}>
                {inv.task_id}
              </Link>
            </td>
            <td>
              <Link
                to={`/tasks/${encodeURIComponent(inv.task_name)}`}
                className="mono truncate"
                title={inv.task_name}
              >
                {shortTaskName(inv.task_name)}
              </Link>
            </td>
            <td><Badge state={inv.state} small /></td>
            <td className="mono small">{inv.worker || "\u2014"}</td>
            <td className="r num">{fmtMs(inv.runtime_ms)}</td>
            <td className="small">{fmtTs(inv.received_at)}</td>
            <td className="mono small txt-fail">{inv.exception_type || ""}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
