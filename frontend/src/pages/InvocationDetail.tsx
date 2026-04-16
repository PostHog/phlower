import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import type { InvocationRecord } from "../api/client";
import { invocationDetailOptions } from "../api/generated/@tanstack/react-query.gen";
import { Badge } from "../components/Badge";
import { fmtMs, fmtTsFull } from "../util";

/** A record is "partial" if it has a terminal state but no detail fields — thinned by SQLite. */
function isPartial(inv: InvocationRecord): boolean {
  const terminal = ["SUCCESS", "FAILURE", "RETRY"].includes(inv.state);
  return terminal && !inv.args_preview && !inv.kwargs_preview && !inv.traceback_snippet && inv.transitions.length === 0;
}

export function InvocationDetail() {
  const { taskId } = useParams<{ taskId: string }>();
  const id = taskId!;

  const { data: inv } = useQuery({
    ...invocationDetailOptions({ path: { task_id: id } }),
  });

  if (!inv) {
    return (
      <>
        <div className="page-header">
          <Link to="/" className="back">&larr; Tasks</Link>
          <h1>Invocation not found</h1>
        </div>
        <p>
          No record for task id <code>{id}</code>. It may have been evicted or
          never observed.
        </p>
      </>
    );
  }

  // Build a map of transition states for badge display
  const transitionMap = new Map(inv.transitions.map((t) => [t.state, t.ts]));

  return (
    <>
      <div className="page-header">
        <Link
          to={`/tasks/${encodeURIComponent(inv.task_name)}`}
          className="back"
        >
          &larr; {inv.task_name}
        </Link>
        <h1 className="mono">{inv.task_id}</h1>
        <Badge state={inv.state} />
        {isPartial(inv) && <span className="badge partial-badge">partial</span>}
      </div>

      <div className="detail-grid">
        {/* Lifecycle — merged with state transitions */}
        <div className="detail-card">
          <h3>Lifecycle</h3>
          <table className="kv">
            <tbody>
              <LifecycleRow label="Received" ts={inv.received_at} state={transitionMap.has("RECEIVED") ? "RECEIVED" : undefined} />
              <LifecycleRow label="Started" ts={inv.started_at} state={transitionMap.has("STARTED") ? "STARTED" : undefined} />
              <LifecycleRow label="Finished" ts={inv.finished_at} state={inv.state !== "RECEIVED" && inv.state !== "STARTED" ? inv.state : undefined} />
              <tr><td>Runtime</td><td>{fmtMs(inv.runtime_ms)}</td></tr>
              <tr><td>Worker group</td><td className="mono">{inv.worker_group || "\u2014"}</td></tr>
              <tr><td>Instance</td><td className="mono">{inv.worker || "\u2014"}</td></tr>
              <tr><td>Queue</td><td className="mono">{inv.queue || "\u2014"}</td></tr>
              {inv.retries > 0 && <tr><td>Retries</td><td>{inv.retries}</td></tr>}
            </tbody>
          </table>
        </div>

        {/* Arguments */}
        {(inv.args_preview || inv.kwargs_preview) && (
          <div className="detail-card">
            <h3>Arguments</h3>
            {inv.args_preview && (
              <div className="code-block">
                <label>args</label>
                <pre>{inv.args_preview}</pre>
              </div>
            )}
            {inv.kwargs_preview && (
              <div className="code-block">
                <label>kwargs</label>
                <pre>{inv.kwargs_preview}</pre>
              </div>
            )}
          </div>
        )}

        {/* Error */}
        {inv.exception_type && (
          <div className="detail-card wide">
            <h3>Error</h3>
            <div className="code-block err">
              <label>{inv.exception_type}</label>
              {inv.exception_message && <pre>{inv.exception_message}</pre>}
              {inv.traceback_snippet && (
                <pre className="traceback">{inv.traceback_snippet}</pre>
              )}
            </div>
          </div>
        )}

        {isPartial(inv) && (
          <div className="detail-card wide partial-note">
            <p>This is a partial record. Args, kwargs, and traceback were removed after {"\u2248"}8 hours to save storage. Metadata (timestamps, state, worker, queue) is retained for up to 7 days.</p>
          </div>
        )}
      </div>
    </>
  );
}

function LifecycleRow({ label, ts, state }: { label: string; ts: number | null; state?: string }) {
  return (
    <tr>
      <td>{label}</td>
      <td>
        {state && <Badge state={state} small />}{" "}
        {fmtTsFull(ts)}
      </td>
    </tr>
  );
}
