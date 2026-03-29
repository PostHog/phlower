import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { Badge } from "../components/Badge";
import { fmtMs, fmtTsFull } from "../util";

export function InvocationDetail() {
  const { taskId } = useParams<{ taskId: string }>();
  const id = taskId!;

  const { data: inv } = useQuery({
    queryKey: ["invocations", id],
    queryFn: () => api.invocation(id),
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
      </div>

      <div className="detail-grid">
        {/* Lifecycle */}
        <div className="detail-card">
          <h3>Lifecycle</h3>
          <table className="kv">
            <tbody>
              <tr><td>Received</td><td>{fmtTsFull(inv.received_at)}</td></tr>
              <tr><td>Started</td><td>{fmtTsFull(inv.started_at)}</td></tr>
              <tr><td>Finished</td><td>{fmtTsFull(inv.finished_at)}</td></tr>
              <tr><td>Runtime</td><td>{fmtMs(inv.runtime_ms)}</td></tr>
              <tr><td>Worker</td><td className="mono">{inv.worker || "\u2014"}</td></tr>
              <tr><td>Queue</td><td className="mono">{inv.queue || "\u2014"}</td></tr>
              <tr><td>Retries</td><td>{inv.retries}</td></tr>
            </tbody>
          </table>
        </div>

        {/* State transitions */}
        <div className="detail-card">
          <h3>State transitions</h3>
          <table className="kv">
            <tbody>
              {inv.transitions.map((t, i) => (
                <tr key={i}>
                  <td><Badge state={t.state} small /></td>
                  <td>{fmtTsFull(t.ts)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Arguments */}
        {(inv.args_preview || inv.kwargs_preview) && (
          <div className="detail-card wide">
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
      </div>
    </>
  );
}
