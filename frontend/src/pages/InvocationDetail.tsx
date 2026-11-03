import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import type { InvocationRecord } from "../api/client";
import { invocationDetailOptions } from "../api/generated/@tanstack/react-query.gen";
import { Badge } from "../components/Badge";
import { errorLine, fmtMs, fmtTs, fmtTsFull } from "../util";

export function InvocationDetail() {
  const { taskId } = useParams<{ taskId: string }>();
  const id = taskId!;

  const { data: inv, isPending } = useQuery({
    ...invocationDetailOptions({ path: { task_id: id } }),
  });

  if (!inv) {
    return (
      <div className="main-content">
        <div className="main-scroll">
          <div className="inv-header">
            <Link to="/" className="back-btn">← Tasks</Link>
            <h1>{isPending ? id : "Invocation not found"}</h1>
          </div>
          <div className="empty-state">
            {isPending
              ? "Loading…"
              : <>No record for task id <code>{id}</code>. It may have been evicted or never observed.</>}
          </div>
        </div>
      </div>
    );
  }

  const runtimeMs = inv.runtime_ms ?? 0;
  const terminal = ["SUCCESS", "FAILURE", "RETRY"].includes(inv.state);
  const partial = terminal && !inv.args_preview && !inv.kwargs_preview && !inv.traceback_snippet && inv.transitions.length === 0;
  const error = inv.state === "FAILURE" || inv.state === "RETRY" ? errorLine(inv) : null;

  return (
    <div className="main-content">
      <div className="main-scroll">
        {/* Header */}
        <div className="inv-header">
          <Link
            to={`/tasks/${encodeURIComponent(inv.task_name)}`}
            className="back-btn"
            style={{ textDecoration: "none" }}
          >
            ← {inv.task_name.length > 42 ? inv.task_name.slice(0, 40) + "…" : inv.task_name}
          </Link>
          <span className="slash">/</span>
          <h1>{inv.task_id}</h1>
          <Badge state={inv.state} />
          {partial && <span style={{ fontFamily: "var(--sans)", fontSize: 10, fontWeight: 600, color: "var(--warn)", letterSpacing: "0.04em" }}>PARTIAL</span>}
          <div style={{ flex: 1 }} />
          <span className="inv-header-meta">
            worker={inv.worker?.split("@")[1]?.split("-").slice(-1)[0] ?? inv.worker ?? "—"}
            {" · runtime="}{fmtMs(runtimeMs)}
          </span>
        </div>

        {/* Parent strip */}
        {/* Retry strip */}
        {inv.retries > 0 && (
          <div className="strip">
            <span className="strip-label">Retries</span>
            <span style={{ color: "var(--fg)" }}>{inv.retries} attempt(s)</span>
          </div>
        )}

        {/* Lifecycle */}
        <div className="rail">
          <span className="rail-label">Lifecycle</span>
          <span className="rail-right">
            received → {inv.state === "SUCCESS" ? "finished" : inv.state === "FAILURE" ? "failed" : "pending"}
            {" · "}{fmtMs(runtimeMs)}
          </span>
        </div>
        <div className="lifecycle-band">
          <LifecycleTimeline inv={inv} runtimeMs={runtimeMs} />
        </div>

        {/* Two-column body: metadata | code */}
        <div className="inv-body">
          <div className="inv-metadata">
            <div style={{ padding: "18px 20px 6px", fontFamily: "var(--sans)", fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--fg-muted)" }}>
              Metadata
            </div>
            <KvRow k="Task" v={inv.task_name} />
            <KvRow k="Queue" v={inv.queue ?? "—"} />
            <KvRow k="Worker group" v={inv.worker_group ?? "—"} />
            <KvRow k="Instance" v={inv.worker ?? "—"} />
            <KvRow k="Runtime" v={fmtMs(runtimeMs)} />
            <KvRow k="Received" v={fmtTsFull(inv.received_at)} />
            <KvRow k="Started" v={fmtTsFull(inv.started_at)} />
            <KvRow k="Finished" v={fmtTsFull(inv.finished_at)} color={inv.state === "FAILURE" ? "var(--bad)" : undefined} />

            {/* Transitions */}
            {inv.transitions.length > 0 && (
              <>
                <div style={{ padding: "14px 20px 6px", fontFamily: "var(--sans)", fontSize: 10, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--fg-muted)" }}>
                  Transitions
                </div>
                {inv.transitions.map((t, i) => (
                  <KvRow key={i} k={t.state} v={fmtTsFull(t.ts)} />
                ))}
              </>
            )}
          </div>

          <div className="inv-code">
            {inv.args_preview && (
              <div className="code-block-section">
                <div className="code-block-label">Args</div>
                <pre className="code-block-body">{inv.args_preview}</pre>
              </div>
            )}
            {inv.kwargs_preview && (
              <div className="code-block-section">
                <div className="code-block-label">Kwargs</div>
                <pre className="code-block-body">{inv.kwargs_preview}</pre>
              </div>
            )}
            {error && (
              <div className="code-block-section">
                <div className="code-block-label txt-bad">{inv.state === "RETRY" ? "Retry reason" : "Error"}</div>
                <pre className="code-block-body txt-bad">{error}</pre>
              </div>
            )}
            {inv.traceback_snippet && (
              <div className="code-block-section">
                <div className="code-block-label">Traceback</div>
                <pre className="code-block-body">{inv.traceback_snippet}</pre>
              </div>
            )}
            {!inv.args_preview && !inv.kwargs_preview && !error && (
              <div className="empty-state">
                <p>No args/kwargs/error data available.</p>
                {partial && (
                  <p style={{ marginTop: 4, fontSize: 11 }}>
                    Partial record — args, kwargs, and traceback are not stored for high-rate
                    tasks to save storage. Metadata (timestamps, state, worker, queue) is still kept.
                  </p>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

/** Plain HTML so the 10px labels stay 10px at any width (an SVG viewBox scaled them up). */
function LifecycleTimeline({ inv, runtimeMs }: { inv: InvocationRecord; runtimeMs: number }) {
  const color =
    inv.state === "SUCCESS" ? "var(--success)" :
    inv.state === "FAILURE" ? "var(--bad)" :
    "var(--warn)";
  const endLabel = inv.state === "SUCCESS" ? "finished" : inv.state === "FAILURE" ? "failed" : "pending";

  return (
    <div className="lifecycle">
      <div className="lifecycle-labels">
        <span className="txt-muted">received</span>
        <span>{fmtMs(runtimeMs)}</span>
        <span className="txt-muted">{endLabel}</span>
      </div>
      <div className="lifecycle-bar" style={{ background: color }} />
      <div className="lifecycle-labels">
        <span>{fmtTs(inv.received_at)}</span>
        <span>{fmtTs(inv.finished_at)}</span>
      </div>
    </div>
  );
}

function KvRow({ k, v, color }: { k: string; v: string; color?: string }) {
  return (
    <div className="kv-row">
      <span className="kv-key">{k}</span>
      <span className="kv-val" style={color ? { color } : undefined}>{v}</span>
    </div>
  );
}
