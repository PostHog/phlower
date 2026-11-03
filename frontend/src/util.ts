export function fmtMs(val: number | null): string {
  if (val == null) return "\u2014";
  if (val < 1) return `${val.toFixed(2)} ms`;
  if (val < 1000) return `${Math.round(val)} ms`;
  return `${(val / 1000).toFixed(2)} s`;
}

export function fmtRate(val: number): string {
  return `${(val * 100).toFixed(1)}%`;
}

export function fmtNum(val: number): string {
  if (val >= 1_000_000) return `${(val / 1_000_000).toFixed(1)}M`;
  if (val >= 10_000) return `${(val / 1_000).toFixed(1)}K`;
  if (val >= 1_000) return `${(val / 1_000).toFixed(2)}K`;
  return String(val);
}

export function fmtPerMin(val: number): string {
  if (val === 0) return "\u2014";
  if (val < 0.1) return "<0.1/min";
  if (val < 10) return `${val.toFixed(1)}/min`;
  if (val >= 1000) return `${fmtNum(Math.round(val))}/min`;
  return `${Math.round(val)}/min`;
}

export function fmtTs(val: number | null): string {
  if (val == null) return "\u2014";
  return new Date(val * 1000).toLocaleTimeString("en-GB", { hour12: false });
}

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

/** Local time, same zone as fmtTs and the charts. */
export function fmtTsFull(val: number | null): string {
  if (val == null) return "\u2014";
  const d = new Date(val * 1000);
  const date = `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
  return `${date} ${fmtTs(val)}`;
}

/** "ValueError: x" or "pkg.mod.ValueError: x" names ValueError. */
function namesException(line: string, exceptionType: string | null | undefined): boolean {
  if (!exceptionType) return false;
  const name = line.split(":")[0].trim();
  return name === exceptionType || name.endsWith(`.${exceptionType}`);
}

/**
 * Best available error text for an invocation.
 *
 * The exception message is only kept in memory until the record is flushed
 * to SQLite, so fall back to the traceback's "Type: message" tail.
 */
export function errorLine(inv: {
  exception_type?: string | null;
  exception_message?: string | null;
  traceback_snippet?: string | null;
}): string | null {
  if (inv.exception_message) return inv.exception_message;
  const traceback = inv.traceback_snippet?.trim();
  if (!traceback) return inv.exception_type ?? null;
  const lines = traceback.split("\n");

  // A multiline message continues after the "Type: message" line, so start
  // there instead of showing only the last continuation line.
  const start = lines.findLastIndex((line) => namesException(line, inv.exception_type));
  if (start >= 0) return lines.slice(start).join("\n").trim();
  return lines[lines.length - 1].trim();
}

/** Common prefixes to strip from task names for readability. */
const TASK_PREFIXES = ["posthog.tasks.", "posthog."];

export function shortTaskName(name: string): string {
  for (const prefix of TASK_PREFIXES) {
    if (name.startsWith(prefix) && name.length > prefix.length) {
      return name.slice(prefix.length);
    }
  }
  return name;
}
