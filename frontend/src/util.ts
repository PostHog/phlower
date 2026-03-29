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

export function fmtTsFull(val: number | null): string {
  if (val == null) return "\u2014";
  const d = new Date(val * 1000);
  return d.toISOString().replace("T", " ").slice(0, 19);
}
