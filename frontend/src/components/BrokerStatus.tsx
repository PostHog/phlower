import { useQuery } from "@tanstack/react-query";
import { statsOptions } from "../api/generated/@tanstack/react-query.gen";

function fmtUptime(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`;
  if (sec < 3600) return `${Math.round(sec / 60)}m`;
  const h = Math.floor(sec / 3600);
  const m = Math.round((sec % 3600) / 60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function fmtRate(rate: number): string {
  if (rate < 0.1) return "0";
  if (rate < 10) return rate.toFixed(1);
  return Math.round(rate).toString();
}

export function BrokerStatus() {
  // Stats are pushed via SSE into this cache key — no polling.
  // Initial fetch on mount, then SSE keeps it updated.
  const { data } = useQuery({
    ...statsOptions(),
    staleTime: Infinity,
  });

  if (!data) return null;

  return (
    <div className="nav-status">
      <span className="nav-ticker">
        <span className="ticker-rate">{fmtRate(data.events_per_sec)}</span>
        <span className="ticker-unit"> tasks/s</span>
      </span>
      <span className="ticker-uptime">{fmtUptime(data.uptime_sec)}</span>
      <span className={`dot ${data.broker_connected ? "dot-ok" : "dot-err"}`} />
    </div>
  );
}
