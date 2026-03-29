import { useQuery } from "@tanstack/react-query";

function fmtUptime(sec: number, retentionSec: number): string {
  // Only show uptime if less than retention window
  if (sec >= retentionSec) return "";
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
  const { data } = useQuery({
    queryKey: ["stats"],
    queryFn: () =>
      fetch("/api/stats").then((r) => r.json()) as Promise<{
        events_per_sec: number;
        tasks_tracked: number;
        uptime_sec: number;
        retention_sec: number;
        broker_connected: boolean;
      }>,
    refetchInterval: 1000,
  });

  if (!data) return null;

  const uptime = fmtUptime(data.uptime_sec, data.retention_sec);

  return (
    <div className="nav-status">
      <span className="nav-ticker">
        <span className="ticker-rate">{fmtRate(data.events_per_sec)}</span>
        <span className="ticker-unit"> tasks/s</span>
      </span>
      {uptime && (
        <span className="ticker-uptime">{uptime}</span>
      )}
      <span className={`dot ${data.broker_connected ? "dot-ok" : "dot-err"}`} />
    </div>
  );
}
