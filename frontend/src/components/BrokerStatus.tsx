import { useQuery } from "@tanstack/react-query";
import { statsOptions } from "../api/generated/@tanstack/react-query.gen";

function fmtRate(rate: number): string {
  if (rate < 0.1) return "0";
  if (rate < 10) return rate.toFixed(1);
  return Math.round(rate).toString();
}

function fmtUptime(sec: number): string {
  if (sec < 60) return `${Math.round(sec)}s`;
  if (sec < 3600) return `${Math.round(sec / 60)}m`;
  const h = Math.floor(sec / 3600);
  const m = Math.round((sec % 3600) / 60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

export function BrokerStatus() {
  const { data } = useQuery({
    ...statsOptions(),
    staleTime: Infinity,
  });

  if (!data) return null;

  return (
    <div className="heartbeat">
      <span className="heartbeat-rate">
        <span className="heartbeat-rate-val">{fmtRate(data.events_per_sec)}</span>
        <span> tasks/s</span>
      </span>
      <span className="heartbeat-latency">{fmtUptime(data.uptime_sec)}</span>
      <span className={`heartbeat-dot${data.broker_connected ? "" : " disconnected"}`} />
    </div>
  );
}
