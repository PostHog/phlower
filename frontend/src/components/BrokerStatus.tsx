import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";

export function BrokerStatus() {
  const { data } = useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    refetchInterval: 5000,
  });

  if (!data) return null;

  return (
    <div className="nav-status">
      <span className={`dot ${data.broker_connected ? "dot-ok" : "dot-err"}`} />
      {data.broker_connected ? "connected" : "disconnected"}
    </div>
  );
}
