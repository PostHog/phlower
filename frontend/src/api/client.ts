const BASE = "";

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// -- types ----------------------------------------------------------------

export interface TaskSummary {
  task_name: string;
  total_count: number;
  success_count: number;
  failure_count: number;
  retry_count: number;
  active_count: number;
  failure_rate: number;
  p50_ms: number | null;
  p95_ms: number | null;
  p99_ms: number | null;
  rate_per_min: number;
  top_exceptions: { type: string; count: number }[];
  top_workers: { worker: string; count: number }[];
  top_queues: { queue: string; count: number }[];
  sparkline: number[];
}

export interface LatencyPoint {
  t: number;
  count: number;
  success: number;
  failure: number;
  retry: number;
  p50: number | null;
  p95: number | null;
  p99: number | null;
  failure_rate: number;
}

export interface InvocationRecord {
  task_id: string;
  task_name: string;
  state: string;
  received_at: number | null;
  started_at: number | null;
  finished_at: number | null;
  runtime_ms: number | null;
  worker: string | null;
  queue: string | null;
  args_preview: string | null;
  kwargs_preview: string | null;
  exception_type: string | null;
  exception_message: string | null;
  traceback_snippet: string | null;
  retries: number;
  transitions: { state: string; ts: number }[];
}

export interface HealthStatus {
  status: string;
  broker_connected: boolean;
  broker_error: string | null;
  broker_reconnects: number;
  tasks_tracked: number;
  invocations_stored: number;
  sse_clients: number;
  queues: string[];
}

export interface Meta {
  queues: string[];
  worker_groups: string[];
  workers_seen: number;
  last_inspect_at: number;
  pickup_latency_p95: Record<string, number | null>;
}

// -- endpoints ------------------------------------------------------------

export const api = {
  tasks: () => fetchJSON<TaskSummary[]>("/api/tasks"),
  taskSummary: (name: string) =>
    fetchJSON<TaskSummary>(`/api/tasks/${encodeURIComponent(name)}/summary`),
  taskLatency: (name: string) =>
    fetchJSON<LatencyPoint[]>(`/api/tasks/${encodeURIComponent(name)}/latency`),
  taskInvocations: (
    name: string,
    opts?: { limit?: number; before_ts?: number; after_ts?: number }
  ) => {
    const params = new URLSearchParams();
    params.set("limit", String(opts?.limit ?? 100));
    if (opts?.before_ts) params.set("before_ts", String(opts.before_ts));
    if (opts?.after_ts) params.set("after_ts", String(opts.after_ts));
    return fetchJSON<InvocationRecord[]>(
      `/api/tasks/${encodeURIComponent(name)}/invocations?${params}`
    );
  },
  invocation: (id: string) => fetchJSON<InvocationRecord>(`/api/invocations/${id}`),
  search: (params: Record<string, string>) => {
    const qs = new URLSearchParams(
      Object.fromEntries(Object.entries(params).filter(([, v]) => v))
    );
    return fetchJSON<InvocationRecord[]>(`/api/search/invocations?${qs}`);
  },
  health: () => fetchJSON<HealthStatus>("/healthz"),
  meta: () => fetchJSON<Meta>("/api/meta"),
};
