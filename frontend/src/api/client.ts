/**
 * API client — re-exports generated types and provides fetch helpers.
 *
 * Types are generated from the FastAPI OpenAPI schema via Hey API.
 * Run `pnpm typegen` after changing backend schemas.
 */
export type { TaskSummaryResponse as TaskSummary } from "./generated/types.gen";
export type { InvocationResponse as InvocationRecord } from "./generated/types.gen";
export type { LatencyPoint } from "./generated/types.gen";
export type { MetaResponse as Meta } from "./generated/types.gen";
export type { HealthResponse as HealthStatus } from "./generated/types.gen";
export type { StatsResponse } from "./generated/types.gen";

const BASE = "";

async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

// Re-export generated types under the old names for backward compat
import type { TaskSummaryResponse } from "./generated/types.gen";
import type { InvocationResponse } from "./generated/types.gen";
import type { LatencyPoint } from "./generated/types.gen";
import type { HealthResponse } from "./generated/types.gen";
import type { MetaResponse } from "./generated/types.gen";

// -- endpoints (thin wrappers — migrate to generated query options over time) --

export const api = {
  tasks: () => fetchJSON<TaskSummaryResponse[]>("/api/tasks"),
  taskSummary: (name: string) =>
    fetchJSON<TaskSummaryResponse>(`/api/tasks/${encodeURIComponent(name)}/summary`),
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
    return fetchJSON<InvocationResponse[]>(
      `/api/tasks/${encodeURIComponent(name)}/invocations?${params}`
    );
  },
  invocation: (id: string) => fetchJSON<InvocationResponse>(`/api/invocations/${id}`),
  search: (params: Record<string, string>) => {
    const qs = new URLSearchParams(
      Object.fromEntries(Object.entries(params).filter(([, v]) => v))
    );
    return fetchJSON<InvocationResponse[]>(`/api/search/invocations?${qs}`);
  },
  health: () => fetchJSON<HealthResponse>("/healthz"),
  meta: () => fetchJSON<MetaResponse>("/api/meta"),
};
