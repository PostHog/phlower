import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { TaskSummary } from "../api/client";

/**
 * SSE stream — pushes only changed task summaries + stats.
 * Merges diffs into the TanStack Query cache. Full task list
 * (with sparklines) is fetched once on page load.
 */
export function useSSE() {
  const queryClient = useQueryClient();
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    function connect() {
      const es = new EventSource("/api/stream");
      sourceRef.current = es;

      es.addEventListener("task_update", (e) => {
        try {
          const payload = JSON.parse(e.data);

          // Merge changed summaries into the cached task list
          if (payload.changed?.length) {
            queryClient.setQueryData<TaskSummary[]>(["tasks"], (old) => {
              if (!old) return old;
              const updated = new Map(old.map((t) => [t.task_name, t]));
              for (const diff of payload.changed) {
                const existing = updated.get(diff.task_name);
                if (existing) {
                  updated.set(diff.task_name, { ...existing, ...diff });
                } else {
                  updated.set(diff.task_name, { ...diff, sparkline: [], top_exceptions: [], top_workers: [], top_queues: [] });
                }
                // Also update per-task summary cache (detail page)
                queryClient.setQueryData(
                  ["tasks", diff.task_name, "summary"],
                  (old: TaskSummary | undefined) => old ? { ...old, ...diff } : undefined,
                );
              }
              return [...updated.values()];
            });

            // Latency charts: per-minute data, refreshed via refetchInterval
            // on the detail page (30s). No SSE invalidation needed.
          }

          // Stats — write directly
          if (payload.stats) {
            queryClient.setQueryData(["stats"], payload.stats);
          }
        } catch {
          // Fallback: invalidate to trigger refetch
          queryClient.invalidateQueries({ queryKey: ["tasks"], exact: true });
        }
      });

      es.addEventListener("sparkline_update", (e) => {
        try {
          const { points } = JSON.parse(e.data) as { points: Record<string, number> };
          queryClient.setQueryData<TaskSummary[]>(["tasks"], (old) => {
            if (!old) return old;
            return old.map((t) => {
              const count = points[t.task_name];
              if (count === undefined) return t;
              // Shift sparkline left, append new point
              const spark = [...t.sparkline.slice(1), count];
              return { ...t, sparkline: spark };
            });
          });
        } catch { /* ignore */ }
      });

      es.addEventListener("invocation_update", () => {
        // Invalidate invocation queries so active detail pages refetch
        queryClient.invalidateQueries({
          predicate: (query) =>
            Array.isArray(query.queryKey) &&
            query.queryKey[0] === "tasks" &&
            query.queryKey[2] === "invocations",
        });
        queryClient.invalidateQueries({ queryKey: ["search"] });
      });

      es.onerror = () => {
        es.close();
        setTimeout(connect, 2000);
      };
    }

    connect();
    return () => { sourceRef.current?.close(); };
  }, [queryClient]);
}
