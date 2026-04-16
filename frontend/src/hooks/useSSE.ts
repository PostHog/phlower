import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import type { TaskSummary } from "../api/client";
import {
  listTasksQueryKey,
  statsQueryKey,
  taskSummaryQueryKey,
} from "../api/generated/@tanstack/react-query.gen";

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
            queryClient.setQueryData<TaskSummary[]>(listTasksQueryKey(), (old) => {
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
                  taskSummaryQueryKey({ path: { task_name: diff.task_name } }),
                  (old: TaskSummary | undefined) => old ? { ...old, ...diff } : undefined,
                );
              }
              return [...updated.values()];
            });
          }

          // Stats — write directly
          if (payload.stats) {
            queryClient.setQueryData(statsQueryKey(), payload.stats);
          }
        } catch {
          // Fallback: invalidate to trigger refetch
          queryClient.invalidateQueries({ queryKey: listTasksQueryKey() });
        }
      });

      es.addEventListener("sparkline_update", (e) => {
        try {
          const { points } = JSON.parse(e.data) as { points: Record<string, number> };
          queryClient.setQueryData<TaskSummary[]>(listTasksQueryKey(), (old) => {
            if (!old) return old;
            return old.map((t) => {
              const count = points[t.task_name];
              if (count === undefined) return t;
              const spark = [...t.sparkline.slice(1), count];
              return { ...t, sparkline: spark };
            });
          });
        } catch { /* ignore */ }
      });

      es.addEventListener("invocation_update", () => {
        // Invalidate invocation queries so active detail pages refetch.
        // Match both manual keys (["tasks", *, "invocations"], ["search", *])
        // and generated keys ({ _id: "taskInvocations" | "searchInvocations" }).
        queryClient.invalidateQueries({
          predicate: (query) => {
            const k0 = query.queryKey[0];
            // Manual keys used by TaskDetail and Search pages
            if (k0 === "tasks" && query.queryKey[2] === "invocations") return true;
            if (k0 === "search") return true;
            // Generated keys (future migration)
            if (typeof k0 === "object" && k0 !== null && "_id" in k0) {
              const id = (k0 as { _id: string })._id;
              return id === "taskInvocations" || id === "searchInvocations";
            }
            return false;
          },
        });
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
