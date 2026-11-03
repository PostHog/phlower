import { useEffect, useState } from "react";
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
 *
 * Returns whether the stream is currently connected. Diffs sent while the
 * stream was down are lost, so a reconnect refetches everything.
 */
export function useSSE(): boolean {
  const queryClient = useQueryClient();
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    let source: EventSource | null = null;
    let retryTimer: ReturnType<typeof setTimeout> | undefined;
    let hadError = false;

    function connect() {
      const es = new EventSource("/api/stream");
      source = es;

      es.onopen = () => {
        setConnected(true);
        if (hadError) {
          hadError = false;
          queryClient.invalidateQueries();
        }
      };

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
        window.dispatchEvent(new CustomEvent("phlower:invocation_update"));
      });

      es.onerror = () => {
        es.close();
        setConnected(false);
        hadError = true;
        retryTimer = setTimeout(connect, 2000);
      };
    }

    connect();
    return () => {
      clearTimeout(retryTimer);
      source?.close();
    };
  }, [queryClient]);

  return connected;
}
