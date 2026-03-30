import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";

/**
 * Connects to the SSE stream and updates TanStack Query caches
 * directly with pushed data. No HTTP polling needed.
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

          // Write task list directly into cache — no refetch
          if (payload.tasks) {
            queryClient.setQueryData(["tasks"], payload.tasks);
          }

          // Write stats directly into cache — no polling
          if (payload.stats) {
            queryClient.setQueryData(["stats"], payload.stats);
          }
        } catch {
          // Fallback: invalidate so it refetches
          queryClient.invalidateQueries({ queryKey: ["tasks"], exact: true });
        }
      });

      es.addEventListener("invocation_update", () => {
        // Invalidate invocation-related queries to trigger refetch
        queryClient.invalidateQueries({
          predicate: (query) => {
            const key = query.queryKey;
            return (
              (key[0] === "tasks" &&
                (key[2] === "invocations" ||
                  key[2] === "summary" ||
                  key[2] === "latency")) ||
              key[0] === "search"
            );
          },
        });
      });

      es.onerror = () => {
        es.close();
        setTimeout(connect, 2000);
      };
    }

    connect();

    return () => {
      sourceRef.current?.close();
    };
  }, [queryClient]);
}
