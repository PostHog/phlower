import { useEffect, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";

/**
 * Connects to the SSE stream and invalidates TanStack Query caches
 * when the server pushes updates. Reconnects automatically.
 */
export function useSSE() {
  const queryClient = useQueryClient();
  const sourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    function connect() {
      const es = new EventSource("/api/stream");
      sourceRef.current = es;

      es.addEventListener("task_update", () => {
        // Invalidate task list and summaries — NOT invocations
        // (invocations change too fast and cause visual jumping)
        queryClient.invalidateQueries({ queryKey: ["tasks"], exact: true });
        queryClient.invalidateQueries({ queryKey: ["meta"] });
        queryClient.invalidateQueries({ queryKey: ["stats"] });
      });

      es.addEventListener("invocation_update", () => {
        // Only refresh invocation-related queries on terminal events
        queryClient.invalidateQueries({
          predicate: (query) => {
            const key = query.queryKey;
            return (
              (key[0] === "tasks" && (key[2] === "invocations" || key[2] === "summary" || key[2] === "latency")) ||
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
