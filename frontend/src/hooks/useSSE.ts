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
        queryClient.invalidateQueries({ queryKey: ["tasks"] });
      });

      es.addEventListener("invocation_update", () => {
        queryClient.invalidateQueries({ queryKey: ["invocations"] });
      });

      es.onerror = () => {
        es.close();
        // Reconnect after 2s (EventSource auto-reconnects, but just in case)
        setTimeout(connect, 2000);
      };
    }

    connect();

    return () => {
      sourceRef.current?.close();
    };
  }, [queryClient]);
}
