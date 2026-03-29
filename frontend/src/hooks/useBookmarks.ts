import { useCallback, useSyncExternalStore } from "react";

const STORAGE_KEY = "phlower_bookmarks";

function getSnapshot(): string[] {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEY) || "[]");
  } catch {
    return [];
  }
}

let cached = getSnapshot();

function subscribe(cb: () => void) {
  const handler = (e: StorageEvent) => {
    if (e.key === STORAGE_KEY) {
      cached = getSnapshot();
      cb();
    }
  };
  window.addEventListener("storage", handler);
  return () => window.removeEventListener("storage", handler);
}

function getSnapshotCached() {
  return cached;
}

export function useBookmarks() {
  const bookmarks = useSyncExternalStore(subscribe, getSnapshotCached);

  const toggle = useCallback((taskName: string) => {
    const bms = getSnapshot();
    const idx = bms.indexOf(taskName);
    if (idx === -1) bms.push(taskName);
    else bms.splice(idx, 1);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(bms));
    cached = bms;
    // Force re-render by dispatching storage event manually (same-tab)
    window.dispatchEvent(
      new StorageEvent("storage", { key: STORAGE_KEY })
    );
  }, []);

  const isBookmarked = useCallback(
    (taskName: string) => bookmarks.includes(taskName),
    [bookmarks]
  );

  return { bookmarks, toggle, isBookmarked };
}
