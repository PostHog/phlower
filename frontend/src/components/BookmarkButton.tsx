import { useBookmarks } from "../hooks/useBookmarks";

interface Props {
  taskName: string;
  size?: number;
}

export function BookmarkButton({ taskName, size = 10 }: Props) {
  const { toggle, isBookmarked } = useBookmarks();
  const active = isBookmarked(taskName);

  return (
    <button
      className={`bm-btn${active ? " active" : ""}`}
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        toggle(taskName);
      }}
      title={active ? "Remove bookmark" : "Bookmark"}
      aria-label={active ? `Remove bookmark for ${taskName}` : `Bookmark ${taskName}`}
      aria-pressed={active}
    >
      <svg width={size} height={Math.round(size * 1.2)} viewBox="0 0 10 12" style={{ display: "block" }} aria-hidden="true">
        <path
          d="M1.5 1.5h7v9L5 8 1.5 10.5v-9z"
          fill={active ? "currentColor" : "none"}
          stroke="currentColor"
          strokeWidth="1"
          strokeLinejoin="round"
          strokeLinecap="round"
        />
      </svg>
    </button>
  );
}
