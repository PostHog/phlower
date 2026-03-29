import { useBookmarks } from "../hooks/useBookmarks";

interface Props {
  taskName: string;
  size?: number;
}

export function BookmarkButton({ taskName, size = 14 }: Props) {
  const { toggle, isBookmarked } = useBookmarks();
  const active = isBookmarked(taskName);

  return (
    <button
      className={`bm-btn${active ? " bm-active" : ""}`}
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
        toggle(taskName);
      }}
      title="Bookmark"
    >
      <svg
        width={size}
        height={size}
        viewBox="0 0 24 24"
        fill={active ? "currentColor" : "none"}
        stroke="currentColor"
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        <path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z" />
      </svg>
    </button>
  );
}
