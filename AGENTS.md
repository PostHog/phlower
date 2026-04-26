# Phlower

## Design philosophy

Dense, numeric, data-forward UI. Information-dense internal tool aesthetic — nerdy, printed-ledger feel. No cards, no rounded corners, no shadows, no gradients. Motion is near-zero: the only animation is the heartbeat pulse dot.

Show actual numeric values (0-100 scores, raw metrics), not icons or simplified visual indicators. Optimize for information density and scannability. Monospace numbers, tabular alignment, subtle color heat for magnitude.

## Public repo guidelines

Phlower is open source. Commits, PRs, and code comments must not reference internal infrastructure — no cluster names, pod names, restart counts, specific memory numbers, or deployment details. Describe problems and solutions generically ("large databases", "high-throughput environments") not as deployment incidents.

## Frontend design brief

### Design DNA

- **1px rules everywhere.** Shared gridlines via `border-right + border-bottom` on panes, `border-bottom` on rows. No doubled borders.
- **Zero border-radius** except the filter chip (4px) and scrollbar thumb.
- **Zero shadows.** Flat and printed.
- **Monospace for data, sans for chrome.** Task names, IDs, numbers, timestamps = JetBrains Mono. Nav, headers, labels = Inter.
- **Warm off-white palette.** Background `#F5F3EE`, sidebar `#EFEDE7`, not clinical white.
- **PostHog accent orange `#F54E00`** for selection, active states, bookmarks. Amber `#F5A623` for warnings/retries. Red `#E5484D` for failures. Green `#2FBF71` for success/heartbeat. Blue `#1D4AFF` for sparklines and active counts.

### Layout

Two-pane: **left sidebar** (220px, fixed) + **right main** (flex, full remaining width). Topbar is 42px with wordmark, nav tabs (Tasks / Search), and heartbeat ticker (tasks/s + pulse dot).

**Sidebar** — Queue facets and Worker facets as ledger rows: label + worker count + optional mini sparkline (36×12) + right-aligned task count. Active facet: 2px left border in accent, background tint. Section headers: uppercase 10px Inter 600, letter-spacing 0.08em.

**Task list** — Full-width table, no outer border. Columns: bookmark icon | status dot + task name | 1h sparkline | rate | active | fail/retry | p50 | p95 | p99 | Ovhd | Bneck | FImp. Row height 34px. Sticky header. Sortable columns.

**Task detail** — 12-col CSS grid. Each pane has `border-right + border-bottom` only — outer container has `border-top + border-left`. Row 1: 6 number panes. Row 2: 6 latency panes. Row 3: charts. Row 4: workers + failures. Below the grid: virtualized invocations ledger (TanStack Virtual).

**Search** — Left rail (240px) with state/queue checkboxes. Main area: free-text search bar + results table.

**Invocation detail** — Header with back link, task ID, state badge. Lifecycle timeline (horizontal SVG bar). Two-column body: metadata ledger (left 320px) + code blocks (right).

### Typography

| Use | Font | Size | Weight |
|-----|------|------|--------|
| Page title | Inter | 18px | 600 |
| Detail task name | Mono | 15px | 500 |
| Stat pane values | Mono | 22px | 500 |
| Table cells | Mono | 12.5px | 400 |
| Runs rows | Mono | 10.5px | 400 |
| Section labels | Inter | 10px | 600, uppercase |
| Column headers | Inter | 10.5px | 500 |

### Color tokens (light theme)

```
bg:            #F5F3EE     surface:       #FFFFFF
sidebar:       #EFEDE7     border:        #DAD5CC
borderSubtle:  #E8E4DB     fg:            #1D1B18
fgMuted:       #706B61     fgDim:         #A39D92
accent:        #F54E00     warn:          #F5A623
bad:           #E5484D     heartbeat:     #2FBF71
spark:         #1D4AFF     success:       #2FBF71
dotOk:         #CFCABE     rowHover:      #F2EFE8
rowSelected:   rgba(245, 78, 0, 0.07)
```

### Logo

Wordmark "Phlower" with colored first letters: P = blue `#1D4AFF`, h = red `#F54E00`, l = yellow `#F1A82C`, "ower" = foreground black. Small flower SVG icon (5 circles) beside it.

### Data layer

- TanStack Query for fetching, TanStack Virtual for large lists.
- SSE stream (`/api/stream`) pushes task_update, sparkline_update, invocation_update events.
- SSE merges diffs into query cache — no full refetches.
- Bookmarks persisted in localStorage.
