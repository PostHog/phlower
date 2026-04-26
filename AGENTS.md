# Frontend design brief

Phlower's frontend follows a **Bloomberg-style, information-dense internal tool** aesthetic. The design targets a nerdy, printed-ledger feel — no cards, no rounded corners, no shadows, no gradients. Motion is near-zero: the only animation is the heartbeat pulse dot.

## Design DNA

- **1px rules everywhere.** Shared gridlines via `border-right + border-bottom` on panes, `border-bottom` on rows. No doubled borders.
- **Zero border-radius** except the filter chip (4px) and scrollbar thumb.
- **Zero shadows.** Flat and printed.
- **Monospace for data, sans for chrome.** Task names, IDs, numbers, timestamps = JetBrains Mono. Nav, headers, labels = Inter.
- **Warm off-white palette.** Background `#F5F3EE`, sidebar `#EFEDE7`, not clinical white.
- **PostHog accent orange `#F54E00`** for selection, active states, bookmarks. Amber `#F5A623` for warnings/retries. Red `#E5484D` for failures. Green `#2FBF71` for success/heartbeat. Blue `#1D4AFF` for sparklines and active counts.

## Layout

Two-pane: **left sidebar** (220px, fixed) + **right main** (flex, full remaining width). Topbar is 42px with wordmark, nav tabs (Tasks / Search), and heartbeat ticker (tasks/s + pulse dot).

### Sidebar
- Queue facets and Worker facets as ledger rows: label + optional mini sparkline (36×12) + right-aligned count.
- Active facet: 2px left border in accent, background tint `rgba(245,78,0,0.07)`.
- Section headers: uppercase 10px Inter 600, letter-spacing 0.08em.

### Task list (pure grid)
Full-width table, no outer border. Columns: bookmark icon | status dot + task name | 1h sparkline | rate | active | fail/retry | p50 | p95 | p99. Row height 34px. Sticky header. Hover = `#F2EFE8`. Selected = accent tint + 2px left border.

### Task detail (Bloomberg 12-col grid)
`display: grid; grid-template-columns: repeat(12, 1fr)`. Each pane has `border-right + border-bottom` only — outer container has `border-top + border-left`. Pane headers: uppercase 10px label, optional right-aligned dim meta.

Row 1: 6 number panes (rate, total, success, fail rate, active, retries).
Row 2: 6 latency panes (p50–max).
Row 3: latency chart + throughput chart (span 6 each).
Row 4: workers (bar chart) + failures by class.
Below the grid: recent invocations ledger with search, virtualized via TanStack Virtual.

### Search (S2 faceted rail)
Left rail (240px): state checkboxes (SUCCESS/FAILURE/RETRY with color dots), queue checkboxes, time range. Main area: free-text search bar + results table.

### Invocation detail (I1 timeline-first)
Header with back link, task ID, state badge. Lifecycle timeline (horizontal SVG bar). Two-column body: metadata ledger (left 320px) + code blocks (right, args/kwargs/result/traceback).

## Typography scale

| Use | Font | Size | Weight |
|-----|------|------|--------|
| Page title | Inter | 18px | 600 |
| Detail task name | Mono | 15px | 500 |
| Stat pane values | Mono | 22px | 500 |
| Table cells | Mono | 12.5px | 400 |
| Runs rows | Mono | 10.5px | 400 |
| Section labels | Inter | 10px | 600, uppercase |
| Column headers | Inter | 10.5px | 500 |
| Nav tabs | Inter | 12.5px | 500/600 |

## Color tokens (light theme)

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

## Logo

Wordmark "Phlower" with colored first letters: P = blue `#1D4AFF`, h = red `#F54E00`, l = yellow `#F1A82C`, "ower" = foreground black. Small flower SVG icon (5 circles) beside it.

## Interactions

- No page transitions. Instant. The only motion is the heartbeat dot pulse (opacity 1→0.35→1, 1.6s linear infinite).
- Row hover: instant background color swap.
- Facet click filters table, shows filter chip in header.
- Invocation rows navigate directly to detail on click.
- Charts use Chart.js with 1px strokes, no interior padding or axis titles.

## Data layer

- TanStack Query for fetching, TanStack Virtual for large lists.
- SSE stream (`/api/stream`) pushes task_update, sparkline_update, invocation_update events.
- SSE merges diffs into query cache — no full refetches.
- Bookmarks persisted in localStorage.

## Reference design files

The design was prototyped in HTML/React+Babel. Reference files are in `/tmp/phlower_design_extract/phlower/project/design_handoff_phlower/reference_design/`:
- `phlower-shell.jsx` — tokens, sidebar, topbar
- `phlower-table.jsx` — task table, sparkline, formatters
- `phlower-detail.jsx` — Bloomberg grid detail (V2 is the chosen variant)
- `phlower-detail-primitives.jsx` — charts, data generator
- `phlower-search.jsx` — search variants (S2 is chosen)
- `phlower-invocation.jsx` — invocation variants (I1 is chosen)
