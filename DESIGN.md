# Design — qrunnr dashboard

The locked design system for the `/admin` dashboard. Every dashboard page follows it. Change this file first if the system needs to grow; don't override it per page.

Tokens live in [`web/admin/tailwind.css`](web/admin/tailwind.css). The default Tailwind palette is removed there, so a colour class that is not listed below renders nothing.

## Genre and intent

- **Genre:** modern-minimal, operator console.
- **Audience:** on-call and platform engineers checking queue health, often mid-incident.
- **Tone:** technical, utilitarian, an instrument panel. Calm until something is wrong.
- **Principles** (drawn from studies of production dashboards):
  - Lead with the verdict. The overview opens with one sentence that says whether anything is wrong, computed only from live data.
  - Put attention first: queues that need attention sort to the top.
  - Panels are flat and separated by rules. No cards with shadows, no rounded pills.
  - Figures in a row share size, weight, and alignment. All numbers use tabular mono numerals.
  - Empty states are one plain sentence.
  - Bulk actions show the selected count and stay disabled until something is selected.

## Macrostructure

All app pages use **Workbench**:

1. Top bar with the wordmark and tab navigation.
2. A page title and a one-line lede, or the verdict on the overview.
3. Controls.
4. Flat panels: a header row (title plus mono metadata), then a table or spec sheet.

There is no footer and no enrichment imagery: function carries the page.

## Colour (OKLCH)

| Token | Light | Dark | Use |
| --- | --- | --- | --- |
| `paper` | `98.2% 0.005 85` | `17.5% 0.006 70` | Page and panel background |
| `paper-2` | `95.6% 0.007 85` | `20.5% 0.007 70` | Table headers, code, hover |
| `paper-3` | `92.8% 0.008 85` | `24% 0.008 70` | Bar tracks, selected rows |
| `ink` | `22% 0.012 70` | `93% 0.006 85` | Primary text, primary button |
| `ink-2` | `40% 0.012 70` | `78% 0.008 85` | Secondary text |
| `ink-3` | `52% 0.011 70` | `64% 0.008 85` | Labels, metadata |
| `rule` | `89.5% 0.008 85` | `27% 0.008 70` | Panel and row dividers |
| `rule-2` | `80% 0.01 85` | `36% 0.01 70` | Input borders |
| `accent` | `60% 0.19 38` | `70% 0.17 40` | Wordmark only (vermillion) |
| `focus` | `55% 0.17 250` | `72% 0.14 250` | Focus ring |
| `ok` | `50% 0.12 150` | `76% 0.14 150` | done, healthy |
| `warn` | `55% 0.13 65` | `80% 0.14 75` | retry, attention |
| `fail` | `53% 0.19 27` | `71% 0.17 27` | dead, errors, destructive |
| `info` | `50% 0.13 245` | `75% 0.11 245` | pending |
| `run` | `50% 0.16 290` | `75% 0.13 290` | claimed, executing |

Rules:
- The accent is used for the brand mark only. Status colour is semantic and never decorative.
- A status is always a **square mark plus a mono label**, never colour alone and never a pill.
- Row health is a 3px inset bar on the row's first cell.

## Typography

- **Sans:** IBM Plex Sans 400/500/600, for UI text and headings.
- **Mono:** IBM Plex Mono 400/500, for IDs, queue and worker names, numbers, timestamps, and field prefixes.
- The font files are bundled in `web/admin/static/fonts` (SIL OFL), so the dashboard makes no CDN requests at runtime.

Scale:

| Role | Size and style |
| --- | --- |
| Page title and verdict | 22/28 semibold, tracking −0.015em |
| Big figures | mono 28/32 medium, tabular |
| Body and table text | 13px |
| Labels and table headers | 12px |
| Metadata | mono 11px |

Headers are sentence case; there are no uppercase eyebrows.

## Components

- **Buttons:** square (2px radius), 32px tall, 13px medium. They come in four variants: primary (ink fill), secondary (rule border), danger (fail outline), and ghost (row actions). Every button has hover, `:focus-visible`, active (1px press), disabled, and loading (`htmx-request`) states.
- **Fields:** console style. A mono prefix cell (`queue`, `worker`, `after`) labels each input, so there are no floating labels.
- **Segmented control:** the status filter is a set of radio inputs with a visible focus ring, so it works without JavaScript.
- **Tables:** 32px header rows on `paper-2` and 36px body rows. Numbers are right-aligned and tabular. Rows are separated by rules, not zebra stripes.
- **Bars:** 6px tracks on `paper-3`. Queue depth is relative to the deepest queue. Worker freshness fills toward the 20s stale threshold: neutral, then warn, then fail.
- **Flash:** a 2px coloured left edge, a square mark, and the message. No tinted boxes.
- **Live:** a static square plus `live · hh:mm:ss`. There is no pulsing animation.

## Motion

- The only motion is colour transitions at 150ms `--ease-out` (`cubic-bezier(0.16, 1, 0.3, 1)`) and a 1px press on active.
- Nothing animates on load.
- `prefers-reduced-motion` removes all transitions.

## Copy

- Declarative and specific: "2 of 5 queues need attention.", "webhooks: 3 dead letters waiting for review".
- No invented numbers. Every figure comes from the store.
- Times are in UTC, shown as relative values in tables, with the absolute time in the title attribute.
