# Feature 9 — Time-Range Selector

## Goal

Single segmented control at the top of the Analytics card, driving every
chart in the card. Buttons: `3h · 6h · 12h · 24h · 48h · 7d`. Default
`6h`. Stored in `topo:analytics-range` localStorage.

## UI

Matches the elburg layout (top-right of card). Reuse the existing
`.topo-radar-seg` segmented-button styling — it already has `.active`
state and tight spacing.

```html
<div class="analytics-range-seg" id="analytics-range-seg">
    <span class="analytics-range-label">TIME RANGE</span>
    <button data-range="10800">3h</button>
    <button data-range="21600" class="active">6h</button>
    <button data-range="43200">12h</button>
    <button data-range="86400">24h</button>
    <button data-range="172800">48h</button>
    <button data-range="604800">7d</button>
</div>
```

## Behaviour

- Clicking a button updates `_analyticsRange` (seconds) and persists.
- All registered panels are notified via a single `_analyticsRefresh()`
  call which:
  - cancels any in-flight `analytics` WS query;
  - issues one new query per active panel with `from = now - range`
    and `to = now`;
  - updates the chart on response.

### Auto-refresh

While the Analytics card is open, a `setInterval(_analyticsRefresh,
60_000)` keeps everything within ~1 min of fresh. Cleared on card
close.

### Bucket size selection

`_analyticsBucket(range_s)` returns the bucket size from the table in
`00-timeseries-store.md`. Passed in every WS query as
`{cmd: "analytics", panel, from, to, bucket}`.

## Tests

- Manual:
  - Switching range refreshes every chart.
  - Reload preserves the last range.
  - Auto-refresh kicks in after 60 s without changing zoom / hover.

## Effort

~1 h.

## Dependencies

- None — the selector is pure frontend. Ship before any panel for
  cleanest integration, but it can also be retrofitted after panels
  exist (each panel just needs to consume `_analyticsRange` instead of
  a hard-coded default).
