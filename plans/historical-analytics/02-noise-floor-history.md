# Feature 3 — Noise Floor History

## Goal

Highcharts line chart of noise-floor measurements per node over the
selected time range. Y axis dBm. Same multi-line layout as RSSI/SNR
(#1+#2) but with **dashed lines** matching the elburg dashboard style
(visually distinguishes "ambient" data from "link" data).

## Source

- `self` node: `noise_floor` from `get_stats_radio` already lands in
  `_ts_radio` via `_on_self_stats_radio` (see #0 ingestion table).
- Remote nodes: not directly available. **Skip remote nodes for v1.**
  In screenshots the elburg dashboard shows remote node noise floors —
  that data comes from periodic `req_status_sync` polls which we don't
  do today. Adding that is a bigger project (poll budget, response
  latency, offline-node fallback). Out of scope here; note in the
  panel's empty-state.

For v1 the chart will typically have one or two lines (self + any
remote that volunteered noise data via a `STATUS_RESPONSE` we already
process). That's still useful as a quality-of-environment trend.

## Frontend

- `_renderNoiseChart(data)` reuses the cross-chart legend / crosshair
  pattern from #1+#2 — if a node appears in both, hiding it from the
  RSSI/SNR charts also hides it here. The three charts together form a
  per-node "radio health" group.
- Dashed series style: Highcharts `dashStyle: "ShortDash"`.

## Empty state

"Only self noise floor is recorded today. Remote-node noise reporting
needs periodic STATUS_SYNC polling — planned." (Plus a Wiki link if/when
we add one.)

## Tests

- Inherits `_q_noise` test from #0.
- Manual: 24 h chart for self renders; series persists across panel
  re-open without flicker (`series.setData` style update).

## Effort

~1 h, assuming #1+#2 patterns are in place.

## Dependencies

- **Requires #0** (`ts_radio` + `_q_noise`).
- Soft dep on #1+#2 for the cross-chart legend helper.
