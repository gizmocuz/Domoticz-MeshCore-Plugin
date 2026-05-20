# Feature 1+2 — RSSI / SNR Over Time

## Goal

Two side-by-side Highcharts line charts in the Analytics card:

- **RSSI over time** — y axis dBm, multi-line, one series per node.
- **SNR over time** — y axis dB, multi-line, one series per node.

Both honour the shared time-range selector (#9). Node series are
identified by adv_name with a colour locked to that node across both
charts (so the legend reads the same).

## Why

- Most useful per-link health diagnostic — matches the elburg panels.

## Data flow

```
ts_radio table → _q_rssi_snr(panel="rssi"/"snr", from, to)
   → { categories: [ts_buckets…], series: [{name, color, data}, …] }
WS frame {t:"analytics_result", panel:"rssi"|"snr", …}
   → frontend renders Highcharts spline/line
```

## Frontend

- New `_renderRssiChart(data)` / `_renderSnrChart(data)` mirrored after
  the existing radar/donut renderers.
- Hosted in a new `analytics-card` element (open by default, collapsible).
- Each chart 250 px tall, side-by-side at ≥ 900 px viewports, stacked
  below that.
- Hidden by `display: none` when no data in the chosen range; empty-state
  reads "No samples in the last X" with a hint to widen the range.
- Subscribed only while the Analytics card is open (mirrors the rxlog
  on-demand subscription pattern).

### Series & colours

- Cap visible series to top 8 nodes by sample count in the range;
  remainder collapse into "Other" (averaged). Avoids 80-line spaghetti.
- Colour stable across reloads: derive from a hash of the node's pubkey
  into the existing topology palette (`_typeColour` doesn't suffice —
  we need per-node colour, not per-type). Add `_nodePalette(pk)`
  returning HSL with hash-stable hue, fixed S/L.
- Click on a legend item hides that node from both RSSI + SNR charts
  simultaneously (cross-chart legend).

### Tooltip

Shared crosshair tooltip across both charts:

```
03:37:21   RSSI    SNR
self       −78     8.5
RPT-LOIC   −95     2.1
…
```

Achieved via Highcharts' `chart.synchronizeExtremes` pattern (linked
xAxis + crosshair sync handler).

## Tests

- `tests/test_timeseries_store.py::test_q_rssi_snr_basics` (already in
  #0).
- Manual:
  - 6 h range with > 1 active node renders both charts.
  - Hiding a series in the legend hides it in both charts.
  - Switching range refetches and redraws.
  - Closing the panel cancels the rxlog subscription.

## Effort

~3 h, assuming #0 is in place.

## Dependencies

- **Requires #0** for the `ts_radio` table and `_q_rssi_snr` query.
- Soft dep on #9: until the selector exists, a fixed `6h` default is
  acceptable.
