# Feature 6 — Traffic Volume per Hour

## Goal

Vertical bar chart, one bar per hour in the selected range, showing
**total packets per hour** (RX + TX combined). Matches the elburg
"TRAFFIC VOLUME (PACKETS PER HOUR)" panel.

## Source

- `ts_packets_hourly` table from #0. Already upserted with
  `(rx_count, tx_count)` per `hour_ts` — `_q_packets_hourly(from, to)`
  returns `[{hour_ts, total}, …]`.

## Frontend

- Highcharts `column`, all-purple bars matching the screenshot.
- X-axis: `HH:00:00`. Tick every hour for 6/12 h ranges; every 6 h for
  48 h; every day for 7 d.
- Y-axis: total packets, integer.
- Tooltip: `1 234 packets (567 RX / 667 TX)` — uses both columns from
  `ts_packets_hourly`.

## Tests

- `_q_packets_hourly`:
  - returns one row per hour bucket in the range;
  - sums RX + TX correctly;
  - excludes the current (in-progress) hour from totals when the
    selected range ends exactly on `now()` (avoids a misleading
    half-height final bar) — caller passes `inclusive=False`.

## Effort

~1.5 h.

## Dependencies

- **Requires #0** (`ts_packets_hourly`).
- Conceptual sibling of #4 — both can land in either order.
