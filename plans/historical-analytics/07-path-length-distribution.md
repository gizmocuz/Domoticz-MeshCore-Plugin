# Feature 8 — Path Length Distribution (Mesh Depth)

## Goal

Vertical bar chart titled "PATH LENGTH DISTRIBUTION (MESH DEPTH)" —
buckets along the x-axis (`2 hops`, `5 hops`, `8 hops`, …) and counts on
the y-axis. Same as the elburg panel.

## Source

- `ts_hops` table from #0. Every routed frame whose `path_len` is
  known (read from the rxlog event) inserts one row.
- `_q_hop_histogram(from, to)` returns:
  ```python
  SELECT hops, COUNT(*) FROM ts_hops
   WHERE ts BETWEEN ? AND ?
   GROUP BY hops ORDER BY hops
  ```

## Frontend

- Highcharts `column` chart, cyan bars.
- X-axis: integer `hops`, sparse labels (the elburg dashboard shows
  every 3 hops). Highcharts `xAxis.tickInterval = 3`.
- Y-axis: integer counts.
- Tooltip per bar: `7 hops → 142 frames (15.3% of range)`.

## Interpretation note

For amateur-radio mesh depth analysis, the *most common* hop count is
the meaningful headline number. Add a small caption under the title:
`median = 8 hops · p90 = 14 hops` computed from the returned data.

## Tests

- `_q_hop_histogram`:
  - groups correctly by `hops`;
  - excludes rows outside the range;
  - returns `[]` for an empty range.
- Frontend: median / p90 computed correctly with manual fixture.

## Effort

~2 h.

## Dependencies

- **Requires #0** (`ts_hops` table + ingestion).
- Stand-alone.
