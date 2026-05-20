# Feature 4 — RX / TX Packet Volume Over Time

## Goal

Single Highcharts spline chart with two series — **RX (cyan)** and **TX
(red)** — counting packets per bucket over the selected range. Mirrors
the elburg "RX / TX PACKET VOLUME" panel.

## Source

- `ts_packets_min` table (from #0), bucketed up to the selector's
  bucket size via `_q_packets`.
- Data is self-only (we count our own radio's TX/RX deltas from
  `get_stats_packets` every `STATS_REFRESH_S`).

## Bucket alignment with the chart

`_q_packets` returns `[{ts, rx, tx}, …]` already rolled up. Highcharts
spline:

```js
series: [
  { name: "RX", color: "#3eb6e6", lineWidth: 2, marker: { enabled: false } },
  { name: "TX", color: "#e63946", lineWidth: 2, marker: { enabled: false } },
]
```

## Edge cases

- First sample after the plugin starts shows a spike (delta vs zero).
  Drop the first delta on session start to avoid the wall.
- Counter rollover (32-bit unsigned on the radio): detect `new < prev`
  and treat as wrap; record `(MAX_U32 - prev) + new`.
- Time-skew on the worker host: clamp deltas to a sane upper bound
  (≤ 100 000 packets per `STATS_REFRESH_S`).

## Tests

- `_q_packets` returns one row per bucket, summed correctly across
  bucket boundaries (covered in #0).
- Manual: 3 h vs 24 h ranges show coarser vs finer detail; TX/RX both
  visible; tooltip shows exact counts.

## Effort

~2 h.

## Dependencies

- **Requires #0** (`ts_packets_min`, `_q_packets`).
- Independent of all other sibling panels.
