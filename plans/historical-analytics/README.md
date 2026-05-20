# Historical Analytics — Implementation Plan

Adds the time-series and distribution panels seen on
[dashboard-elburg.f3dp.nl](https://dashboard-elburg.f3dp.nl) (screenshot
in chat). The panels live in a new "Analytics" side card (or as a tab on
the existing Stats card), gated by a shared **time-range selector**:
`3h / 6h / 12h / 24h / 48h / 7d`.

| # | Panel | File |
|---|---|---|
| 0 | Time-series storage in SQLite | [00-timeseries-store.md](00-timeseries-store.md) |
| 1 | RSSI over time (per node) | [01-rssi-snr-over-time.md](01-rssi-snr-over-time.md) |
| 2 | SNR over time (per node) | [01-rssi-snr-over-time.md](01-rssi-snr-over-time.md) |
| 3 | Noise floor history (per node) | [02-noise-floor-history.md](02-noise-floor-history.md) |
| 4 | RX / TX packet volume over time | [03-packet-volume.md](03-packet-volume.md) |
| 5 | Messages per channel | [04-messages-per-channel.md](04-messages-per-channel.md) |
| 6 | Traffic volume (packets / hour) | [05-traffic-per-hour.md](05-traffic-per-hour.md) |
| 7 | Top relay keys (path 2-char hex) | [06-top-relay-keys.md](06-top-relay-keys.md) |
| 8 | Path-length distribution (mesh depth) | [07-path-length-distribution.md](07-path-length-distribution.md) |
| 9 | Time-range selector wiring | [08-time-range-selector.md](08-time-range-selector.md) |

## Why a single shared plan

All eight panels read from the **same persistent time-series store** and
honour the **same time-range selector**. Building them piecemeal would
mean repeated, contradictory storage choices. Plan #0 lays the
foundation; #1–#8 are sibling consumers.

## Storage decision: SQLite (not JSON)

Current persistent files (`meshcore_rx_log.json`, `meshcore_stats.json`,
`meshcore_heard.json`, `meshcore_channels.json`) are small, append-mostly,
re-written wholesale every few seconds — JSON is the right call for them.

For the new analytics:

- **time-series samples** need range queries (`WHERE ts BETWEEN ? AND ?`
  AND `node = ?`) over potentially 7 days × every adv/msg event;
- **rolling aggregates** (packets/hour, msgs/channel) need atomic
  increment under concurrency from the worker thread;
- **histograms** (hop distribution) need counts grouped by integer
  buckets — SQL `GROUP BY` beats JSON manipulation;
- the row-cap pruning the message store already uses (`_MSG_STORE_CAP`)
  ports cleanly to time-series with a `(ts < ?)` `DELETE`.

Verdict: **extend the existing `meshcore_messages.db`** with new tables
under a bumped `MSG_DB_SCHEMA_VERSION`. The store is already opened in
WAL mode with `_msgdb_lock`, the migration ladder (`_msg_store_migrate`)
is in place, and we never raise to callers on DB error — all of which
the analytics panels need too.

**Schema version bump: 2 → 3.** Migration adds the new tables; no
backfill from rxlog (we are forward-only, matching the existing message-
store behaviour).

See `00-timeseries-store.md` for the full schema.

## Dependency graph

```
                       ┌────────────────────────────┐
                       │ 0 SQLite time-series tables │
                       │   + ingestion + queries     │
                       └──────────────┬──────────────┘
                                      │
        ┌─────────┬───────┬───────────┼────────┬─────────┬────────┐
        ▼         ▼       ▼           ▼        ▼         ▼        ▼
   ┌────────┐┌────────┐┌──────┐┌──────────┐┌──────┐┌──────┐┌──────────┐
   │ 1+2    ││ 3      ││ 4    ││ 5        ││ 6    ││ 7    ││ 8        │
   │ RSSI/  ││ Noise  ││ TX/RX││ msgs/    ││ pkts/││ relay││ hop      │
   │ SNR    ││ floor  ││ vol  ││ channel  ││ hour ││ keys ││ histogram│
   └────┬───┘└────┬───┘└──┬───┘└─────┬────┘└──┬───┘└──┬───┘└────┬─────┘
        └────────┴───────┴───────────┴────────┴───────┴────────┘
                                 │
                                 ▼
                       ┌────────────────────────┐
                       │ 9 Time-range selector  │
                       │   (drives all panels)  │
                       └────────────────────────┘
```

### Sequencing

**Sequential prerequisite**: #0 ships first. Without the storage layer
nothing else has data to plot.

**After #0, parallel-safe**: #1+#2 (same chart pair), #3, #4, #5, #6,
#7, #8 are independent — different panels, different SQL queries, no
shared frontend state beyond the time-range selector.

**Independent of everything**: #9 can be drafted up front so when each
panel lands it slots into the same selector frame.

Suggested merge order if working serially:

1. #0 + #9 (foundation + UX shell)
2. #1+#2 (highest user value — per-node link quality)
3. #4 RX/TX volume (uses same time-bin pattern as #1, easy follow-up)
4. #3 noise floor (same pattern again)
5. #8 hop histogram (uses different query shape — first GROUP BY panel)
6. #6 packets/hour (extends #4 with hourly bucketing)
7. #7 top relay keys (table, not a chart — different rendering)
8. #5 messages per channel (uses existing `_stats` payload mostly)

## Cross-cutting

| Concern | Approach |
|---|---|
| Chart library | Reuse Highcharts (already lazy-loaded for radar + donut). One global theme; no new lib. |
| WebSocket query shape | New cmd `{cmd: "analytics", panel: "rssi", from: ts, to: ts, nodes?: [...]}` → `{ok:true, series: [...]}`. Centralised in `_handle_analytics_query`. |
| Caching | Server-side LRU on `(panel, from-bucket, to-bucket)` keyed at minute granularity, TTL 30 s. Saves repeated 7-day scans when several panels open at once. |
| Pruning | New `_ts_prune()` runs on the same opportunistic cadence as `_msg_store_prune`. Default retention 14 days for raw samples, indefinite for hour-bucketed aggregates. |
| Backfill | None. Forward-only from the moment #0 ships. Older sessions show partial ranges. |
| Build version | No Domoticz build bump required. |

## Out of scope

- Per-channel time-series (we only need msgs/channel totals).
- Export to CSV / PNG. (Highcharts ships these for free — just enable
  the export menu if the user asks.)
- Long-term retention (months / years). 14 days is enough for an
  amateur-radio dashboard; longer needs proper down-sampling.
- A "compare two nodes" overlay tool — power-user feature, defer.
