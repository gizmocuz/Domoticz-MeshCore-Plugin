# Feature 7 — Top Relay Keys (Path 2-char Hex)

## Goal

Sortable table listing the top relay path-hash bytes seen on incoming
routed frames, with:

| Column | Notes |
|---|---|
| Hex | 2-char lowercase hex (e.g. `1d`, `72`) |
| Name | Best-known resolved adv_name for that hash (may be ambiguous — show the most-recently-seen) |
| Count | Total frames whose `out_path` contains this byte in the selected range |

Sorted by count descending. Up to 20 rows visible; scrollable beyond.

## Source

- The existing `_rx_log` already carries each frame's `out_path`. The new
  `ts_relay_keys` table from #0 maintains a running count per hex byte.
- For the panel, `_q_top_relays(from, to)` returns the top rows from
  `ts_relay_keys`. **However:** `ts_relay_keys` is a lifetime
  cumulative tally, not a range tally. Two options:

  1. **Cumulative-only (cheap):** ignore the time-range selector for
     this panel. Tooltip explains "Counts are lifetime totals since
     plugin first ran."
  2. **Range-accurate (expensive):** add a `ts_relay_events(ts, hex)`
     table and scan it. Cardinality is ~20× higher than `ts_hops` and
     gains us a tiny amount of info; not worth it.

  **Choose option 1.** This panel is the only one that doesn't honour
  the range selector; document it in the panel header.

## Name resolution

`ts_relay_keys.name` is updated to whichever adv_name we resolved most
recently for that hex (via `_resolvePathHop` on the worker). The
2-char prefix is ambiguous in theory (1/256 collision rate); display
the most-recently-seen mapping but show all candidates in a tooltip
when hovered.

## Frontend

- Plain HTML `<table>` with sticky header inside the analytics card.
- Hex column rendered with the same chip styling as the rxlog path
  display.
- Click a row → opens that node's side panel (if name resolves to a
  known contact / heard node).

## Tests

- `_ts_ingest` for a rxlog event with `out_path = "1d72ca"` increments
  three rows (`1d`, `72`, `ca`) in `ts_relay_keys` and stamps
  `last_seen`.
- `_q_top_relays(limit=20)` returns rows ordered by count desc.

## Effort

~1.5 h.

## Dependencies

- **Requires #0** (`ts_relay_keys` table + ingestion).
- Stand-alone otherwise.
