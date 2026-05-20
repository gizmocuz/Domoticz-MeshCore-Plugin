# Feature 0 — Time-Series Storage (SQLite)

## Goal

Extend the existing `meshcore_messages.db` SQLite store with tables for
per-node radio samples and aggregate counters. All other analytics
panels (#1–#8) query these tables.

## Why SQLite, not JSON

| Requirement | JSON file | SQLite |
|---|---|---|
| Range queries `(node, ts BETWEEN)` | Linear scan of full file | Indexed B-tree |
| Atomic increment under worker-thread concurrency | Read-modify-write race, file-lock dance | Single `UPDATE` |
| GROUP BY for histogram (#8) | Custom Python aggregation | One SQL query |
| Pruning old rows | Rewrite whole file | `DELETE WHERE ts < ?` |
| Schema evolution | Ad-hoc per-file migration | Versioned ladder in `_msg_store_migrate` |
| WAL durability / crash safety | Atomic-rename trick we already use | Built-in |

We already pay for the SQLite dependency for `meshcore_messages.db`,
the lock + migration plumbing is already there, so the marginal cost is
zero.

## Schema (DB version 3)

Bump `MSG_DB_SCHEMA_VERSION = 3`. Migration `_msg_store_migrate` adds the
following when `ver == 2`:

```sql
-- Per-node radio samples. One row per event the worker observes
-- (advert, message, rxlog entry) with a usable signal reading.
CREATE TABLE IF NOT EXISTS ts_radio (
    ts        INTEGER NOT NULL,           -- unix seconds
    node_key  TEXT    NOT NULL,           -- 12-hex pubkey prefix or "self"
    rssi      INTEGER,                    -- dBm * 1, NULL if unknown
    snr       REAL,                       -- dB,      NULL if unknown
    noise     INTEGER,                    -- dBm * 1, NULL if unknown
    path_len  INTEGER,                    -- hops, NULL if not from a routed frame
    src       TEXT                        -- short tag: "adv" / "msg" / "rx"
);
CREATE INDEX IF NOT EXISTS ix_ts_radio_ts       ON ts_radio (ts);
CREATE INDEX IF NOT EXISTS ix_ts_radio_node_ts  ON ts_radio (node_key, ts);

-- Per-hour packet counters. Self-only — we only see our own TX/RX
-- counter deltas reliably. Keyed by hour-floored unix timestamp.
CREATE TABLE IF NOT EXISTS ts_packets_hourly (
    hour_ts   INTEGER PRIMARY KEY,        -- unix seconds floored to the hour
    rx_count  INTEGER NOT NULL DEFAULT 0,
    tx_count  INTEGER NOT NULL DEFAULT 0,
    flood_rx  INTEGER NOT NULL DEFAULT 0,
    flood_tx  INTEGER NOT NULL DEFAULT 0,
    direct_rx INTEGER NOT NULL DEFAULT 0,
    direct_tx INTEGER NOT NULL DEFAULT 0
);

-- Per-minute packet samples (raw). Drives the high-resolution RX/TX
-- volume chart (#4). Pruned to last 48 h.
CREATE TABLE IF NOT EXISTS ts_packets_min (
    ts        INTEGER PRIMARY KEY,        -- unix seconds floored to the minute
    rx_count  INTEGER NOT NULL DEFAULT 0,
    tx_count  INTEGER NOT NULL DEFAULT 0
);

-- Relay-key tallies. Keyed by 2-char hex path-hash prefix; we keep a
-- running counter from the rxlog so #7 doesn't have to scan every
-- event. Updated incrementally; never pruned (low cardinality, ~256).
CREATE TABLE IF NOT EXISTS ts_relay_keys (
    hex_key    TEXT PRIMARY KEY,          -- 2-char lowercase hex
    name       TEXT,                      -- best-known resolved name
    last_seen  INTEGER NOT NULL,
    count      INTEGER NOT NULL DEFAULT 0
);

-- Hop counts seen on incoming routed frames. Bucket = hops (int).
-- Pruned to last 14 days.
CREATE TABLE IF NOT EXISTS ts_hops (
    ts    INTEGER NOT NULL,
    hops  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ts_hops_ts ON ts_hops (ts);
```

## Ingestion hooks (plugin.py)

A single helper `_ts_ingest(event_kind, **fields)` is called from the
existing rxlog / message / advert dispatch points. It batches inserts
under `_msgdb_lock` with a ~1 s flush window so we don't fsync per packet
on a busy mesh.

| Source | Existing call site | New action |
|---|---|---|
| `_on_rx_log` (`plugin.py:2920`) | already appends to `_rx_log` | `_ts_ingest("rx", node_key=src_pk, rssi=…, snr=…, path_len=…)` |
| `_handle_advertisement` | logs adv into rxlog | `_ts_ingest("adv", node_key=adv_pk, rssi=…, snr=…, path_len=0)` |
| `_handle_contact_msg_recv` | inbox add + ack | `_ts_ingest("msg", node_key=sender_pk, snr=…, path_len=…)` |
| `_on_self_stats_radio` | updates self noise/RSSI/SNR devices | `_ts_ingest("rx", node_key="self", rssi=last_rssi, snr=last_snr, noise=noise_floor)` |
| `_on_self_stats_packets` | updates TX/RX counters | compute delta vs last call → `_ts_packets_add(now, drx, dtx)` |

`_ts_packets_add` upserts both `ts_packets_min` (current minute) and
`ts_packets_hourly` (current hour) in one transaction.

## Query API (worker side)

Centralised in a new `_handle_analytics_query` method. Dispatched on the
WebSocket `cmd: "analytics"` (`panel`, `from`, `to`, optional `nodes`).

```python
ANALYTICS_PANELS = {
    "rssi":       _q_rssi_snr,        # series per node
    "snr":        _q_rssi_snr,
    "noise":      _q_noise,
    "packets":    _q_packets,         # rx/tx series
    "channels":   _q_msg_per_channel, # bar data
    "hourly":     _q_packets_hourly,
    "relays":     _q_top_relays,      # table rows
    "hops":       _q_hop_histogram,
}
```

Each query function returns a JSON-shaped dict the frontend can hand
straight to Highcharts.

### Bucketing

Range → bucket size mapping (chosen so each chart has ~100–300 points):

| Range | Bucket |
|---|---|
| 3 h | 1 min |
| 6 h | 2 min |
| 12 h | 5 min |
| 24 h | 10 min |
| 48 h | 20 min |
| 7 d | 1 h |

Bucketing happens in SQL via `ts / bucket_seconds * bucket_seconds`.
Aggregation is `AVG(rssi)` / `AVG(snr)` / `AVG(noise)` for radio panels,
`SUM(rx_count)` / `SUM(tx_count)` for packets.

### Server-side cache

`@functools.lru_cache(maxsize=64)` on each `_q_*` taking a tuple
`(panel, from_bucket, to_bucket, nodes_tuple)`. Manually invalidated
when new data spills over a bucket boundary (i.e. when `_ts_ingest`
mutates a bucket whose timestamp matches the most recent cached
`to_bucket`).

## Pruning

`_ts_prune()` runs every 5 min from the heartbeat:

```python
ts_radio:         DELETE WHERE ts < now - 14*86400
ts_packets_min:   DELETE WHERE ts < now - 48*3600
ts_packets_hourly:keep all (small)
ts_hops:          DELETE WHERE ts < now - 14*86400
ts_relay_keys:    DELETE WHERE last_seen < now - 30*86400  AND count < 100
```

## Tests

`tests/test_timeseries_store.py`:

- migration from v2 → v3 creates all tables and indices.
- `_ts_ingest("rx", …)` inserts one row with the right fields.
- `_ts_packets_add` upserts the minute + hour rows correctly across a
  minute boundary and an hour boundary.
- `_q_rssi_snr` returns one series per node, averaged into the requested
  bucket size, bounded by the requested range.
- `_q_hop_histogram` returns counts by `hops` bucket.
- `_ts_prune` removes rows older than the cutoff and leaves newer rows
  alone.
- LRU cache invalidates when a fresh ingest lands inside the cached
  range's last bucket.

## Effort

~1 day:
- 2 h schema + migration + tests for migration.
- 2 h ingestion hooks (5 call sites, plus batching).
- 3 h query layer (8 panel queries, bucketing, cache).
- 1 h pruning + heartbeat hook.
- Plus integration test against a populated DB.

## Dependencies

- None inside this plan.
- Sequential prerequisite for all sibling files (#1–#8).
