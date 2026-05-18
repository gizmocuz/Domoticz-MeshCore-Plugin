# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Domoticz plugin that integrates MeshCore LoRa mesh nodes as automation devices. MeshCore is treated as a telemetry/event backend — the plugin exposes only useful automation data, not the full MeshCore contact/channel structure.

## Development

No build system or test framework is configured yet. The plugin runs inside the Domoticz Python plugin runtime — it cannot be executed standalone.

To test manually: copy `plugin.py` and `meshcore.html` into a Domoticz plugin folder (e.g. `domoticz/plugins/MeshCore/`) and restart Domoticz. Logs appear in the Domoticz log viewer.

The `Domoticz` module (imported in `plugin.py`) is provided by the Domoticz runtime and is not installable via pip. When writing logic outside of Domoticz lifecycle hooks, keep it in plain Python so it can be tested independently.

## Architecture

### Plugin lifecycle (Domoticz pattern)

Domoticz calls module-level functions (`onStart`, `onStop`, `onHeartbeat`). These delegate to a singleton `BasePlugin` instance. Never block the main thread — use a worker thread for I/O.

`onStop` fires on every disable AND on Domoticz shutdown — there is no separate "uninstall" hook. Cleanup logic must be idempotent and **must not delete user-visible state** that we want to survive a restart. In particular:

- `meshcore.html` and `leaflet/` are re-installed on every `onStart`, so removing them in `onStop` is fine (and we do).
- `meshcore_devices.json`, `meshcore_rx_log.json`, `meshcore_channels.json`, `meshcore_stats.json`, `meshcore_heard.json` accumulate runtime state (24 h heatmap, signal history, last-known device map, lifetime stats, heard-over-air nodes). They all live in the **plugin directory** (not `www/templates/`). They are **deliberately not removed** on `onStop` so a restart preserves history. On `onStart` they are **loaded back into memory** (`_load_rx_log`/`_load_stats`/`_load_heard`/`_load_channels`) so the very first WebSocket snapshot reflects last-known state immediately (parity with the old file-polling behaviour). `_migrate_state_files()` performs a one-time move of any legacy copies from `www/templates/` into the plugin dir.
- `meshcore_locations.json` is user-owned (manual location overrides), read from the plugin directory; never delete it.
- `meshcore_messages.db` is the **SQLite message store** (plugin directory). It is the single source of truth for **all** messages — the global inbox (every channel) and per-contact DM conversations. Opened in `_msg_store_open` (WAL mode, own `_msgdb_lock`, never raises to callers); rows inserted by `_msg_store_add` on every inbox/DM/outgoing line, ACK status updated by `_msg_store_set_ack`, paginated/searched by `_msg_store_query`. It is forward-only (no history backfill) and **deliberately not removed** on `onStop`. Row cap `_MSG_STORE_CAP` pruned every `_MSG_STORE_PRUNE_EVERY` inserts. A `preferences (key TEXT PRIMARY KEY, value TEXT)` table stores `db_version`; `MSG_DB_SCHEMA_VERSION` (currently `1`) is the code's target. `_msg_store_migrate()` runs an ordered `while ver < MSG_DB_SCHEMA_VERSION` patch ladder on open (v1 = baseline, no ALTERs) — future schema changes add one `elif ver == N:` branch and bump the constant; it is idempotent and back-compatible with a pre-existing preferences-less DB (treats missing version as 0). `_pref_get`/`_pref_set` are the accessors.

### Implementation (meshcore Python package over TCP)

```
Domoticz Plugin
  ├── Worker thread (asyncio event loop)
  │     ├── MeshCore.create_tcp(host, port)         — connects via meshcore package
  │     ├── mc.commands.get_contacts()              — refresh contact list every 30s
  │     ├── mc.commands.get_msg()                   — drain message queue every 30s
  │     ├── mc.commands.get_stats_core()            — self node: battery, uptime every 5 min
  │     ├── mc.commands.get_stats_radio()           — self node: RSSI, SNR, noise every 5 min
  │     ├── mc.commands.get_stats_packets()         — self node: pkt counters every 5 min
  │     ├── EventType.CONTACT_MSG_RECV push         — live messages from known contacts
  │     ├── EventType.CHANNEL_MSG_RECV push         — live channel messages
  │     ├── EventType.ADVERTISEMENT push            — node broadcast adverts
  │     └── Puts results on a queue
  └── onHeartbeat()
        └── Drains queue → updates Domoticz devices
```

### What data is actually available (verified against live hardware)

| Source | Data |
|---|---|
| Self node (`get_stats_core`) | `battery_mv`, `uptime_secs`, `errors`, `queue_len` |
| Self node (`get_stats_radio`) | `noise_floor`, `last_rssi`, `last_snr`, `tx_air_secs`, `rx_air_secs` |
| Self node (`get_stats_packets`) | `recv`, `sent`, `flood_tx`, `direct_tx`, `flood_rx`, `direct_rx` |
| Contacts list | `adv_name`, `last_advert`, `out_path_len`, `adv_lat`, `adv_lon`, `type`, `public_key` |
| Incoming messages (get_msg / push) | `text`, `SNR`, `path_len`, `sender_timestamp`, `type` (CHAN/PRIV) |
| Advertisement push | `adv_name`, `adv_lat`, `adv_lon`, `adv_timestamp` |

> **Note:** `send_statusreq()` (old text API) does NOT return data from remote nodes. Use `req_status_sync()` (binary API) instead — it sends a binary status request and waits for `STATUS_RESPONSE` containing `bat`, `last_rssi`, `last_snr`, `noise_floor`, `uptime`, etc. Remote nodes may still not respond if offline or firmware-limited.

### Dependencies

- `pip install meshcore` — Python package for MeshCore TCP communication

### Domoticz devices (DomoticzEx framework)

The plugin uses `import DomoticzEx as Domoticz`. Devices are keyed by a string
**DeviceID**; each DeviceID carries a `Units` dict. There is **no 255-unit
cap** — the old `NODE_BASE`/`NODE_SLOTS`/`_node_unit`/`_node_index` slot math
is gone. Access devices via the `_dev(device_id, unit)` helper and resolve a
node name to its DeviceID via `_device_id_for(name)`.

DeviceID scheme:

| DeviceID | Meaning | Units |
|---|---|---|
| `mesh` | Global devices | `UNIT_INBOX=1`, `UNIT_SEND=2`, `UNIT_MSGS_RECV=3`, `UNIT_MSGS_SENT_=4` |
| `self` | The connected node | `OFF_*` (see below) |
| `<pubkey[:12]>` | A remote contact | `OFF_*` (see below) |

`_device_id_for(name)` returns `"self"` for the connected node, the 12-hex
pubkey prefix for a remote contact (from `self._node_did`), or `None` if the
pubkey isn't known yet (device is created on the next contacts poll). Units
are 1-based because DomoticzEx requires `Unit >= 1`.

#### Self node units (DeviceID `self`)

| Unit (OFF_*) | Device | Type |
|---|---|---|
| 1 STATUS | Status | Switch (always On when connected) |
| 2 BATT_PCT | Battery % | Percentage |
| 3 BATT_V | Battery V | Custom (V) |
| 4 RSSI | RSSI | Custom (dBm) |
| 5 SNR | SNR | Custom (dB) |
| 6 NOISE | Noise Floor | Custom (dBm) |
| 7 LASTSEEN | Last Seen | Text |
| 11 UPTIME | Uptime | Custom (min) |
| 12 AIRTIME | Airtime TX | Custom (s) |
| 13 MSGS_SENT | Pkts Sent | Custom (pkts) |
| 14 MSGS_RECV | Pkts Recv | Custom (pkts) |

#### Remote node units (DeviceID = pubkey[:12])

Only data that is reliably available without over-the-air requests:

| Unit (OFF_*) | Device | Type |
|---|---|---|
| 1 STATUS | Status | Switch (On/Off based on last_advert age < 8h) |
| 5 SNR | SNR | Custom (dB) — from incoming messages |
| 7 LASTSEEN | Last Seen | Text |
| 10 HOPS | Hops | Custom (hops) — from contact out_path_len |

### Poll intervals

| What | Interval | Constant |
|---|---|---|
| Message drain + contacts refresh | 30 s | `MSG_POLL_INTERVAL` |
| Self-node stats (core + radio + packets) | 300 s | `SELF_STATS_INTERVAL` |

### Config (Domoticz hardware params)

| Param field | Content |
|---|---|
| Address | MeshCore TCP host |
| Port | TCP port (default 5000) |
| Mode4 | Install custom dashboard page (`"true"` / `"false"`) |
| Mode6 | Debug level (0 / 62 / -1) |

### Custom dashboard page

`meshcore.html` is a self-contained HTML+JS dashboard (external dependency: Leaflet.js CDN for the node map). On `onStart`, if Mode4 is `"true"`, the plugin copies `meshcore.html` verbatim to `<domoticz_root>/www/templates/meshcore.html`. The page is removed on `onStop`.

Dashboard features:
- Node cards with live telemetry, battery bars, and signal quality bars (SNR)
- Human-readable uptime formatting (e.g. "2d 5h 12m" instead of raw minutes)
- Collapsible node map (Leaflet.js + dark CARTO tiles) showing nodes with GPS coordinates
- Manual location overrides via `meshcore_locations.json` in the plugin directory
- Message inbox with filters, search, compose bar, emoji picker, and @mention highlighting
- The map only appears when at least one node has coordinates (from `adv_lat`/`adv_lon` or manual overrides)
- The map is collapsed by default to keep the chat visible; click the header to expand

**Transport: native Domoticz plugin↔frontend WebSocket channel** (not JSON-file polling). The dashboard no longer fetches `/templates/*.json` or the Domoticz JSON API for its state. This requires **Domoticz build ≥ 17956 (`2025.2.17956`)**, which added `Domoticz.WebSocketSend` / `onWebSocketMessage`. The page opens a raw WebSocket and speaks the same wire protocol as the AngularJS `livesocket` service (subscribe topic `plugin:MeshCore`, `plugin_command`, `plugin`).

Frames are serialized by the plugin with `json.dumps` and sent as a string (Domoticz's built-in dict→JSON is lossy: it emits non-string keys unquoted and `None` as `"None"`); the frontend `JSON.parse`s `data`. Each frame is `{ "t": <type>, ... }`:

- `cmd_result` — ack/result for a `hello` or `cmd` (carries the correlation `id`).
- `snapshot` — **lean** first-paint state: `deviceMap` (self + contacts + inbox), `stats`, `channels`, plus `deviceSeq` (the deviceMap-delta baseline marker). `heard` is **deliberately excluded** (it can be hundreds of KB) and sent as a deferred `heard` follow-up frame immediately after the snapshot.
- `devices` (full) / `devices_delta` (changed/added nodes, removed names, changed scalars + `seq`) — incremental device map; gap → frontend sends `{t:'resync',feed:'devices'}` → plugin forces a full `devices`.
- `stats` / `heard` / `channels` — pushed when their per-feed dirty flag is set, coalesced to ≤1/s.
- `rxlog` / `rxlog_delta` — on-demand: subscribed (`{t:'sub',feed:'rxlog'}`) only while a heavy panel (firehose/traffic/channels/stats) is open, with seq + gap recovery.
- `inbox_page` — paginated reply to an `inbox_query`: `{id, scope, search, rows:[{id,chan,sender,epoch,bad,body,hops,snr,rssi,path,ack,dir}], has_more, oldest_id}`, newest-first. Drives **both** the main inbox and pinned DM conversation views (server-side pagination + search; the dashboard no longer reads any Domoticz text-log for messages).

Inbound from the dashboard: `hello`, `sub {feed}`, `cmd {cmd,id}`, `resync {feed}`, `inbox_query {id, scope, before, limit, search}` (`scope` = `"all"` | channel tag (`"C<idx>"` / stored name) | `"P"` (all DMs) | `"@<contact>"` (one DM thread); `before` = pagination cursor = previous page's `oldest_id`). The plugin still distinguishes self vs remote nodes by Domoticz device name suffix (`" Uptime"` = self, `" Hops"` = remote). Each metric links to `/index.html#/Devices/{idx}/Log`.

Connection state: the connected node's online/offline badge is driven by the Domoticz `self` STATUS device — the plugin sets it Off on node (USB/TCP) disconnect and back On on reconnect (propagated via `devices_delta`). The self-card settings (gear) button is disabled while the node is offline.

The self node card shows: Battery, Voltage, RSSI, SNR, Noise Floor, Uptime, TX Air, Pkts Sent, Pkts Recv, Last Heard.
Remote node cards show: Battery, Voltage, RSSI, SNR, Hops, Last Heard. (The "Last Heard" label is a UI rename; the underlying device/key is still `last_seen`.)

The legacy Domoticz "Mesh Send" text device (`UNIT_SEND`) has been removed; sending is still available **from the dashboard** (compose bar / inbox replies) and is dispatched as a `cmd` frame over the WebSocket channel, executed by the worker via `_send_message_for_text`. A one-time `onStart` cleanup deletes the stale `UNIT_SEND` device from older installs.

The per-contact "<name> Messages" DM text devices (`OFF_MSGS`) are **retired**: they are no longer created for new contacts, no longer written (`_log_contact_dm` is a no-op stub kept only to avoid touching call sites), and the `"msgs"` device-map key is gone. DM conversation history is now served from the SQLite store via `inbox_query` `scope="@<contact>"`. Pre-existing `OFF_MSGS` devices are **deliberately NOT auto-deleted** on `onStart` (unlike `UNIT_SEND`) — the user removes them manually if/when they decide they are no longer needed. The single global "Mesh Inbox" text device (`UNIT_INBOX`) stays, but only as the realtime new-message trigger (its `sValue` change drives the `devices_delta` `inbox_value` push the dashboard uses for instant rendering); it is no longer read for history.
