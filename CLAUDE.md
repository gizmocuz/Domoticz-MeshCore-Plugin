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
- `meshcore_devices.json`, `meshcore_rx_log.json`, `meshcore_channels.json` accumulate runtime state (24 h heatmap, signal history, last-known device map). They are **deliberately not removed** on `onStop` so a restart preserves history; they're overwritten on the first heartbeat after restart anyway.
- `meshcore_locations.json` is user-owned (manual location overrides). Treat it as read-only state that the plugin copies from `plugin_dir/` into `www/templates/`; never delete it from either location.

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

The page derives all node names dynamically from the Domoticz JSON API (`/json.htm?type=command&param=getdevices&order=Name`) — no template injection needed:
- **Self node**: identified by a device whose name ends in `" Uptime"` (unique to the connected node)
- **Remote nodes**: identified by devices whose name ends in `" Hops"` (unique to tracked nodes)

The page fetches live device data and auto-refreshes every 10 seconds. Each metric links to `/index.html#/Devices/{idx}/Log`.

The self node card shows: Battery, Voltage, RSSI, SNR, Noise Floor, Uptime, TX Air, Pkts Sent, Pkts Recv, Last Seen.
Remote node cards show: Battery, Voltage, RSSI, SNR, Hops, Last Seen.

Send functionality has been intentionally removed.
