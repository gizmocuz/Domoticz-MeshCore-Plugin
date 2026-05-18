# Todo

## Reliable message send with retry (companion-app style)

Use the `meshcore` library's built-in retry instead of plain single-shot sends,
so private messages are retried until ACKed and the user can see how many
attempts it took (like the phone companion app).

### Background

- `meshcore/commands/messaging.py`:
  - `send_msg_with_retry(dst, msg, timestamp=None, max_attempts=3,
    max_flood_attempts=2, flood_after=2, timeout=0, min_timeout=0)` — retries on
    missing ACK, encodes the attempt number into the packet, and falls back from
    a direct path to flood after `flood_after` failed attempts. Returns `None`
    if no ACK ever arrived, else the send `Event`.
  - `send_msg(...)` / `send_chan_msg(...)` — what the plugin uses today,
    single-shot, no ACK wait.
- ACK-based retry only applies to **direct/private** messages. **Channel/flood**
  messages (`send_chan_msg`) are broadcast — no ACK, so retry is not applicable
  and must keep using `send_chan_msg` as-is.

### Tasks

- [ ] In `plugin.py`, route private (non-`#`) sends through
      `send_msg_with_retry` instead of `send_msg`. Keep channel sends on
      `send_chan_msg`.
- [ ] Decide retry tunables (start with library defaults: 3 attempts, flood
      fallback after 2). Consider exposing as a hardware param later.
- [ ] Capture the attempt count. The library does not return it directly —
      either parse the `"Retry sending msg: N"` log lines via a logging handler,
      or reimplement the loop in the plugin to track attempts explicitly.
- [ ] Surface delivery outcome to the dashboard inbox: distinguish
      "delivered (ACK after N attempts)" vs "no ACK / failed" instead of the
      current optimistic-echo-only behaviour. Ties into the existing
      pending/settled placeholder logic in `meshcore.html`.
- [ ] Handle the `None` return (all attempts failed) — show the message as
      failed in the inbox rather than silently settling it as sent.
- [ ] Test against a live node: direct send with good path, direct send with
      stale path (verify flood fallback), and confirm channel sends are
      unaffected.

### Notes

- `send_msg_with_retry` blocks while waiting for ACKs (up to
  `suggested_timeout * 1.2` per attempt). It already runs on the asyncio worker
  thread, so this is fine, but message-drain cadence may need a re-check so a
  slow retry doesn't starve the poll loop.

## WebSocket transport migration

Replace the `www/templates` JSON-file + poll architecture with Domoticz's
native plugin↔frontend WebSocket channel (build **17956+**,
`2025.2.17956`, 2026-05-16). Decisions: WebSocket-only (no fallback, README
states min build), in-memory state pushed (no HTTP-served JSON), big
rx-log on-demand + deltas. Full feature-split plan with dependency graph:
see [`plans/websocket-migration/`](websocket-migration/README.md).

## Repeater directory for path-hop resolution (community map)

A static snapshot `meshcore_repeaters.json` is already bundled (NL repeaters,
keyed by full public key, with name/lat/lon/last_advert/freq/sf/bw/cr). It lets
the dashboard resolve `P(n):a>b>c` path hashes to repeater names even for
repeaters we've never heard advertise — but only reliably at **2-byte+** hashes
(1-byte is hopelessly ambiguous; see analysis below).

### Snapshot facts (as downloaded 2026-05-16)

- Source: `https://map.meshcore.io/api/v1/nodes` (global; `map.meshcore.dev`
  307-redirects here). ~44.5k nodes, ~34 MB.
- Filtered to **type=Repeater** within an **NL bbox** (lat 50.6–53.7,
  lon 3.2–7.4): **3,744 repeaters**, ~767 KB JSON.
- 2-byte prefix uniqueness: NL repeaters → only 113 colliding prefixes
  (233 nodes), ~94% unique. **Global 2-byte is ~39% colliding — do not use
  globally; keep it region-scoped.** 3-byte is ~unique even globally.
- Per-repeater fields kept: `name`, `lat`, `lon`, `last_advert`, `freq`,
  `sf`, `bw`, `cr`. Also available but dropped: `link` (a `meshcore://…`
  contact-import URI — could power one-click "add repeater as contact"),
  `source`/`inserted_by`/`updated_by` (provenance, not useful).

### Periodic poll (future — opt-in)

- [ ] Add an opt-in hardware param (default OFF) + a configurable region
      bbox (default NL) to periodically (e.g. once/day) fetch the map API,
      filter to repeaters in-bbox, and rewrite `meshcore_repeaters.json`.
      Keep it server-side on a slow timer so we never ship/poll the 34 MB
      blob from the browser.
- [ ] Wire `_resolvePathHop` to use this file as a **final fallback** after
      contacts + heard, **2-byte+ only**, and **drop ambiguous prefixes**
      (better blank than mis-attributed — it's an unauthenticated community
      directory, not identity).
- [ ] Optionally surface GPS from this file on the node map for repeaters
      that appear in a path but we've never heard (so a route can be drawn
      even for unheard hops).
- [ ] Consider a "add this repeater as a contact" action using the
      `meshcore://` import link from the same API entry.
- [ ] Staleness: stamp `generated` (already in the file) and show its age
      in the UI; never treat as authoritative.
