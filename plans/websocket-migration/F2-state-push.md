# F2 — State push + frontend reducer

Push device-map / stats / heard / channels over the socket; the dashboard
renders from pushed state instead of polling JSON files.

## Dependencies
**F1** (transport, `_push()`, `hello`/subscribe plumbing). Intra-feature
parallel: the plugin push side and the frontend reducer side can be split
across two agents against the agreed message protocol.

## Scope

### plugin.py (push side)
- Keep device map / stats / heard / channels in memory (mostly already).
- On change, `_push` a delta; coalesce to ≤1 push/sec/feed (dirty flag +
  timer on the worker cadence, like the current rx-log write throttle).
- On `hello` (from F1) reply `snapshot` = full
  `{deviceMap, stats, heard, channels, selfInfo}`.
- Stop writing `meshcore_devices/stats/heard/channels.json` into
  `www/templates` (relocation handled in F4; here just stop the push
  consumers needing them).

### meshcore.html (reducer side)
- `onPluginMessage` reducer: apply `snapshot` (replace) and
  `devices/stats/heard/channels` (merge) into the existing in-memory
  `_deviceMap` / `_stats` / `_heard` / `_channelNames` the renderers use.
- Delete every `fetch('/templates/*.json?_=ts')` and the poll
  `setInterval`s; re-render on push instead.
- On (re)connect send `{t:'hello'}`; ignore feeds until snapshot applied.

## Acceptance
- Fresh page load shows full state from a single `snapshot` (no `fetch`).
- A device/stat/heard/channel change appears within ~1s with no file I/O
  to `www/templates`.
- Domoticz restart / browser reload → reducer rebuilds from a fresh
  `snapshot`, no stale UI.
