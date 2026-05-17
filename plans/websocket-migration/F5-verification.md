# F5 — Verification

End-to-end validation of the migrated transport.

## Dependencies
**F1–F4** complete. Final feature.

## Scope / test matrix

- **Multi-instance**: two MeshCore hardware entries → every message
  carries the correct `hwid`; each dashboard binds only to its instance;
  `sendPluginCommand` targets the right instance.
- **Reconnect / reload**: Domoticz restart and browser reload both result
  in a fresh `hello`→`snapshot`; no stale UI, subscriptions re-established
  (livesocket auto-resubscribe + our `hello`).
- **Volume / latency**: busy mesh — confirm rx-log travels as deltas (not
  full dumps), per-feed coalescing holds (≤1/s), UI stays responsive,
  socket throughput sane.
- **Seq-gap recovery**: drop/skew rxlog seq → window re-request, no
  duplicates or gaps in the firehose.
- **Min-version UX**: run on Domoticz `<17956` (no `WebSocketSend`) →
  plugin logs the requirement once; dashboard shows an explicit
  "requires Domoticz build 17956+" banner instead of an empty/broken page;
  devices still function.
- **No regressions**: inbox, conversation toggle, per-message signal/path,
  reply / reply-private, heard filter, stats panel all work over the new
  transport.

## Acceptance
All matrix rows pass; code reviewer pass; README min-version accurate.
