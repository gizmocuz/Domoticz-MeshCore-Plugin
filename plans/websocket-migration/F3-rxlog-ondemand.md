# F3 — rx-log on-demand + deltas

The bulky rx-log / firehose / heatmap / sparkline data is only sent while a
panel that needs it is open, then as incremental deltas.

## Dependencies
**F2** (reducer, subscribe model, snapshot). Sequential after F2.

## Scope

### meshcore.html
- When the firehose / packet-analyzer / heatmap / sparkline panels open,
  send `{t:'sub', feed:'rxlog'}`; on close `{t:'sub', feed:'none'}`.
- Maintain a local rx-log buffer fed by `rxlog` (window, replace) and
  `rxlog_delta` (append). Track `seq`; on gap → re-`{t:'sub',feed:'rxlog'}`
  to get a fresh window.
- Drop the `fetch('/templates/meshcore_rx_log.json')` path.

### plugin.py
- Track per-subscriber desired feed (from F1's `sub` handling).
- On `feed:'rxlog'` subscribe → push one `rxlog` window immediately.
- On the existing rx-log cadence, if any subscriber wants rxlog, push
  `rxlog_delta` = entries appended since the last sent `seq` (monotonic
  seq per connection/instance). No subscriber → push nothing.
- Heatmap/derived aggregates ride along in `stats` (F2) so the heatmap
  panel doesn't force a full rxlog subscription just to draw bars.

## Acceptance
- With no rx-log panel open, zero rx-log traffic on the socket.
- Opening the firehose streams deltas (not full dumps) and stays correct
  under a busy mesh; closing it stops the traffic.
- A forced seq gap (e.g. reconnect mid-stream) self-heals via window
  re-request with no duplicate/missing rows.
