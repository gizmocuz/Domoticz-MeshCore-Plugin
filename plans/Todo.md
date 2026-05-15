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
