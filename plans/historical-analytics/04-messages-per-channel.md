# Feature 5 — Messages per Channel

## Goal

Horizontal bar chart titled "MESSAGES PER CHANNEL (n total)" — one bar
per channel we've seen messages on in the selected range, sorted
descending. Colour per channel = the existing channel tint used in the
inbox.

## Source

- For ranges ≤ 24 h: derive directly from the existing rxlog +
  `_msg_store_query` so no new ingestion is required. Easier and
  cheaper than introducing a per-channel time-series table.
- For ranges > 24 h: query `meshcore_messages.db` directly
  (`SELECT chan, COUNT(*) FROM messages WHERE epoch BETWEEN ?…`),
  GROUP BY chan. The message store already retains 7 days of rows on
  modest meshes (subject to `_MSG_STORE_CAP`).

Single new helper `_q_msg_per_channel(from, to)` returns
`[{name, count, color}, …]`.

## Frontend

- Highcharts bar chart, `inverted: true` (horizontal bars).
- Total in the panel title from `series[0].data.reduce(+)`.
- Click a bar → opens the inbox side card pre-filtered to that channel
  (`_setInboxChan(name)`).
- Empty-state: "No channel messages in the last X."

## Tests

- `_q_msg_per_channel`:
  - returns rows from the messages table grouped by `chan`;
  - excludes the `P` (private) bucket (per UI: this is per-channel,
    DMs are tracked elsewhere);
  - sorted by count descending;
  - skips rows older than `from`.
- Manual: bar click opens the inbox with the right chip selected.

## Effort

~2 h (most of the work is the channel-colour lookup and click
plumbing).

## Dependencies

- **Requires #0** only for the time-range selector contract; the data
  itself comes from existing `meshcore_messages.db`. Safe to ship before
  #0's `_ts_*` tables exist.
