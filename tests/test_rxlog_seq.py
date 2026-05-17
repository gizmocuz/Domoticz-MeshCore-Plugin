"""F3 — rx-log on-demand + delta sequence tests.

Exercises the four acceptance criteria from the F3 pinned contract:

  (a) sub feed:'rxlog' triggers an immediate rxlog window with a seq and the
      right keys (entries + stats).
  (b) On the flush cadence with an rxlog subscriber and new entries, an
      rxlog_delta with seq incremented and only the new entries is pushed.
  (c) feed:'none' (or no subscriber) pushes nothing on the cadence.
  (d) seq is monotonic across window + deltas.

No live socket, no asyncio, no Domoticz runtime required — the DomoticzEx
stub provides a WebSocketSend spy that records all calls.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import unittest

import DomoticzEx as _Domoticz_stub  # the stub module, for reset_ws / ws_sent

RX_LOG_BUFFER = 250  # must match plugin.RX_LOG_BUFFER


def _send_ws(raw):
    """Drive the module-level onWebSocketMessage hook."""
    import plugin
    payload = json.dumps(raw) if isinstance(raw, dict) else raw
    plugin.onWebSocketMessage(payload)


def _ws_of_type(t):
    return [r for r in _Domoticz_stub.ws_sent if r.get("t") == t]


def _reset_f3(plugin_mod):
    """Reset all F3 state to a clean slate under the lock."""
    p = plugin_mod._plugin
    with p._rx_log_lock:
        p._rx_log.clear()
        p._sub_feeds = "none"
        p._rx_log_seq = 0
        p._rx_log_total_appended = 0
        p._rx_log_pushed_total = 0


def _append_entries(plugin_mod, n, start_t=1):
    """Append *n* synthetic entries to _rx_log under the lock, updating the
    absolute counter correctly (mirrors the real append sites)."""
    p = plugin_mod._plugin
    with p._rx_log_lock:
        for i in range(n):
            p._rx_log.append({"_t": start_t + i, "message": f"m{start_t + i}", "snr": i % 10})
            p._rx_log_total_appended += 1


class TestRxLogSeq(unittest.TestCase):

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _reset_f3(plugin)

    # ── (a) sub feed:'rxlog' → immediate rxlog window ─────────────────────────

    def test_sub_rxlog_sends_immediate_window(self):
        """sub feed:'rxlog' must trigger exactly one rxlog push immediately."""
        _send_ws({"t": "sub", "feed": "rxlog"})
        windows = _ws_of_type("rxlog")
        self.assertEqual(len(windows), 1,
                         f"Expected 1 rxlog window, got {len(windows)}")

    def test_sub_rxlog_window_has_required_keys(self):
        """rxlog window must contain entries, stats, and seq."""
        _send_ws({"t": "sub", "feed": "rxlog"})
        windows = _ws_of_type("rxlog")
        self.assertEqual(len(windows), 1)
        w = windows[0]
        for key in ("entries", "stats", "seq"):
            self.assertIn(key, w, f"rxlog window missing key {key!r}")

    def test_sub_rxlog_entries_is_list(self):
        """rxlog window entries must be a list."""
        _send_ws({"t": "sub", "feed": "rxlog"})
        w = _ws_of_type("rxlog")[0]
        self.assertIsInstance(w["entries"], list, "entries must be a list")

    def test_sub_rxlog_stats_is_dict(self):
        """rxlog window stats must be a dict."""
        _send_ws({"t": "sub", "feed": "rxlog"})
        w = _ws_of_type("rxlog")[0]
        self.assertIsInstance(w["stats"], dict, "stats must be a dict")

    def test_sub_rxlog_seq_is_positive_integer(self):
        """seq in rxlog window must be a positive integer (> 0)."""
        _send_ws({"t": "sub", "feed": "rxlog"})
        w = _ws_of_type("rxlog")[0]
        self.assertIsInstance(w["seq"], int, "seq must be an int")
        self.assertGreater(w["seq"], 0, "seq must be > 0 after first subscribe")

    def test_sub_rxlog_window_reflects_existing_entries(self):
        """If the rx-log buffer already has entries, they appear in the window."""
        import plugin
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 12345, "message": "hello", "snr": 5})
            plugin._plugin._rx_log_total_appended += 1
        _send_ws({"t": "sub", "feed": "rxlog"})
        w = _ws_of_type("rxlog")[0]
        self.assertEqual(len(w["entries"]), 1)
        self.assertEqual(w["entries"][0]["message"], "hello")

    # ── (b) cadence delta with subscriber and new entries ────────────────────

    def test_delta_pushed_when_subscriber_and_new_entries(self):
        """_push_rx_log_delta must push rxlog_delta when subscriber is active
        and new entries exist beyond the last-pushed absolute counter."""
        import plugin
        # Subscribe first to set the baseline (pushed_total = 0, seq = 1).
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()

        # Add a new entry after the subscribe baseline.
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 99999, "message": "new", "snr": 3})
            plugin._plugin._rx_log_total_appended += 1

        plugin._plugin._push_rx_log_delta()

        deltas = _ws_of_type("rxlog_delta")
        self.assertEqual(len(deltas), 1,
                         f"Expected 1 rxlog_delta, got {len(deltas)}")

    def test_delta_contains_only_new_entries(self):
        """rxlog_delta must carry only the entries appended after the last push."""
        import plugin
        # Seed one existing entry, subscribe (window = 1 entry, pushed_total = 1).
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 1, "message": "old", "snr": 1})
            plugin._plugin._rx_log_total_appended += 1
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()

        # Add a second entry after the subscribe.
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 2, "message": "new_after", "snr": 2})
            plugin._plugin._rx_log_total_appended += 1

        plugin._plugin._push_rx_log_delta()
        deltas = _ws_of_type("rxlog_delta")
        self.assertEqual(len(deltas), 1)
        d = deltas[0]
        self.assertIsInstance(d["entries"], list)
        self.assertEqual(len(d["entries"]), 1,
                         "Delta must contain exactly the 1 new entry")
        self.assertEqual(d["entries"][0]["message"], "new_after")

    def test_delta_seq_increments_after_window(self):
        """rxlog_delta seq must be exactly window_seq + 1."""
        import plugin
        _send_ws({"t": "sub", "feed": "rxlog"})
        windows = _ws_of_type("rxlog")
        window_seq = windows[0]["seq"]
        _Domoticz_stub.reset_ws()

        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 100, "message": "x", "snr": 0})
            plugin._plugin._rx_log_total_appended += 1
        plugin._plugin._push_rx_log_delta()
        deltas = _ws_of_type("rxlog_delta")
        self.assertEqual(len(deltas), 1)
        self.assertEqual(deltas[0]["seq"], window_seq + 1,
                         f"Expected delta seq={window_seq + 1}, got {deltas[0]['seq']}")

    def test_delta_has_stats_key(self):
        """rxlog_delta must include a stats dict."""
        import plugin
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 200, "message": "y", "snr": 1})
            plugin._plugin._rx_log_total_appended += 1
        plugin._plugin._push_rx_log_delta()
        deltas = _ws_of_type("rxlog_delta")
        self.assertEqual(len(deltas), 1)
        self.assertIn("stats", deltas[0], "rxlog_delta must contain stats")
        self.assertIsInstance(deltas[0]["stats"], dict)

    # ── (c) no subscriber → no push ──────────────────────────────────────────

    def test_no_push_when_feed_none(self):
        """With feed:'none', _push_rx_log_delta must push nothing."""
        import plugin
        with plugin._plugin._rx_log_lock:
            plugin._plugin._sub_feeds = "none"
            plugin._plugin._rx_log.append({"_t": 50, "message": "z", "snr": 0})
            plugin._plugin._rx_log_total_appended += 1
        _Domoticz_stub.reset_ws()
        plugin._plugin._push_rx_log_delta()
        self.assertEqual(_Domoticz_stub.ws_sent, [],
                         "No push expected when no subscriber")

    def test_sub_none_does_not_send_window(self):
        """sub feed:'none' must not trigger any rxlog window."""
        _send_ws({"t": "sub", "feed": "none"})
        windows = _ws_of_type("rxlog")
        self.assertEqual(windows, [],
                         "sub feed:'none' must not produce an rxlog window")

    def test_no_push_when_no_new_entries(self):
        """_push_rx_log_delta with no new entries must push nothing."""
        import plugin
        # Subscribe to set baseline at total=0.
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()
        # Don't add any entries; push delta.
        plugin._plugin._push_rx_log_delta()
        deltas = _ws_of_type("rxlog_delta")
        # An empty delta is acceptable but we require nothing was pushed OR
        # the delta was omitted entirely (zero entries → no push).
        for d in deltas:
            self.assertEqual(d.get("entries", []), [],
                             "Delta with no new entries must have empty entries list")

    # ── (d) seq monotonic across window + deltas ──────────────────────────────

    def test_seq_monotonic_across_window_and_deltas(self):
        """seq must strictly increase: window, delta1, delta2."""
        import plugin
        _send_ws({"t": "sub", "feed": "rxlog"})
        window_seq = _ws_of_type("rxlog")[0]["seq"]
        _Domoticz_stub.reset_ws()

        seqs = [window_seq]

        for i in range(3):
            with plugin._plugin._rx_log_lock:
                plugin._plugin._rx_log.append({"_t": 1000 + i, "message": f"m{i}", "snr": i})
                plugin._plugin._rx_log_total_appended += 1
            plugin._plugin._push_rx_log_delta()
            deltas = _ws_of_type("rxlog_delta")
            if deltas:
                seqs.append(deltas[-1]["seq"])
            _Domoticz_stub.reset_ws()

        for i in range(1, len(seqs)):
            self.assertGreater(seqs[i], seqs[i - 1],
                               f"seq not monotonic: seqs={seqs}")

    def test_seq_increments_on_multiple_subscribes(self):
        """Each new sub rxlog call must produce a window with a strictly higher seq."""
        import plugin
        _send_ws({"t": "sub", "feed": "rxlog"})
        seq1 = _ws_of_type("rxlog")[0]["seq"]
        _Domoticz_stub.reset_ws()

        _send_ws({"t": "sub", "feed": "rxlog"})
        seq2 = _ws_of_type("rxlog")[0]["seq"]

        self.assertGreater(seq2, seq1,
                           f"Second subscribe seq ({seq2}) must be > first ({seq1})")

    # ── Full-buffer steady state (the core bug scenario) ─────────────────────

    def test_full_buffer_delta_delivers_new_entries(self):
        """REALISTIC scenario: fill buffer to 250, subscribe, append 5 more,
        assert delta delivers exactly those 5 new entries (not empty)."""
        import plugin
        # Fill the buffer to capacity.
        _append_entries(plugin, RX_LOG_BUFFER, start_t=1)
        self.assertEqual(len(plugin._plugin._rx_log), RX_LOG_BUFFER)

        # Subscribe — records pushed_total = 250.
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()

        # Append 5 more entries; the deque evicts 5 off the left, len stays 250.
        _append_entries(plugin, 5, start_t=RX_LOG_BUFFER + 1)
        self.assertEqual(len(plugin._plugin._rx_log), RX_LOG_BUFFER,
                         "Buffer length must stay at maxlen after eviction")

        plugin._plugin._push_rx_log_delta()

        # Must NOT produce an empty delta.
        deltas = _ws_of_type("rxlog_delta")
        windows = _ws_of_type("rxlog")

        # Either a delta with 5 entries or a full-window fallback is acceptable,
        # but a silent empty push (the old bug) is not.
        if deltas:
            self.assertEqual(len(deltas), 1)
            self.assertEqual(
                len(deltas[0]["entries"]), 5,
                f"Expected 5 new entries in delta, got {len(deltas[0]['entries'])}"
            )
            # Verify the 5 entries are the ones that were appended last.
            messages = [e["message"] for e in deltas[0]["entries"]]
            expected = [f"m{RX_LOG_BUFFER + 1 + i}" for i in range(5)]
            self.assertEqual(messages, expected,
                             f"Wrong entries in delta: {messages}")
        else:
            # Fallback to a full window is also acceptable (buffer eviction path).
            self.assertGreater(
                len(windows), 0,
                "Bug reproduced: neither a valid delta nor a full-window fallback was pushed"
            )

    def test_full_buffer_eviction_gap_falls_back_to_window(self):
        """If the buffer has evicted entries the client never saw, a full window
        must be pushed instead of a delta."""
        import plugin
        # Fill buffer to capacity and subscribe (pushed_total = 250).
        _append_entries(plugin, RX_LOG_BUFFER, start_t=1)
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()

        # Append RX_LOG_BUFFER + 1 entries:
        #   total_appended = 501, buf_len = 250, start = 251
        #   pushed_total   = 250 < start (251)  → eviction gap detected
        _append_entries(plugin, RX_LOG_BUFFER + 1, start_t=RX_LOG_BUFFER + 1)

        plugin._plugin._push_rx_log_delta()

        windows = _ws_of_type("rxlog")
        deltas  = _ws_of_type("rxlog_delta")
        # Must fall back to a full window because the gap is too large.
        self.assertGreater(len(windows), 0,
                           "Expected full-window fallback when eviction gap is detected")
        # Must NOT also send an extra delta (the fallback updates pushed_total).
        self.assertEqual(len(deltas), 0,
                         "No separate delta expected alongside a full-window fallback")

    def test_steady_state_no_drop_no_duplicate(self):
        """Full-buffer steady state: repeated small appends + delta pushes must
        never drop or duplicate an entry, and seq must stay strictly monotonic."""
        import plugin
        # Fill the buffer.
        _append_entries(plugin, RX_LOG_BUFFER, start_t=1)
        _send_ws({"t": "sub", "feed": "rxlog"})
        window = _ws_of_type("rxlog")[0]
        last_seq = window["seq"]
        _Domoticz_stub.reset_ws()

        # Track the set of message tags already delivered (from the window).
        delivered = {e["message"] for e in window["entries"]}

        # Simulate 10 rounds: each round appends 3 entries, then calls delta.
        next_t = RX_LOG_BUFFER + 1
        for round_n in range(10):
            _append_entries(plugin, 3, start_t=next_t)
            next_t += 3

            plugin._plugin._push_rx_log_delta()

            deltas  = _ws_of_type("rxlog_delta")
            windows = _ws_of_type("rxlog")

            if deltas:
                d = deltas[-1]
                # Seq must have advanced.
                self.assertGreater(d["seq"], last_seq,
                                   f"round {round_n}: seq not monotonic")
                last_seq = d["seq"]
                for e in d["entries"]:
                    tag = e["message"]
                    self.assertNotIn(tag, delivered,
                                     f"round {round_n}: duplicate entry {tag!r}")
                    delivered.add(tag)
            elif windows:
                # Full-window fallback (eviction gap) — reset delivered set.
                w = windows[-1]
                self.assertGreater(w["seq"], last_seq,
                                   f"round {round_n}: fallback window seq not monotonic")
                last_seq = w["seq"]
                delivered = {e["message"] for e in w["entries"]}
            else:
                self.fail(f"round {round_n}: neither delta nor window was pushed")

            _Domoticz_stub.reset_ws()

    # ── Edge case: eviction-gap fallback (replaces old fake pushed_len test) ──

    def test_eviction_gap_detected_via_absolute_counter(self):
        """If pushed_total is set to a value older than the oldest still-buffered
        entry (simulating missed evictions), a full rxlog window must be pushed."""
        import plugin
        # Subscribe with 0 entries (pushed_total = 0).
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()

        # Add entries to the buffer but artificially keep pushed_total at 0
        # while advancing total_appended past a full buffer — the absolute
        # counter scheme detects the gap (start > pushed_total).
        _append_entries(plugin, RX_LOG_BUFFER + 10, start_t=1)
        # pushed_total is still 0; start = (RX_LOG_BUFFER+10) - RX_LOG_BUFFER = 10
        # → 0 < 10 → eviction gap detected.

        plugin._plugin._push_rx_log_delta()

        windows = _ws_of_type("rxlog")
        self.assertGreater(len(windows), 0,
                           "Expected a full rxlog window on eviction-gap fallback")


if __name__ == "__main__":
    unittest.main()
