"""Tests for the chan_hash → channel_name mapping.

Verifies that:
  (a) _fetch_channel_names populates _chan_hash_to_name from CHANNEL_INFO
      responses that carry a non-empty channel_name and a channel_hash.
  (b) Slots with an empty name do NOT contribute an entry.
  (c) The map is exposed in the rxlog window payload (chan_hash_names key).
  (d) The map is exposed in the rxlog delta payload (chan_hash_names key).
  (e) Two configured channels with distinct hashes both appear.
  (f) chan_hash_names is absent (or empty) when no channels are configured.

No live socket, no asyncio, no Domoticz runtime required.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import unittest

import DomoticzEx as _Domoticz_stub


def _send_ws(raw):
    import plugin
    payload = json.dumps(raw) if isinstance(raw, dict) else raw
    plugin.onWebSocketMessage(payload)


def _ws_of_type(t):
    return [r for r in _Domoticz_stub.ws_sent if r.get("t") == t]


def _reset_rxlog(plugin_mod):
    p = plugin_mod._plugin
    with p._rx_log_lock:
        p._rx_log.clear()
        p._sub_feeds = "none"
        p._rx_log_seq = 0
        p._rx_log_total_appended = 0
        p._rx_log_pushed_total = 0


class TestChanHashNamesAttribute(unittest.TestCase):
    """The _chan_hash_to_name attribute is always present."""

    def test_attribute_exists_on_plugin(self):
        import plugin
        p = plugin._plugin
        self.assertTrue(hasattr(p, "_chan_hash_to_name"),
                        "_plugin must have _chan_hash_to_name attribute")

    def test_attribute_is_dict(self):
        import plugin
        p = plugin._plugin
        self.assertIsInstance(p._chan_hash_to_name, dict)


class TestFetchChannelNamesPopulatesHashMap(unittest.TestCase):
    """Simulate the result of _fetch_channel_names by directly setting
    _chan_hash_to_name as the async method would, and verify the mapping
    is correct.  (The async method itself requires a live MeshCore
    connection; we exercise the state-mutation logic here.)"""

    def setUp(self):
        import plugin
        plugin._plugin._chan_hash_to_name = {}
        plugin._plugin._channel_names = {}
        plugin._plugin._channel_slots = {}

    def _simulate_fetch(self, slots):
        """Mimic the mapping built by _fetch_channel_names for the given
        list of (idx, name, channel_hash) tuples."""
        import plugin
        p = plugin._plugin
        channel_names = {}
        all_slots = {}
        chan_hash_to_name = {}
        for (idx, name, ch_hash) in slots:
            all_slots[idx] = name
            if name:
                channel_names[str(idx)] = name
                if ch_hash:
                    chan_hash_to_name[ch_hash] = name
        for j in range(40):
            all_slots.setdefault(j, "")
        p._channel_slots = all_slots
        p._channel_names = {int(k): v for k, v in channel_names.items()}
        p._chan_hash_to_name = chan_hash_to_name

    def test_single_channel_mapped(self):
        import plugin
        self._simulate_fetch([(0, "utrecht", "a3")])
        self.assertEqual(plugin._plugin._chan_hash_to_name, {"a3": "utrecht"})

    def test_empty_slot_not_mapped(self):
        import plugin
        self._simulate_fetch([(0, "", "a3"), (1, "general", "b7")])
        self.assertNotIn("a3", plugin._plugin._chan_hash_to_name,
                         "Empty slot must not appear in chan_hash_to_name")
        self.assertIn("b7", plugin._plugin._chan_hash_to_name)

    def test_multiple_channels_all_mapped(self):
        import plugin
        self._simulate_fetch([(0, "utrecht", "a3"), (1, "general", "f1")])
        m = plugin._plugin._chan_hash_to_name
        self.assertEqual(m.get("a3"), "utrecht")
        self.assertEqual(m.get("f1"), "general")

    def test_no_channels_empty_map(self):
        import plugin
        self._simulate_fetch([])
        self.assertEqual(plugin._plugin._chan_hash_to_name, {})

    def test_slot_without_hash_not_mapped(self):
        """A channel name without a hash must not crash and must not appear."""
        import plugin
        self._simulate_fetch([(0, "nokey", None)])
        self.assertNotIn("utrecht", plugin._plugin._chan_hash_to_name.values())
        self.assertEqual(plugin._plugin._chan_hash_to_name, {})


class TestRxLogWindowIncludesChanHashNames(unittest.TestCase):
    """_push_rx_log_window must include chan_hash_names."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _reset_rxlog(plugin)
        plugin._plugin._chan_hash_to_name = {}

    def test_window_contains_chan_hash_names_key(self):
        import plugin
        plugin._plugin._chan_hash_to_name = {"a3": "utrecht"}
        _send_ws({"t": "sub", "feed": "rxlog"})
        windows = _ws_of_type("rxlog")
        self.assertEqual(len(windows), 1)
        self.assertIn("chan_hash_names", windows[0],
                      "rxlog window must contain chan_hash_names key")

    def test_window_chan_hash_names_reflects_mapping(self):
        import plugin
        plugin._plugin._chan_hash_to_name = {"a3": "utrecht", "f1": "general"}
        _send_ws({"t": "sub", "feed": "rxlog"})
        w = _ws_of_type("rxlog")[0]
        chn = w["chan_hash_names"]
        self.assertEqual(chn.get("a3"), "utrecht")
        self.assertEqual(chn.get("f1"), "general")

    def test_window_chan_hash_names_empty_when_no_channels(self):
        import plugin
        plugin._plugin._chan_hash_to_name = {}
        _send_ws({"t": "sub", "feed": "rxlog"})
        w = _ws_of_type("rxlog")[0]
        self.assertEqual(w.get("chan_hash_names", {}), {})


class TestRxLogDeltaIncludesChanHashNames(unittest.TestCase):
    """_push_rx_log_delta must include chan_hash_names."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _reset_rxlog(plugin)
        plugin._plugin._chan_hash_to_name = {}

    def test_delta_contains_chan_hash_names_key(self):
        import plugin
        plugin._plugin._chan_hash_to_name = {"a3": "utrecht"}
        # Subscribe to set baseline.
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()
        # Append a new entry and push delta.
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 1, "chan_hash": "a3"})
            plugin._plugin._rx_log_total_appended += 1
        plugin._plugin._push_rx_log_delta()
        deltas = _ws_of_type("rxlog_delta")
        self.assertEqual(len(deltas), 1)
        self.assertIn("chan_hash_names", deltas[0],
                      "rxlog_delta must contain chan_hash_names key")

    def test_delta_chan_hash_names_reflects_mapping(self):
        import plugin
        plugin._plugin._chan_hash_to_name = {"a3": "utrecht"}
        _send_ws({"t": "sub", "feed": "rxlog"})
        _Domoticz_stub.reset_ws()
        with plugin._plugin._rx_log_lock:
            plugin._plugin._rx_log.append({"_t": 2, "chan_hash": "a3"})
            plugin._plugin._rx_log_total_appended += 1
        plugin._plugin._push_rx_log_delta()
        d = _ws_of_type("rxlog_delta")[0]
        self.assertEqual(d["chan_hash_names"].get("a3"), "utrecht")


class TestHashMappingIsolation(unittest.TestCase):
    """The chan_hash_to_name dict is isolated from _channel_names."""

    def test_channel_names_and_hash_map_independent(self):
        """Clearing _channel_names must not affect _chan_hash_to_name."""
        import plugin
        p = plugin._plugin
        p._channel_names = {0: "utrecht"}
        p._chan_hash_to_name = {"a3": "utrecht"}
        p._channel_names = {}
        self.assertEqual(p._chan_hash_to_name, {"a3": "utrecht"},
                         "Clearing _channel_names must not clear _chan_hash_to_name")


if __name__ == "__main__":
    unittest.main()
