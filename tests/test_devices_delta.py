"""F7 — device-map delta tests.

Exercises the delta-push behaviour introduced in F7:

  (a) No baseline → full t:'devices' push with 'deviceMap' and 'seq'.
  (b) After a baseline, only changed/added nodes appear in t:'devices_delta'.
  (c) Removed nodes appear in delta['removed'], unchanged nodes are absent.
  (d) Changed scalar fields appear in delta['scalars'].
  (e) 'inbox_value' scalar change (the inbox prepend trigger) arrives in delta.
  (f) seq increments monotonically: full → delta → delta.
  (g) Identical map → no push (nothing changed).
  (h) t:'snapshot' sets _last_pushed_device_map and deviceSeq.
  (i) t:'resync' feed:'devices' clears baseline → next flush is full.
  (j) Gap in seq causes a resync request from the frontend (plugin side: resync
      clears baseline so next push is full).

No live socket, no asyncio, no Domoticz runtime required.
"""
import _bootstrap  # noqa: F401
import copy
import json
import unittest

import DomoticzEx as _Domoticz_stub


def _send_ws(raw):
    import plugin
    payload = json.dumps(raw) if isinstance(raw, dict) else raw
    plugin.onWebSocketMessage(payload)


def _ws_of_type(t):
    return [r for r in _Domoticz_stub.ws_sent if r.get("t") == t]


def _reset_f7(plugin_mod):
    """Reset F7 device-map delta state to a clean slate."""
    p = plugin_mod._plugin
    with p._rx_log_lock:
        p._last_pushed_device_map = None
        p._device_seq = 0


def _inject_devices(plugin_mod):
    plugin_mod.Devices = _Domoticz_stub.Devices


def _eject_devices(plugin_mod):
    try:
        del plugin_mod.Devices
    except AttributeError:
        pass


class TestDevicesDeltaFull(unittest.TestCase):
    """(a) No baseline → full t:'devices' push."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _reset_f7(plugin)

    def test_no_baseline_sends_full_devices(self):
        """With no baseline, _push_devices_feed must send t:'devices' (full)."""
        import plugin
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        msgs = _ws_of_type("devices")
        self.assertEqual(len(msgs), 1, f"Expected 1 t:'devices', got {len(msgs)}")

    def test_full_push_carries_device_map(self):
        """Full push must contain 'deviceMap'."""
        import plugin
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        msg = _ws_of_type("devices")[0]
        self.assertIn("deviceMap", msg, "t:'devices' must contain 'deviceMap'")

    def test_full_push_carries_seq(self):
        """Full push must contain a positive integer 'seq'."""
        import plugin
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        msg = _ws_of_type("devices")[0]
        self.assertIn("seq", msg, "t:'devices' must contain 'seq'")
        self.assertIsInstance(msg["seq"], int)
        self.assertGreater(msg["seq"], 0)

    def test_full_push_sets_baseline(self):
        """After a full push, _last_pushed_device_map must be set."""
        import plugin
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        with plugin._plugin._rx_log_lock:
            baseline = plugin._plugin._last_pushed_device_map
        self.assertIsNotNone(baseline, "_last_pushed_device_map must be set after full push")
        self.assertIsInstance(baseline, dict)


class TestDevicesDeltaIncremental(unittest.TestCase):
    """(b)/(c)/(d)/(e)/(f) — incremental delta behaviour."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _reset_f7(plugin)
        # Establish a baseline with a first full push.
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        _Domoticz_stub.reset_ws()

    def _set_baseline(self, plugin_mod, dm):
        """Override the stored baseline with an arbitrary dict."""
        with plugin_mod._plugin._rx_log_lock:
            plugin_mod._plugin._last_pushed_device_map = copy.deepcopy(dm)

    def test_unchanged_map_produces_no_push(self):
        """(g) If the device map has not changed, no message is sent."""
        import plugin
        # The baseline was just set from _build_device_map_payload().
        # Calling _push_devices_feed again with the same state must produce
        # no push (or a delta with empty diff that is suppressed).
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        all_msgs = _Domoticz_stub.ws_sent
        self.assertEqual(all_msgs, [],
                         f"No push expected when device map is unchanged, got: {all_msgs}")

    def test_changed_node_produces_delta(self):
        """(b) A node whose data changed must appear in t:'devices_delta'.changed."""
        import plugin
        # Inject a synthetic baseline with one node.
        baseline = {
            "nodes": {"Alpha": {"snr": 5, "lastseen": "1m ago"}},
            "self": "Alpha",
        }
        self._set_baseline(plugin, baseline)

        # Patch _build_device_map_payload to return a map with the node changed.
        def _patched_build():
            return {
                "nodes": {"Alpha": {"snr": 9, "lastseen": "now"}},
                "self": "Alpha",
            }

        import unittest.mock as mock
        _inject_devices(plugin)
        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1, f"Expected 1 devices_delta, got {len(deltas)}")
        d = deltas[0]
        self.assertIn("Alpha", d.get("changed", {}),
                      "Changed node 'Alpha' must appear in delta['changed']")

    def test_added_node_in_delta_changed(self):
        """A new node (not in baseline) must appear in delta['changed']."""
        import plugin
        import unittest.mock as mock
        baseline = {"nodes": {"Alpha": {"snr": 5}}, "self": "Alpha"}
        self._set_baseline(plugin, baseline)

        def _patched_build():
            return {
                "nodes": {
                    "Alpha": {"snr": 5},   # unchanged
                    "Beta":  {"snr": 3},   # new
                },
                "self": "Alpha",
            }

        _inject_devices(plugin)
        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1)
        d = deltas[0]
        self.assertIn("Beta", d.get("changed", {}),
                      "New node 'Beta' must appear in delta['changed']")
        self.assertNotIn("Alpha", d.get("changed", {}),
                         "Unchanged node 'Alpha' must NOT appear in delta['changed']")

    def test_removed_node_in_delta_removed(self):
        """(c) A node present in baseline but absent from new map must be in delta['removed']."""
        import plugin
        import unittest.mock as mock
        baseline = {
            "nodes": {"Alpha": {"snr": 5}, "Beta": {"snr": 3}},
            "self": "Alpha",
        }
        self._set_baseline(plugin, baseline)

        def _patched_build():
            return {"nodes": {"Alpha": {"snr": 5}}, "self": "Alpha"}

        _inject_devices(plugin)
        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1)
        d = deltas[0]
        self.assertIn("Beta", d.get("removed", []),
                      "Removed node 'Beta' must appear in delta['removed']")
        self.assertNotIn("Alpha", d.get("removed", []),
                         "Present node 'Alpha' must NOT be in delta['removed']")

    def test_unchanged_node_absent_from_delta(self):
        """An unchanged node must not appear in delta['changed']."""
        import plugin
        import unittest.mock as mock
        baseline = {
            "nodes": {"Alpha": {"snr": 5}, "Beta": {"snr": 3}},
            "self": "Alpha",
        }
        self._set_baseline(plugin, baseline)

        def _patched_build():
            return {
                "nodes": {
                    "Alpha": {"snr": 5},   # unchanged
                    "Beta":  {"snr": 9},   # changed
                },
                "self": "Alpha",
            }

        _inject_devices(plugin)
        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1)
        d = deltas[0]
        self.assertNotIn("Alpha", d.get("changed", {}),
                         "Unchanged node must not appear in delta['changed']")
        self.assertIn("Beta", d.get("changed", {}),
                      "Changed node must appear in delta['changed']")

    def test_scalar_change_in_delta_scalars(self):
        """(d) A changed top-level scalar must appear in delta['scalars']."""
        import plugin
        import unittest.mock as mock
        baseline = {
            "nodes": {"Alpha": {"snr": 5}},
            "self": "Alpha",
            "inbox": 42,
        }
        self._set_baseline(plugin, baseline)

        def _patched_build():
            return {
                "nodes": {"Alpha": {"snr": 5}},
                "self": "Alpha",
                "inbox": 99,  # changed
            }

        _inject_devices(plugin)
        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1)
        d = deltas[0]
        self.assertIn("inbox", d.get("scalars", {}),
                      "'inbox' scalar change must appear in delta['scalars']")
        self.assertEqual(d["scalars"]["inbox"], 99)

    def test_inbox_value_scalar_change_in_delta(self):
        """(e) inbox_value change (inbox prepend trigger) must appear in delta['scalars']."""
        import plugin
        import unittest.mock as mock
        baseline = {
            "nodes": {},
            "self": "Alpha",
            "inbox_value": "old|msg1",
        }
        self._set_baseline(plugin, baseline)

        def _patched_build():
            return {
                "nodes": {},
                "self": "Alpha",
                "inbox_value": "new|msg2",  # changed
            }

        _inject_devices(plugin)
        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1)
        d = deltas[0]
        self.assertIn("inbox_value", d.get("scalars", {}),
                      "'inbox_value' change must appear in delta['scalars']")
        self.assertEqual(d["scalars"]["inbox_value"], "new|msg2")

    def test_seq_increments_full_then_delta(self):
        """(f) seq increments monotonically: full push, then delta."""
        import plugin
        import unittest.mock as mock
        # Get the seq from the first full push (done in setUp).
        # We need to get that seq — reset and do fresh full push.
        _reset_f7(plugin)
        _inject_devices(plugin)
        plugin._plugin._push_devices_feed()
        full_msgs = _ws_of_type("devices")
        self.assertEqual(len(full_msgs), 1)
        full_seq = full_msgs[0]["seq"]
        _Domoticz_stub.reset_ws()

        # Now set a synthetic baseline that differs slightly from what
        # _build_device_map_payload returns (change one scalar).
        with plugin._plugin._rx_log_lock:
            baseline = copy.deepcopy(plugin._plugin._last_pushed_device_map)
            baseline["_test_scalar_"] = "before"
            plugin._plugin._last_pushed_device_map = baseline

        def _patched_build():
            dm = copy.deepcopy(baseline)
            dm["_test_scalar_"] = "after"
            return dm

        with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched_build):
            plugin._plugin._push_devices_feed()
        _eject_devices(plugin)

        deltas = _ws_of_type("devices_delta")
        self.assertEqual(len(deltas), 1, f"Expected 1 delta, got {len(deltas)}")
        delta_seq = deltas[0]["seq"]
        self.assertEqual(delta_seq, full_seq + 1,
                         f"Delta seq ({delta_seq}) must be full_seq+1 ({full_seq+1})")

    def test_delta_seq_monotonic_over_multiple_deltas(self):
        """seq must strictly increase across multiple consecutive deltas."""
        import plugin
        import unittest.mock as mock
        _reset_f7(plugin)
        _inject_devices(plugin)
        plugin._plugin._push_devices_feed()
        full_seq = _ws_of_type("devices")[0]["seq"]
        _Domoticz_stub.reset_ws()

        seqs = [full_seq]
        for i in range(3):
            # Build a baseline with a unique scalar, then return a slightly different map.
            with plugin._plugin._rx_log_lock:
                baseline = copy.deepcopy(plugin._plugin._last_pushed_device_map)
                baseline["_iter_"] = i
                plugin._plugin._last_pushed_device_map = baseline

            def _patched(i=i, baseline=baseline):
                dm = copy.deepcopy(baseline)
                dm["_iter_"] = i + 100
                return dm

            with mock.patch.object(plugin._plugin, "_build_device_map_payload", _patched):
                plugin._plugin._push_devices_feed()
            _eject_devices(plugin)

            pushed = _ws_of_type("devices_delta") or _ws_of_type("devices")
            self.assertEqual(len(pushed), 1, f"iter {i}: expected 1 push")
            seqs.append(pushed[0]["seq"])
            _Domoticz_stub.reset_ws()

        for j in range(1, len(seqs)):
            self.assertGreater(seqs[j], seqs[j - 1],
                               f"seq not monotonic at index {j}: {seqs}")
        _eject_devices(plugin)


class TestDevicesDeltaSnapshot(unittest.TestCase):
    """(h) Snapshot path sets baseline and carries deviceSeq."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _reset_f7(plugin)

    def _inject_devices(self, plugin_mod):
        plugin_mod.Devices = _Domoticz_stub.Devices

    def _eject_devices(self, plugin_mod):
        try:
            del plugin_mod.Devices
        except AttributeError:
            pass

    def test_snapshot_carries_device_seq(self):
        """t:'snapshot' must include 'deviceSeq'."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)
        snaps = [r for r in _Domoticz_stub.ws_sent if r.get("t") == "snapshot"]
        self.assertEqual(len(snaps), 1)
        self.assertIn("deviceSeq", snaps[0],
                      "snapshot must carry 'deviceSeq' for the F7 frontend baseline")

    def test_snapshot_sets_baseline(self):
        """After t:'hello', _last_pushed_device_map must be set."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)
        with plugin._plugin._rx_log_lock:
            baseline = plugin._plugin._last_pushed_device_map
        self.assertIsNotNone(baseline,
                             "_last_pushed_device_map must be set after snapshot")

    def test_snapshot_device_seq_matches_plugin_seq(self):
        """deviceSeq in snapshot must match _device_seq at the time of the push."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)
        snap = next(r for r in _Domoticz_stub.ws_sent if r.get("t") == "snapshot")
        with plugin._plugin._rx_log_lock:
            plugin_seq = plugin._plugin._device_seq
        self.assertEqual(snap["deviceSeq"], plugin_seq,
                         "snapshot deviceSeq must match plugin._device_seq")


class TestDevicesDeltaResync(unittest.TestCase):
    """(i)/(j) Resync request clears baseline → next flush is full."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        # First establish a baseline via a full push.
        _reset_f7(plugin)
        _inject_devices(plugin)
        try:
            plugin._plugin._push_devices_feed()
        finally:
            _eject_devices(plugin)
        _Domoticz_stub.reset_ws()

    def test_resync_clears_baseline(self):
        """t:'resync' feed:'devices' must set _last_pushed_device_map to None."""
        import plugin
        with plugin._plugin._rx_log_lock:
            self.assertIsNotNone(plugin._plugin._last_pushed_device_map,
                                 "Baseline must be set before resync (test setup)")
        _send_ws({"t": "resync", "feed": "devices"})
        with plugin._plugin._rx_log_lock:
            baseline = plugin._plugin._last_pushed_device_map
        self.assertIsNone(baseline, "Resync must clear _last_pushed_device_map")

    def test_resync_sets_devices_dirty(self):
        """t:'resync' feed:'devices' must mark _ws_devices_dirty so the next flush
        sends a full push."""
        import plugin
        plugin._plugin._ws_devices_dirty = False
        _send_ws({"t": "resync", "feed": "devices"})
        self.assertTrue(plugin._plugin._ws_devices_dirty,
                        "_ws_devices_dirty must be True after resync")

    def test_after_resync_flush_sends_full(self):
        """After resync, the next _push_dirty_feeds must send t:'devices' (full)."""
        import plugin
        _send_ws({"t": "resync", "feed": "devices"})
        plugin._plugin._devices_last_push = 0.0
        _Domoticz_stub.reset_ws()
        _inject_devices(plugin)
        try:
            plugin._plugin._push_dirty_feeds()
        finally:
            _eject_devices(plugin)
        full_msgs = [r for r in _Domoticz_stub.ws_sent if r.get("t") == "devices"]
        self.assertEqual(len(full_msgs), 1,
                         "After resync, full t:'devices' must be sent")
        self.assertIn("deviceMap", full_msgs[0])

    def test_resync_unknown_feed_does_nothing(self):
        """t:'resync' with an unknown feed must not crash and must not clear devices baseline."""
        import plugin
        with plugin._plugin._rx_log_lock:
            before = plugin._plugin._last_pushed_device_map
        try:
            _send_ws({"t": "resync", "feed": "unknown_feed_xyz"})
        except Exception as exc:
            self.fail(f"resync with unknown feed raised: {exc}")
        with plugin._plugin._rx_log_lock:
            after = plugin._plugin._last_pushed_device_map
        self.assertIs(before, after,
                      "Resync with unknown feed must not modify devices baseline")


if __name__ == "__main__":
    unittest.main()
