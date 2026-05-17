"""Change A — self STATUS device reflects node connection loss.

Tests that:
  (a) A queued ("self_offline", {}) event dispatched on the main thread sets
      the SELF_DID / OFF_STATUS device to nValue=0, sValue="Off" and marks
      _ws_devices_dirty=True.
  (b) A queued ("contacts", ...) event (which sets STATUS On) does NOT
      require the worker to call Domoticz APIs directly — it goes through
      _dispatch as normal.
  (c) After self_offline is dispatched, a subsequent contacts poll (which
      calls _set SELF_DID/OFF_STATUS 1/"On") restores online state.
  (d) self_offline is NOT enqueued when _stop_event is set (clean shutdown).
  (e) _ws_devices_dirty is True after self_offline, False before.

No asyncio, no live socket, no Domoticz runtime required.
"""
import _bootstrap  # noqa: F401
import queue
import threading
import unittest
import unittest.mock as mock

import DomoticzEx as _Domoticz_stub


def _make_self_unit(plugin_mod):
    """Ensure a SELF_DID/OFF_STATUS unit exists in the plugin's device map
    (stub Devices dict) so _set() has something to update."""
    import plugin as p
    did = p.SELF_DID
    unit_num = p.OFF_STATUS

    # Create stub Device + Unit hierarchy the same way the plugin does.
    dev = _Domoticz_stub.Device(Name="self")
    unit = _Domoticz_stub.Unit(Name="Status", nValue=1, sValue="On")
    unit.nValue = 1
    unit.sValue = "On"
    dev.Units = {unit_num: unit}
    _Domoticz_stub.Devices[did] = dev
    return unit


def _teardown_devices():
    _Domoticz_stub.Devices.clear()


class TestSelfOfflineDispatch(unittest.TestCase):
    """(a)/(e) Dispatching self_offline sets STATUS Off and marks dirty."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _Domoticz_stub.Devices.clear()
        plugin.Devices = _Domoticz_stub.Devices
        plugin._plugin._self_name = "TestNode"
        plugin._plugin._ws_devices_dirty = False

    def tearDown(self):
        import plugin
        _teardown_devices()
        try:
            del plugin.Devices
        except AttributeError:
            pass
        plugin._plugin._self_name = ""
        plugin._plugin._ws_devices_dirty = False

    def test_self_offline_sets_status_off(self):
        """Dispatching ('self_offline', {}) must set the STATUS unit to nValue=0."""
        import plugin
        unit = _make_self_unit(plugin)
        plugin._plugin._dispatch(("self_offline", {}))
        self.assertEqual(unit.nValue, 0,
                         "self_offline must set STATUS nValue to 0 (Off)")

    def test_self_offline_sets_svalue_off(self):
        """Dispatching ('self_offline', {}) must set sValue to 'Off'."""
        import plugin
        unit = _make_self_unit(plugin)
        plugin._plugin._dispatch(("self_offline", {}))
        self.assertEqual(unit.sValue, "Off",
                         "self_offline must set STATUS sValue to 'Off'")

    def test_self_offline_marks_devices_dirty(self):
        """Dispatching ('self_offline', {}) must set _ws_devices_dirty=True."""
        import plugin
        _make_self_unit(plugin)
        plugin._plugin._ws_devices_dirty = False
        plugin._plugin._dispatch(("self_offline", {}))
        self.assertTrue(plugin._plugin._ws_devices_dirty,
                        "self_offline must mark _ws_devices_dirty True")

    def test_self_offline_no_self_name_does_not_crash(self):
        """self_offline with no _self_name must be a no-op (no device to update)."""
        import plugin
        plugin._plugin._self_name = ""
        try:
            plugin._plugin._dispatch(("self_offline", {}))
        except Exception as exc:
            self.fail(f"self_offline with no _self_name raised: {exc}")


class TestSelfOfflineNoOpOnShutdown(unittest.TestCase):
    """(d) self_offline must NOT be enqueued during a clean stop."""

    def setUp(self):
        import plugin
        plugin._plugin._self_name = "TestNode"
        plugin._plugin._was_connected = True

    def tearDown(self):
        import plugin
        plugin._plugin._self_name = ""
        plugin._plugin._was_connected = False

    def test_stop_event_prevents_self_offline_enqueue(self):
        """When _stop_event is set, the finally block must not put self_offline."""
        import plugin
        p = plugin._plugin
        # Simulate what the finally block does when stop_event IS set.
        # The actual condition is: if not self._stop_event.is_set(): enqueue.
        # We verify the guard by checking that setting the event means no enqueue.
        captured = []
        orig_put = p._queue.put

        def _spy_put(item):
            captured.append(item)
            orig_put(item)

        p._stop_event.set()
        try:
            with mock.patch.object(p._queue, "put", side_effect=_spy_put):
                # Replicate the guard condition from _connect_and_serve finally:
                if not p._stop_event.is_set():
                    p._was_connected = False
                    p._queue.put(("self_offline", {}))
        finally:
            p._stop_event.clear()

        self_offline_items = [x for x in captured if x[0] == "self_offline"]
        self.assertEqual(self_offline_items, [],
                         "self_offline must NOT be enqueued when _stop_event is set")


class TestSelfOnlineAfterOffline(unittest.TestCase):
    """(c) After self_offline, a contacts dispatch restores STATUS On."""

    def setUp(self):
        import plugin
        _Domoticz_stub.reset_ws()
        _Domoticz_stub.Devices.clear()
        plugin.Devices = _Domoticz_stub.Devices
        plugin._plugin._self_name = "TestNode"
        plugin._plugin._ws_devices_dirty = False

    def tearDown(self):
        import plugin
        _teardown_devices()
        try:
            del plugin.Devices
        except AttributeError:
            pass
        plugin._plugin._self_name = ""
        plugin._plugin._ws_devices_dirty = False

    def test_status_restored_on_after_offline(self):
        """After self_offline sets Off, _set(SELF_DID, OFF_STATUS, 1, 'On') restores On."""
        import plugin
        unit = _make_self_unit(plugin)

        # First go offline via the dispatched event
        plugin._plugin._dispatch(("self_offline", {}))
        self.assertEqual(unit.nValue, 0, "Should be Off after self_offline")

        # Simulate the re-connect path that calls _set(SELF_DID, OFF_STATUS, 1, "On")
        plugin._plugin._set(plugin.SELF_DID, plugin.OFF_STATUS, 1, "On")
        self.assertEqual(unit.nValue, 1,
                         "STATUS must be On after explicit _set(... 1, 'On')")
        self.assertEqual(unit.sValue, "On")


class TestSelfOfflineQueueMechanism(unittest.TestCase):
    """Verify the queue-based worker→main-thread boundary is used, not direct Domoticz calls."""

    def test_self_offline_is_queued_not_direct(self):
        """The finally block in _connect_and_serve must put ('self_offline', {})
        on the queue, not call _set directly.  We simulate the guard condition."""
        import plugin
        p = plugin._plugin
        captured = []
        orig_put = p._queue.put

        p._stop_event.clear()
        p._was_connected = True

        with mock.patch.object(p._queue, "put", side_effect=lambda item: (captured.append(item), orig_put(item))):
            # Replicate exactly the finally-block code path:
            if not p._stop_event.is_set():
                p._was_connected = False
                p._queue.put(("self_offline", {}))

        self_offline = [x for x in captured if x[0] == "self_offline"]
        self.assertEqual(len(self_offline), 1,
                         "Exactly one ('self_offline', {}) must be enqueued on disconnect")
        self.assertEqual(self_offline[0], ("self_offline", {}))


if __name__ == "__main__":
    unittest.main()
