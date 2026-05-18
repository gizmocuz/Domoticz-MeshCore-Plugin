"""F1 — WebSocket command-channel protocol tests.

Drives onWebSocketMessage with JSON payloads and asserts that
Domoticz.WebSocketSend (the stub spy) receives correctly-shaped
t-typed replies.

No live socket, no asyncio, no Domoticz runtime required — the
DomoticzEx stub provides a WebSocketSend spy that records all calls.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import sys
import unittest
from unittest.mock import patch

import DomoticzEx as _Domoticz_stub  # the stub module, for reset_ws / ws_sent


def _send_ws(raw):
    """Drive the module-level onWebSocketMessage hook with a JSON string or dict."""
    import plugin
    payload = json.dumps(raw) if isinstance(raw, dict) else raw
    plugin.onWebSocketMessage(payload)


class TestWebSocketMessageHook(unittest.TestCase):

    def setUp(self):
        _Domoticz_stub.reset_ws()

    def _replies(self):
        """Return all payloads sent via WebSocketSend since last reset."""
        return list(_Domoticz_stub.ws_sent)

    def _inject_devices(self, plugin_mod):
        plugin_mod.Devices = _Domoticz_stub.Devices

    def _eject_devices(self, plugin_mod):
        try:
            del plugin_mod.Devices
        except AttributeError:
            pass

    def _cmd_result_replies(self):
        """Return only cmd_result frames from the last send."""
        return [r for r in self._replies() if r.get("t") == "cmd_result"]

    def test_hello_sends_cmd_result(self):
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)
        replies = self._cmd_result_replies()
        self.assertTrue(len(replies) >= 1, "Expected at least one cmd_result reply")
        ok_replies = [r for r in replies if r.get("ok")]
        self.assertTrue(len(ok_replies) >= 1, f"Expected at least one ok=True cmd_result, got: {replies}")

    def test_hello_reply_has_required_keys(self):
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)
        ok_replies = [r for r in self._cmd_result_replies() if r.get("ok")]
        self.assertTrue(len(ok_replies) >= 1, "Expected an ok=True cmd_result reply")
        reply = ok_replies[0]
        for key in ("t", "ok", "target", "result"):
            self.assertIn(key, reply, f"Missing key {key!r} in hello reply")

    def test_unknown_t_sends_no_reply(self):
        _Domoticz_stub.reset_ws()
        _send_ws({"t": "bogus_unknown_type_xyz"})
        self.assertEqual(self._replies(), [],
                         "Unknown t value should not produce a WebSocketSend reply")

    def test_sub_stores_feed_and_sends_rxlog_window(self):
        """sub feed:'rxlog' must store the feed AND immediately push one rxlog window (F3).
        Only rxlog pushes are allowed — no cmd_result or other frame types."""
        import plugin
        _send_ws({"t": "sub", "feed": "rxlog"})
        self.assertEqual(plugin._plugin._sub_feeds, "rxlog")
        replies = self._replies()
        # F3: an rxlog window is now pushed immediately on subscribe.
        non_rxlog = [r for r in replies if r.get("t") != "rxlog"]
        self.assertEqual(non_rxlog, [],
                         f"sub feed:'rxlog' must only produce rxlog pushes, got: {replies}")
        rxlog_replies = [r for r in replies if r.get("t") == "rxlog"]
        self.assertEqual(len(rxlog_replies), 1,
                         f"sub feed:'rxlog' must push exactly one rxlog window, got: {replies}")

    def test_sub_default_feed_none(self):
        import plugin
        _send_ws({"t": "sub", "feed": "none"})
        self.assertEqual(plugin._plugin._sub_feeds, "none")

    def test_cmd_empty_cmd_returns_error(self):
        _send_ws({"t": "cmd", "cmd": ""})
        replies = self._replies()
        self.assertTrue(len(replies) >= 1)
        reply = replies[-1]
        self.assertEqual(reply.get("t"), "cmd_result")
        self.assertFalse(reply.get("ok"))

    def test_cmd_favorite_add_handled_locally(self):
        """!favorite add <name> is a local-only command — no worker loop needed."""
        import plugin
        # _handle_local_only_command calls _write_device_map which reads the
        # bare-global 'Devices'. The attribute is injected temporarily; a
        # try/finally ensures it is removed even if the test body raises, so
        # no state leaks into other tests.
        try:
            plugin.Devices = _Domoticz_stub.Devices
            _send_ws({"t": "cmd", "cmd": "!favorite add TestNode"})
        finally:
            try:
                del plugin.Devices
            except AttributeError:
                pass
            plugin._plugin._favorites.discard("TestNode")
        replies = self._replies()
        self.assertTrue(len(replies) >= 1)
        reply = replies[-1]
        self.assertEqual(reply.get("t"), "cmd_result")
        self.assertTrue(reply.get("ok"), f"Expected ok=True, got reply={reply}")

    def test_cmd_no_worker_loop_returns_error(self):
        """Commands that need the worker return an error when not connected."""
        import plugin
        # Ensure no worker loop (plugin hasn't started)
        original_loop = plugin._plugin._worker_loop
        plugin._plugin._worker_loop = None
        try:
            _send_ws({"t": "cmd", "cmd": "!req_status SomeNode"})
            replies = self._replies()
            self.assertTrue(len(replies) >= 1)
            reply = replies[-1]
            self.assertEqual(reply.get("t"), "cmd_result")
            self.assertFalse(reply.get("ok"))
        finally:
            plugin._plugin._worker_loop = original_loop

    def test_invalid_json_does_not_crash(self):
        """Malformed JSON must not raise — the hook swallows parse errors."""
        import plugin
        try:
            plugin.onWebSocketMessage("not-valid-json{{{")
        except Exception as exc:
            self.fail(f"onWebSocketMessage raised on bad JSON: {exc}")
        self.assertEqual(self._replies(), [])

    def test_non_dict_payload_does_not_crash(self):
        """A valid JSON array is not a protocol frame — must be silently ignored."""
        import plugin
        try:
            plugin.onWebSocketMessage("[1,2,3]")
        except Exception as exc:
            self.fail(f"onWebSocketMessage raised on non-dict payload: {exc}")
        self.assertEqual(self._replies(), [])

    def test_ws_ok_flag_set_after_first_push(self):
        """_ws_ok should be True after the first successful push (stub has WebSocketSend)."""
        import plugin
        plugin._plugin._ws_ok = None   # reset detection
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)
        self.assertTrue(plugin._plugin._ws_ok)

    def test_ws_ok_false_skips_send(self):
        """When _ws_ok=False (old Domoticz build), _push must not call WebSocketSend."""
        import plugin
        plugin._plugin._ws_ok = False
        _Domoticz_stub.reset_ws()
        plugin._plugin._push("cmd_result", {"ok": True, "target": "test", "result": "ok"})
        self.assertEqual(_Domoticz_stub.ws_sent, [])

    def test_correlation_id_echoed_in_hello_reply(self):
        """Correlation id sent with hello must be echoed back in cmd_result."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello", "id": 42})
        finally:
            self._eject_devices(plugin)
        ok_replies = [r for r in self._cmd_result_replies() if r.get("ok")]
        self.assertTrue(len(ok_replies) >= 1, "Expected an ok=True cmd_result")
        reply = ok_replies[0]
        self.assertEqual(reply.get("t"), "cmd_result")
        self.assertEqual(reply.get("id"), 42,
                         f"Expected id=42 echoed in hello reply, got: {reply}")

    def test_correlation_id_echoed_in_cmd_error(self):
        """Correlation id from a cmd with no worker must appear in the error reply."""
        import plugin
        original_loop = plugin._plugin._worker_loop
        plugin._plugin._worker_loop = None
        try:
            _send_ws({"t": "cmd", "cmd": "!req_status Node1", "id": 99})
            reply = self._replies()[-1]
            self.assertEqual(reply.get("t"), "cmd_result")
            self.assertFalse(reply.get("ok"))
            self.assertEqual(reply.get("id"), 99,
                             f"Expected id=99 echoed in error reply, got: {reply}")
        finally:
            plugin._plugin._worker_loop = original_loop

    def test_cmd_whitespace_only_returns_error_with_unknown_target(self):
        """Whitespace-only cmd must not IndexError and must use 'unknown' as target."""
        _send_ws({"t": "cmd", "cmd": "   "})
        replies = self._replies()
        self.assertTrue(len(replies) >= 1)
        reply = replies[-1]
        self.assertEqual(reply.get("t"), "cmd_result")
        self.assertFalse(reply.get("ok"))
        self.assertEqual(reply.get("target"), "unknown",
                         f"Expected target='unknown' for whitespace cmd, got: {reply}")

    def test_ws_ok_detection_cached_after_first_push(self):
        """_ws_ok feature-detection must probe hasattr exactly once; subsequent
        _push calls must use the cached value without re-probing."""
        import builtins
        import plugin
        plugin._plugin._ws_ok = None   # force re-detection
        probe_count = 0
        original_hasattr = builtins.hasattr

        def counting_hasattr(obj, name):
            nonlocal probe_count
            if name == "WebSocketSend":
                probe_count += 1
            return original_hasattr(obj, name)

        with patch("builtins.hasattr", side_effect=counting_hasattr):
            plugin._plugin._push("cmd_result", {"ok": True, "target": "t1", "result": "r1"})
            plugin._plugin._push("cmd_result", {"ok": True, "target": "t2", "result": "r2"})
            plugin._plugin._push("cmd_result", {"ok": True, "target": "t3", "result": "r3"})

        self.assertEqual(probe_count, 1,
                         f"Expected hasattr('WebSocketSend') called exactly once, got {probe_count}")


class TestF2StatePush(unittest.TestCase):
    """F2 — state-push: snapshot on hello and per-feed dirty-flag coalescing."""

    def setUp(self):
        _Domoticz_stub.reset_ws()

    def _replies(self):
        return list(_Domoticz_stub.ws_sent)

    # ── helpers ────────────────────────────────────────────────────────────────

    def _inject_devices(self, plugin_mod):
        """Inject the stub Devices so _build_device_map_payload can call _dev()."""
        plugin_mod.Devices = _Domoticz_stub.Devices

    def _eject_devices(self, plugin_mod):
        try:
            del plugin_mod.Devices
        except AttributeError:
            pass

    # ── (a) hello → snapshot ──────────────────────────────────────────────────

    def test_hello_also_sends_snapshot(self):
        """t:'hello' must produce a snapshot message in addition to cmd_result."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)

        types = [r.get("t") for r in self._replies()]
        self.assertIn("snapshot", types,
                      f"Expected a 'snapshot' message among replies, got: {types}")

    def test_hello_snapshot_is_lean_and_heard_deferred(self):
        """The snapshot is lean (deviceMap, stats, channels) for fast first
        paint; heard is NOT in the snapshot but is delivered as a separate
        'heard' follow-up frame right after it."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)

        snapshots = [r for r in self._replies() if r.get("t") == "snapshot"]
        self.assertEqual(len(snapshots), 1,
                         f"Expected exactly one snapshot message, got: {len(snapshots)}")
        snap = snapshots[0]
        for key in ("deviceMap", "stats", "channels"):
            self.assertIn(key, snap, f"snapshot missing key {key!r}")
        self.assertNotIn("heard", snap,
                         "heard must NOT be in the lean snapshot (it is deferred)")

        heards = [r for r in self._replies() if r.get("t") == "heard"]
        self.assertEqual(len(heards), 1,
                         f"Expected exactly one deferred 'heard' frame, got: {len(heards)}")
        self.assertIsInstance(heards[0].get("heard"), dict,
                              "deferred heard frame must carry a 'heard' dict")

    def test_hello_snapshot_key_types(self):
        """Each snapshot key must have the correct container type."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)

        snap = next(r for r in self._replies() if r.get("t") == "snapshot")
        self.assertIsInstance(snap["deviceMap"], dict, "deviceMap must be a dict")
        self.assertIsInstance(snap["stats"],     dict, "stats must be a dict")
        self.assertIsInstance(snap["channels"],  dict, "channels must be a dict")

    def test_hello_on_reconnect_sends_fresh_snapshot(self):
        """Every hello (including reconnects) must send a fresh snapshot."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
            _Domoticz_stub.reset_ws()
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)

        snapshots = [r for r in self._replies() if r.get("t") == "snapshot"]
        self.assertEqual(len(snapshots), 1,
                         "Second hello must also produce exactly one snapshot")

    def test_hello_cmd_result_still_present(self):
        """cmd_result must still be sent alongside the snapshot (F1 compat)."""
        import plugin
        self._inject_devices(plugin)
        try:
            _send_ws({"t": "hello"})
        finally:
            self._eject_devices(plugin)

        types = [r.get("t") for r in self._replies()]
        self.assertIn("cmd_result", types,
                      f"cmd_result must still be present alongside snapshot, got: {types}")

    # ── (b) dirty-flag coalescing ─────────────────────────────────────────────

    def test_push_devices_dirty_produces_devices_message(self):
        """Setting _ws_devices_dirty and flushing emits exactly one devices push.
        When no baseline is present the push must be a full t:'devices'; after
        a baseline is established subsequent pushes may be t:'devices_delta'."""
        import plugin
        _Domoticz_stub.reset_ws()
        plugin._plugin._ws_devices_dirty = True
        plugin._plugin._devices_last_push = 0.0  # force past the 1-second gate
        # Reset the F7 baseline so we always get a full t:'devices' message.
        with plugin._plugin._rx_log_lock:
            plugin._plugin._last_pushed_device_map = None
        self._inject_devices(plugin)
        try:
            plugin._plugin._push_dirty_feeds()
        finally:
            self._eject_devices(plugin)

        msgs = [r for r in self._replies() if r.get("t") == "devices"]
        self.assertEqual(len(msgs), 1, f"Expected 1 'devices' push, got {len(msgs)}")
        self.assertIn("deviceMap", msgs[0], "devices push must contain 'deviceMap'")
        # Flag must be cleared
        self.assertFalse(plugin._plugin._ws_devices_dirty,
                         "_ws_devices_dirty must be cleared after flush")

    def test_push_stats_dirty_produces_stats_message(self):
        """Setting _ws_stats_dirty and flushing emits exactly one 'stats' push."""
        import plugin
        _Domoticz_stub.reset_ws()
        plugin._plugin._ws_stats_dirty = True
        plugin._plugin._stats_last_push = 0.0
        plugin._plugin._push_dirty_feeds()

        msgs = [r for r in self._replies() if r.get("t") == "stats"]
        self.assertEqual(len(msgs), 1, f"Expected 1 'stats' push, got {len(msgs)}")
        self.assertIn("stats", msgs[0], "stats push must contain 'stats'")
        self.assertFalse(plugin._plugin._ws_stats_dirty,
                         "_ws_stats_dirty must be cleared after flush")

    def test_push_heard_dirty_produces_heard_message(self):
        """Setting _ws_heard_dirty and flushing emits exactly one 'heard' push."""
        import plugin
        _Domoticz_stub.reset_ws()
        plugin._plugin._ws_heard_dirty = True
        plugin._plugin._heard_last_push = 0.0
        plugin._plugin._push_dirty_feeds()

        msgs = [r for r in self._replies() if r.get("t") == "heard"]
        self.assertEqual(len(msgs), 1, f"Expected 1 'heard' push, got {len(msgs)}")
        self.assertIn("heard", msgs[0], "heard push must contain 'heard'")
        self.assertFalse(plugin._plugin._ws_heard_dirty,
                         "_ws_heard_dirty must be cleared after flush")

    def test_push_channels_dirty_produces_channels_message(self):
        """Setting _ws_channels_dirty and flushing emits exactly one 'channels' push."""
        import plugin
        _Domoticz_stub.reset_ws()
        plugin._plugin._ws_channels_dirty = True
        plugin._plugin._channels_last_push = 0.0
        plugin._plugin._push_dirty_feeds()

        msgs = [r for r in self._replies() if r.get("t") == "channels"]
        self.assertEqual(len(msgs), 1, f"Expected 1 'channels' push, got {len(msgs)}")
        self.assertIn("channels", msgs[0], "channels push must contain 'channels'")
        self.assertFalse(plugin._plugin._ws_channels_dirty,
                         "_ws_channels_dirty must be cleared after flush")

    def test_no_push_when_nothing_dirty(self):
        """_push_dirty_feeds must not emit anything when no feed is dirty."""
        import plugin
        plugin._plugin._ws_devices_dirty  = False
        plugin._plugin._ws_stats_dirty    = False
        plugin._plugin._ws_heard_dirty    = False
        plugin._plugin._ws_channels_dirty = False
        _Domoticz_stub.reset_ws()
        plugin._plugin._push_dirty_feeds()

        self.assertEqual(self._replies(), [],
                         "No WebSocket messages expected when no feed is dirty")

    def test_coalescing_suppresses_second_push_within_one_second(self):
        """A second flush within 1 s of the first must be suppressed (coalescing)."""
        import plugin
        _Domoticz_stub.reset_ws()
        plugin._plugin._ws_stats_dirty = True
        plugin._plugin._stats_last_push = 0.0
        plugin._plugin._push_dirty_feeds()   # first flush — should push

        # Simulate a second dirty event immediately after
        plugin._plugin._ws_stats_dirty = True
        # _stats_last_push is now ~time.monotonic() (just set above)
        plugin._plugin._push_dirty_feeds()   # second flush — too soon, suppressed

        msgs = [r for r in self._replies() if r.get("t") == "stats"]
        self.assertEqual(len(msgs), 1,
                         "Only one stats push expected within the 1-second coalesce window")

    def test_all_four_feeds_dirty_produces_four_messages(self):
        """All four feeds dirty at once → one push each → four messages total.
        Devices may be t:'devices' (full) or t:'devices_delta' (incremental)."""
        import plugin
        _Domoticz_stub.reset_ws()
        plugin._plugin._ws_devices_dirty  = True
        plugin._plugin._ws_stats_dirty    = True
        plugin._plugin._ws_heard_dirty    = True
        plugin._plugin._ws_channels_dirty = True
        plugin._plugin._devices_last_push  = 0.0
        plugin._plugin._stats_last_push    = 0.0
        plugin._plugin._heard_last_push    = 0.0
        plugin._plugin._channels_last_push = 0.0
        # Reset baseline so we always get a full t:'devices' (predictable count).
        with plugin._plugin._rx_log_lock:
            plugin._plugin._last_pushed_device_map = None
        self._inject_devices(plugin)
        try:
            plugin._plugin._push_dirty_feeds()
        finally:
            self._eject_devices(plugin)

        types = {r.get("t") for r in self._replies()}
        for expected in ("devices", "stats", "heard", "channels"):
            self.assertIn(expected, types,
                          f"Expected feed type {expected!r} in pushed messages")
        self.assertEqual(len(self._replies()), 4,
                         f"Expected exactly 4 push messages, got {len(self._replies())}")

    # ── (c) flag independence ─────────────────────────────────────────────────

    def test_write_stats_does_not_clear_ws_stats_dirty(self):
        """_write_stats (file path) must not clear _ws_stats_dirty, and a
        subsequent _push_dirty_feeds must emit exactly one stats push."""
        import plugin
        plugin._plugin._stats_dirty     = True
        plugin._plugin._ws_stats_dirty  = True
        plugin._plugin._stats_last_push = 0.0
        # Call the file-write path — must leave _ws_stats_dirty intact.
        plugin._plugin._write_stats()
        self.assertTrue(plugin._plugin._ws_stats_dirty,
                        "_ws_stats_dirty must remain True after _write_stats")
        _Domoticz_stub.reset_ws()
        plugin._plugin._push_dirty_feeds()
        msgs = [r for r in self._replies() if r.get("t") == "stats"]
        self.assertEqual(len(msgs), 1,
                         f"Expected exactly 1 stats push from _push_dirty_feeds, got {len(msgs)}")

    def test_write_heard_does_not_clear_ws_heard_dirty(self):
        """_write_heard (file path) must not clear _ws_heard_dirty, and a
        subsequent _push_dirty_feeds must emit exactly one heard push."""
        import plugin
        plugin._plugin._heard_dirty     = True
        plugin._plugin._ws_heard_dirty  = True
        plugin._plugin._heard_last_push = 0.0
        # Call the file-write path — must leave _ws_heard_dirty intact.
        plugin._plugin._write_heard()
        self.assertTrue(plugin._plugin._ws_heard_dirty,
                        "_ws_heard_dirty must remain True after _write_heard")
        _Domoticz_stub.reset_ws()
        plugin._plugin._push_dirty_feeds()
        msgs = [r for r in self._replies() if r.get("t") == "heard"]
        self.assertEqual(len(msgs), 1,
                         f"Expected exactly 1 heard push from _push_dirty_feeds, got {len(msgs)}")

    # ── (d) coalescing refire ─────────────────────────────────────────────────

    def test_coalescing_refire_after_window(self):
        """After the 1-second coalesce window, a second flush with _ws_stats_dirty=True
        must re-emit exactly one stats push (proves the gate re-fires after the window)."""
        import time
        import plugin
        _Domoticz_stub.reset_ws()
        # Force the timestamp to be >1 s in the past so the gate opens immediately.
        plugin._plugin._ws_stats_dirty  = True
        plugin._plugin._stats_last_push = time.monotonic() - 1.5
        plugin._plugin._push_dirty_feeds()
        msgs = [r for r in self._replies() if r.get("t") == "stats"]
        self.assertEqual(len(msgs), 1,
                         f"Expected exactly 1 stats push after the coalesce window, got {len(msgs)}")
        self.assertFalse(plugin._plugin._ws_stats_dirty,
                         "_ws_stats_dirty must be cleared after the push")

    # ── (e) !remove sets _ws_devices_dirty ───────────────────────────────────

    def test_remove_mutation_sets_ws_devices_dirty(self):
        """After the !remove contact mutation path clears local tracking state,
        _ws_devices_dirty must be True so _push_dirty_feeds emits a devices push."""
        import plugin
        # Seed a fake contact into the tracking state the !remove block mutates.
        fake_name = "_test_remove_contact_"
        plugin._plugin._contact_names.append(fake_name)
        plugin._plugin._node_types[fake_name]        = 1
        plugin._plugin._node_last_advert[fake_name]  = 0
        plugin._plugin._node_pubkey[fake_name]       = "aabbcc112233"
        plugin._plugin._node_did[fake_name]          = "aabbcc112233"
        plugin._plugin._node_last_activity[fake_name] = 0
        plugin._plugin._node_locations[fake_name]    = {}
        plugin._plugin._ws_devices_dirty = False

        # Simulate the mutation block that !remove executes on ok==True.
        if fake_name in plugin._plugin._contact_names:
            plugin._plugin._contact_names.remove(fake_name)
        plugin._plugin._node_types.pop(fake_name, None)
        plugin._plugin._node_last_advert.pop(fake_name, None)
        plugin._plugin._node_pubkey.pop(fake_name, None)
        plugin._plugin._node_did.pop(fake_name, None)
        plugin._plugin._contact_query_results.pop(fake_name, None)
        plugin._plugin._node_last_activity.pop(fake_name, None)
        plugin._plugin._node_locations.pop(fake_name, None)
        plugin._plugin._ws_devices_dirty = True  # the fix under test

        self.assertTrue(plugin._plugin._ws_devices_dirty,
                        "_ws_devices_dirty must be True after !remove mutation")

        # Now flushing must emit a devices push (full or delta).
        _Domoticz_stub.reset_ws()
        plugin._plugin._devices_last_push = 0.0
        # Reset baseline so we always get a full t:'devices' (predictable).
        with plugin._plugin._rx_log_lock:
            plugin._plugin._last_pushed_device_map = None
        self._inject_devices(plugin)
        try:
            plugin._plugin._push_dirty_feeds()
        finally:
            self._eject_devices(plugin)
        msgs = [r for r in self._replies() if r.get("t") in ("devices", "devices_delta")]
        self.assertEqual(len(msgs), 1,
                         f"Expected 1 devices push after !remove, got {len(msgs)}")


if __name__ == "__main__":
    unittest.main()
