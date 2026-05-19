"""Tests for the dzVents command bridge.

Covers pure logic that runs on the main thread without a live asyncio loop,
a real Domoticz runtime, or a mesh connection:

  - _dzv_next_id increments and wraps at 1_000_000.
  - _dzv_prune_origins removes stale entries (age >300 s) and enforces the
    200-entry cap.
  - _dzv_channel_match normalises both sides and returns True/False correctly.
  - Inbound gate in _handle_message: a channel "!" message on the configured
    channel with the bridge enabled records an origin entry (kind "chan") and
    writes a JSON payload with a monotonically increasing seq to UNIT_DZV_IN;
    private DMs, wrong channels, empty _dzv_channel, disabled bridge, and
    non-"!" text do not trigger the gate.
  - onCommand reply resolution: id -> channel send string "#chan: body";
    explicit "to" override; unknown/expired id -> no send attempt;
    bad JSON -> no send attempt.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import time
import unittest
from unittest.mock import patch, MagicMock

import DomoticzEx as _stub

import plugin
from plugin import (
    BasePlugin,
    MESH_DID,
    UNIT_DZV_IN,
    UNIT_DZV_REPLY,
    UNIT_DZV_SEND,
)

# Sentinel used to distinguish "Devices was not set" from "Devices was set to X".
_sentinel = object()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_plugin(channel="alerts"):
    """Return a pristine BasePlugin instance with the dzVents bridge configured on *channel*.

    A non-empty channel implies the bridge is enabled, mirroring the onStart
    derivation: _dzv_enabled = bool(_dzv_channel).
    """
    p = BasePlugin()
    p._dzv_channel = channel
    p._dzv_enabled = bool(channel)
    return p


def _make_msg(text, msg_type="CHAN", pubkey_prefix="aabbccddeeff", snr=5.0,
              channel_idx=None, sender_timestamp=None):
    """Build a minimal message dict as the worker pushes it onto the queue."""
    m = {
        "type": msg_type,
        "text": text,
        "pubkey_prefix": pubkey_prefix,
        "SNR": snr,
    }
    if channel_idx is not None:
        m["channel_idx"] = channel_idx
    if sender_timestamp is not None:
        m["sender_timestamp"] = sender_timestamp
    return m


# ---------------------------------------------------------------------------
# _dzv_next_id
# ---------------------------------------------------------------------------

class TestDzvNextId(unittest.TestCase):

    def test_starts_at_one(self):
        p = _fresh_plugin()
        self.assertEqual(p._dzv_next_id(), 1)

    def test_increments(self):
        p = _fresh_plugin()
        ids = [p._dzv_next_id() for _ in range(5)]
        self.assertEqual(ids, [1, 2, 3, 4, 5])

    def test_wraps_at_one_million(self):
        p = _fresh_plugin()
        p._dzv_req_id = 999_999
        nxt = p._dzv_next_id()
        self.assertEqual(nxt, 0)
        self.assertEqual(p._dzv_req_id, 0)

    def test_wrap_then_increment(self):
        p = _fresh_plugin()
        p._dzv_req_id = 999_999
        p._dzv_next_id()        # -> 0 (wrap)
        self.assertEqual(p._dzv_next_id(), 1)


# ---------------------------------------------------------------------------
# _dzv_prune_origins
# ---------------------------------------------------------------------------

class TestDzvPruneOrigins(unittest.TestCase):

    def _origin(self, ts=None):
        return {"kind": "chan", "chan": "alerts", "ts": ts or time.time()}

    def test_removes_stale_entries(self):
        p = _fresh_plugin()
        old_ts = time.time() - 400
        p._cmd_origins = {1: self._origin(old_ts), 2: self._origin()}
        p._dzv_prune_origins()
        self.assertNotIn(1, p._cmd_origins)
        self.assertIn(2, p._cmd_origins)

    def test_keeps_fresh_entries(self):
        p = _fresh_plugin()
        p._cmd_origins = {1: self._origin(), 2: self._origin()}
        p._dzv_prune_origins()
        self.assertEqual(len(p._cmd_origins), 2)

    def test_caps_at_200(self):
        p = _fresh_plugin()
        now = time.time()
        # Fill 250 entries with incrementing timestamps so oldest are evicted.
        p._cmd_origins = {
            i: {"kind": "chan", "chan": "alerts", "ts": now + i}
            for i in range(250)
        }
        p._dzv_prune_origins()
        self.assertLessEqual(len(p._cmd_origins), 200)

    def test_cap_keeps_newest(self):
        p = _fresh_plugin()
        now = time.time()
        p._cmd_origins = {
            i: {"kind": "chan", "chan": "alerts", "ts": now + i}
            for i in range(250)
        }
        p._dzv_prune_origins()
        # The 200 newest (highest ts = highest keys 50..249) should survive.
        remaining_keys = sorted(p._cmd_origins.keys())
        self.assertEqual(remaining_keys[0], 50)

    def test_prune_empty_dict_is_noop(self):
        p = _fresh_plugin()
        p._cmd_origins = {}
        p._dzv_prune_origins()
        self.assertEqual(p._cmd_origins, {})


# ---------------------------------------------------------------------------
# _dzv_channel_match
# ---------------------------------------------------------------------------

class TestDzvChannelMatch(unittest.TestCase):

    def test_exact_match(self):
        p = _fresh_plugin(channel="alerts")
        self.assertTrue(p._dzv_channel_match("alerts"))

    def test_hash_prefix_normalised(self):
        p = _fresh_plugin(channel="alerts")
        self.assertTrue(p._dzv_channel_match("#alerts"))

    def test_case_insensitive(self):
        p = _fresh_plugin(channel="alerts")
        self.assertTrue(p._dzv_channel_match("Alerts"))
        self.assertTrue(p._dzv_channel_match("ALERTS"))

    def test_hash_and_case(self):
        p = _fresh_plugin(channel="alerts")
        self.assertTrue(p._dzv_channel_match("#Alerts"))

    def test_configured_with_hash_prefix(self):
        # If the user stored the name with a leading '#'
        p = _fresh_plugin(channel="#alerts")
        self.assertTrue(p._dzv_channel_match("alerts"))
        self.assertTrue(p._dzv_channel_match("#alerts"))

    def test_wrong_channel_no_match(self):
        p = _fresh_plugin(channel="alerts")
        self.assertFalse(p._dzv_channel_match("general"))
        self.assertFalse(p._dzv_channel_match("#general"))

    def test_empty_configured_channel_no_match(self):
        p = _fresh_plugin(channel="")
        self.assertFalse(p._dzv_channel_match("alerts"))

    def test_disabled_bridge_no_match(self):
        p = _fresh_plugin(channel="alerts")
        p._dzv_enabled = False
        self.assertFalse(p._dzv_channel_match("alerts"))

    def test_private_tag_no_match(self):
        p = _fresh_plugin(channel="alerts")
        self.assertFalse(p._dzv_channel_match("P"))

    def test_whitespace_stripped(self):
        p = _fresh_plugin(channel="  alerts  ")
        self.assertTrue(p._dzv_channel_match("alerts"))
        self.assertTrue(p._dzv_channel_match("  alerts  "))


# ---------------------------------------------------------------------------
# Inbound gate in _handle_message
# ---------------------------------------------------------------------------

class TestInboundGate(unittest.TestCase):
    """Verify _handle_message records an origin and writes UNIT_DZV_IN."""

    def setUp(self):
        self._p = _fresh_plugin(channel="alerts")
        # Pre-populate prefix->name so node_name resolves.
        self._p._prefix_to_name["aabbccddeeff"] = "Alice"
        # Track _set calls.
        self._set_calls = []
        self._p._set = lambda did, unit, nv, sv: self._set_calls.append((did, unit, nv, sv))
        # Stub out side-effectful methods not under test.
        self._p._bump_msg_stats = lambda *a, **k: None
        self._p._msg_store_add = lambda **k: None
        self._p._log_contact_dm = lambda *a, **k: None
        self._p._ensure_node_devices = lambda *a: None
        self._p._write_device_map = lambda: None
        self._p._device_id_for = lambda name: None

    def _handle(self, msg):
        self._p._handle_message(msg)

    def _dzvin_calls(self):
        return [(d, u, nv, sv) for d, u, nv, sv in self._set_calls
                if d == MESH_DID and u == UNIT_DZV_IN]

    # ── channel-based trigger ────────────────────────────────────────────────

    def test_channel_bang_command_with_mapped_name(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self.assertEqual(len(self._p._cmd_origins), 1)
        rid, origin = next(iter(self._p._cmd_origins.items()))
        self.assertEqual(origin["kind"], "chan")
        self.assertEqual(origin["chan"], "alerts")

    def test_channel_bang_command_writes_dzvin(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        calls = self._dzvin_calls()
        self.assertEqual(len(calls), 1)
        _, _, nv, sv = calls[0]
        self.assertEqual(nv, 0)
        data = json.loads(sv)
        self.assertEqual(data["cmd"], "!ping")
        self.assertEqual(data["sender"], "Alice")
        self.assertEqual(data["channel"], "alerts")

    def test_channel_bang_payload_has_id_seq_ts(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!status", msg_type="CHAN", channel_idx=0))
        data = json.loads(self._dzvin_calls()[0][3])
        self.assertIn("id", data)
        self.assertIn("seq", data)
        self.assertIn("ts", data)
        self.assertIsInstance(data["ts"], int)

    def test_chan_tag_with_hash_prefix_matches(self):
        # chan_tag "#alerts" should match configured "alerts"
        self._p._channel_names[0] = "#alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self.assertEqual(len(self._dzvin_calls()), 1)

    def test_chan_tag_case_insensitive_match(self):
        self._p._channel_names[0] = "Alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self.assertEqual(len(self._dzvin_calls()), 1)

    def test_seq_increments_on_repeated_command(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self._handle(_make_msg("!ping", pubkey_prefix="bbccddee1122",
                               msg_type="CHAN", channel_idx=0))
        seqs = [json.loads(sv)["seq"] for _, _, _, sv in self._dzvin_calls()]
        self.assertEqual(seqs[1], seqs[0] + 1)

    def test_seq_guarantees_unique_payload(self):
        """Two identical commands must produce different JSON payloads (via seq)."""
        self._p._channel_names[0] = "alerts"
        self._p._prefix_to_name["bbccddee1122"] = "Alice"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self._handle(_make_msg("!ping", pubkey_prefix="bbccddee1122",
                               msg_type="CHAN", channel_idx=0))
        calls = self._dzvin_calls()
        self.assertNotEqual(calls[0][3], calls[1][3])

    def test_snr_included_in_payload(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0, snr=-3.5))
        data = json.loads(self._dzvin_calls()[0][3])
        self.assertAlmostEqual(data["snr"], -3.5)

    def test_multiple_commands_accumulate_origins(self):
        self._p._channel_names[0] = "alerts"
        self._p._prefix_to_name["112233445566"] = "Bob"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self._handle(_make_msg("!status", pubkey_prefix="112233445566",
                               msg_type="CHAN", channel_idx=0))
        self.assertEqual(len(self._p._cmd_origins), 2)

    # ── must NOT trigger ─────────────────────────────────────────────────────

    def test_private_dm_does_not_trigger(self):
        """Private DMs must no longer trigger the bridge."""
        self._handle(_make_msg("!ping", msg_type="PRIV"))
        self.assertEqual(self._dzvin_calls(), [])
        self.assertEqual(self._p._cmd_origins, {})

    def test_wrong_channel_does_not_trigger(self):
        self._p._channel_names[0] = "general"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self.assertEqual(self._dzvin_calls(), [])

    def test_empty_dzv_channel_does_not_trigger(self):
        self._p._dzv_channel = ""
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self.assertEqual(self._dzvin_calls(), [])

    def test_non_bang_channel_message_does_not_trigger(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("hello world", msg_type="CHAN", channel_idx=0))
        self.assertEqual(self._dzvin_calls(), [])
        self.assertEqual(self._p._cmd_origins, {})

    def test_disabled_bridge_does_not_trigger(self):
        self._p._dzv_enabled = False
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("!ping", msg_type="CHAN", channel_idx=0))
        self.assertEqual(self._dzvin_calls(), [])
        self.assertEqual(self._p._cmd_origins, {})

    def test_whitespace_stripped_cmd(self):
        self._p._channel_names[0] = "alerts"
        self._handle(_make_msg("  !ping  ", msg_type="CHAN", channel_idx=0))
        calls = self._dzvin_calls()
        self.assertEqual(len(calls), 1)
        data = json.loads(calls[0][3])
        self.assertEqual(data["cmd"], "!ping")


# ---------------------------------------------------------------------------
# onCommand reply resolution
# ---------------------------------------------------------------------------

class TestOnCommand(unittest.TestCase):
    """Verify onCommand dispatches the right send string or errors cleanly."""

    def setUp(self):
        self._p = _fresh_plugin(channel="alerts")
        self._sent = []

        # Stub _send_message_for_text so we capture the send string without
        # needing a real asyncio loop.
        async def _fake_send(text, req_id):
            self._sent.append(text)

        self._p._send_message_for_text = _fake_send
        # Provide a fake worker loop that makes run_coroutine_threadsafe a no-op
        # but lets us capture the coroutine via a side channel.
        fake_loop = MagicMock()
        captured = self._sent

        def _rct(coro, loop):
            # Drive the coroutine synchronously so _sent is populated.
            import asyncio
            try:
                asyncio.get_event_loop().run_until_complete(coro)
            except RuntimeError:
                loop2 = asyncio.new_event_loop()
                loop2.run_until_complete(coro)
                loop2.close()

        fake_loop.__class__ = MagicMock  # satisfy any isinstance checks
        self._p._worker_loop = fake_loop

        # Patch asyncio.run_coroutine_threadsafe to run synchronously.
        self._rct_patcher = patch(
            "plugin.asyncio.run_coroutine_threadsafe",
            side_effect=_rct,
        )
        self._rct_patcher.start()

        # Stub _set so we can track reset calls.
        self._set_calls = []
        self._p._set = lambda did, unit, nv, sv: self._set_calls.append((did, unit, nv, sv))

        # Inject a Devices stub for UNIT_DZV_REPLY reads.
        self._reply_unit = _stub.Unit(sValue="")
        mesh_device = _stub.Device()
        mesh_device.Units = {UNIT_DZV_REPLY: self._reply_unit}
        self._old_devices = getattr(plugin, "Devices", _sentinel)
        plugin.Devices = {MESH_DID: mesh_device}

    def tearDown(self):
        self._rct_patcher.stop()
        if self._old_devices is _sentinel:
            try:
                del plugin.Devices
            except AttributeError:
                pass
        else:
            plugin.Devices = self._old_devices

    def _set_reply(self, payload):
        self._reply_unit.sValue = json.dumps(payload) if isinstance(payload, dict) else payload

    def _run_command(self, command="On"):
        self._p.onCommand(MESH_DID, UNIT_DZV_SEND, command, 0, "")

    def _reset_calls(self):
        return [(d, u, nv, sv) for d, u, nv, sv in self._set_calls
                if d == MESH_DID and u == UNIT_DZV_SEND]

    def _add_chan_origin(self, rid, chan="alerts"):
        self._p._cmd_origins[rid] = {
            "kind": "chan", "chan": chan,
            "ts": time.time(),
        }

    # ── happy path: id -> channel send string ────────────────────────────────

    def test_chan_reply_builds_correct_send_string(self):
        self._add_chan_origin(42, chan="alerts")
        self._set_reply({"id": 42, "text": "pong"})
        self._run_command()
        self.assertEqual(self._sent, ["#alerts: pong"])

    def test_chan_origin_popped_after_send(self):
        self._add_chan_origin(42)
        self._set_reply({"id": 42, "text": "pong"})
        self._run_command()
        self.assertNotIn(42, self._p._cmd_origins)

    def test_button_reset_to_off_after_send(self):
        self._add_chan_origin(42)
        self._set_reply({"id": 42, "text": "pong"})
        self._run_command()
        resets = self._reset_calls()
        self.assertTrue(any(sv == "Off" for _, _, _, sv in resets),
                        f"Expected Off reset, got: {resets}")

    # ── explicit 'to' override ───────────────────────────────────────────────

    def test_explicit_to_override_dm(self):
        self._set_reply({"to": "Bob", "text": "hello"})
        self._run_command()
        self.assertEqual(self._sent, ["Bob: hello"])

    def test_explicit_to_override_channel(self):
        self._set_reply({"to": "#General", "text": "broadcast"})
        self._run_command()
        self.assertEqual(self._sent, ["#General: broadcast"])

    def test_explicit_to_does_not_require_id(self):
        self._set_reply({"to": "Charlie", "text": "hi"})
        self._run_command()
        self.assertEqual(len(self._sent), 1)

    # ── error paths ─────────────────────────────────────────────────────────

    def test_unknown_id_no_send(self):
        self._set_reply({"id": 9999, "text": "pong"})
        self._run_command()
        self.assertEqual(self._sent, [])

    def test_unknown_id_still_resets_button(self):
        self._set_reply({"id": 9999, "text": "pong"})
        self._run_command()
        resets = self._reset_calls()
        self.assertTrue(any(sv == "Off" for _, _, _, sv in resets))

    def test_bad_json_no_send(self):
        self._reply_unit.sValue = "not-json{{"
        self._run_command()
        self.assertEqual(self._sent, [])

    def test_bad_json_resets_button(self):
        self._reply_unit.sValue = "not-json{{"
        self._run_command()
        resets = self._reset_calls()
        self.assertTrue(any(sv == "Off" for _, _, _, sv in resets))

    def test_disabled_bridge_ignores_command(self):
        self._p._dzv_enabled = False
        self._add_chan_origin(1)
        self._set_reply({"id": 1, "text": "pong"})
        self._run_command()
        self.assertEqual(self._sent, [])

    def test_wrong_device_ignored(self):
        self._add_chan_origin(1)
        self._set_reply({"id": 1, "text": "pong"})
        self._p.onCommand("other_did", UNIT_DZV_SEND, "On", 0, "")
        self.assertEqual(self._sent, [])

    def test_wrong_unit_ignored(self):
        self._add_chan_origin(1)
        self._set_reply({"id": 1, "text": "pong"})
        self._p.onCommand(MESH_DID, 99, "On", 0, "")
        self.assertEqual(self._sent, [])

    def test_off_command_ignored(self):
        self._add_chan_origin(1)
        self._set_reply({"id": 1, "text": "pong"})
        self._run_command(command="Off")
        self.assertEqual(self._sent, [])

    def test_no_worker_loop_no_send(self):
        self._p._worker_loop = None
        self._add_chan_origin(1)
        self._set_reply({"id": 1, "text": "pong"})
        self._run_command()
        self.assertEqual(self._sent, [])

    def test_missing_text_field_no_send(self):
        self._add_chan_origin(1)
        self._set_reply({"id": 1})
        self._run_command()
        self.assertEqual(self._sent, [])

    def test_empty_text_field_no_send(self):
        self._add_chan_origin(1)
        self._set_reply({"id": 1, "text": "   "})
        self._run_command()
        self.assertEqual(self._sent, [])

    def test_non_string_text_field_no_send(self):
        # A non-string "text" (number / null) must not raise out of
        # onCommand and must not dispatch a send.
        for bad in (123, None, True, [], {}):
            self._sent.clear()
            self._add_chan_origin(1)
            self._set_reply({"id": 1, "text": bad})
            self._run_command()
            self.assertEqual(self._sent, [], f"text={bad!r} should not send")


if __name__ == "__main__":
    unittest.main()
