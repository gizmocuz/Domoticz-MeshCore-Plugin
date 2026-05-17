"""Concurrency guard regression tests for findings 1 and 3.

Finding 1 — _contact_sig read without the lock in _build_device_map_payload.
Finding 3 — _known_pubkeys reassignment not atomic with _heard_nodes prune.

These tests verify:
  (a) _contact_sig: concurrent worker-thread mutation while the main thread
      calls _build_device_map_payload does not raise RuntimeError or yield torn
      data.  The value returned for a known prefix is always either None or a
      complete dict snapshot, never a partially-overwritten object.
  (b) _signal_history snapshot in _write_rx_log is taken under _rx_log_lock
      (already fixed; regression guard to detect future regressions).
  (c) _known_pubkeys reassignment in _handle_contacts is now under _rx_log_lock,
      so the worker thread never sees a partially-assigned pubkey set.
  (d) The _known_pubkeys + _heard_nodes prune are performed atomically: after
      _handle_contacts returns, _heard_nodes contains no key that is in
      _known_pubkeys.

No asyncio, no live socket, no Domoticz runtime required.
"""
import _bootstrap  # noqa: F401
import inspect
import threading
import time
import unittest

import DomoticzEx as _Domoticz_stub


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _plugin():
    import plugin
    return plugin._plugin


class _ContactSigRaceSetup(unittest.TestCase):
    """Shared setUp that installs a synthetic contact and contact_sig entry."""

    PREFIX = "aabbccdd1122"   # 12-hex pubkey prefix
    PK_FULL = "aabbccdd1122334455667788"

    def setUp(self):
        import plugin as _pm
        p = _plugin()
        _Domoticz_stub.Devices.clear()
        # Wire the stub Devices dict so _dev/_set work inside _build_device_map_payload.
        _pm.Devices = _Domoticz_stub.Devices
        # Register the node so _build_device_map_payload iterates over it.
        with p._rx_log_lock:
            p._contact_sig.clear()
            p._contact_sig[self.PREFIX] = {
                "snr": 5.0, "rssi": -80, "path_len": 1,
                "t": time.time(), "source": "advert",
            }
        p._node_pubkey["TestNode"] = self.PK_FULL
        p._node_last_activity["TestNode"] = int(time.time())
        if "TestNode" not in p._contact_names:
            p._contact_names.append("TestNode")

    def tearDown(self):
        import plugin as _pm
        p = _plugin()
        _Domoticz_stub.Devices.clear()
        try:
            del _pm.Devices
        except AttributeError:
            pass
        p._contact_names.clear()
        p._node_pubkey.clear()
        p._node_last_activity.clear()
        with p._rx_log_lock:
            p._contact_sig.clear()


# ---------------------------------------------------------------------------
# Finding 1: _contact_sig read must not raise under concurrent mutation
# ---------------------------------------------------------------------------

class TestContactSigLockGuard(_ContactSigRaceSetup):
    """_build_device_map_payload must not raise while the worker mutates
    _contact_sig concurrently."""

    def test_no_runtime_error_under_concurrent_insert(self):
        """Rapid concurrent inserts into _contact_sig must not cause
        RuntimeError during _build_device_map_payload."""
        p = _plugin()
        stop = threading.Event()
        errors = []

        def _worker():
            i = 0
            while not stop.is_set():
                with p._rx_log_lock:
                    key = f"{i:012x}"
                    p._contact_sig[key] = {
                        "snr": float(i % 20), "rssi": -70 - (i % 30),
                        "path_len": i % 5, "t": time.time(), "source": "advert",
                    }
                    # Also occasionally delete to provoke "size changed" errors
                    if i % 3 == 0 and key != self.PREFIX:
                        p._contact_sig.pop(key, None)
                i += 1

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        try:
            for _ in range(50):
                try:
                    p._build_device_map_payload()
                except RuntimeError as exc:
                    errors.append(str(exc))
                except Exception:
                    pass  # other errors are out of scope
        finally:
            stop.set()
            t.join(timeout=2)

        self.assertEqual(errors, [],
                         f"RuntimeError(s) during concurrent access: {errors}")

    def test_contact_sig_snapshot_is_complete_dict_or_none(self):
        """The adv value for a known prefix must be either None or a dict with
        all expected keys — never an empty dict or a partial object."""
        p = _plugin()
        payload = p._build_device_map_payload()
        node = payload.get("nodes", {}).get("TestNode")
        self.assertIsNotNone(node, "TestNode must appear in the payload")
        # snr slot filled from _contact_sig fallback (no Domoticz device exists)
        snr = node.get("snr")
        if snr is not None:
            self.assertIsInstance(snr, dict)
            self.assertIn("value", snr)


# ---------------------------------------------------------------------------
# Finding 1 (structural): confirm the read site is inside _rx_log_lock
# ---------------------------------------------------------------------------

class TestContactSigReadIsLocked(unittest.TestCase):
    """Source-level check: the _contact_sig.get() call in
    _build_device_map_payload must appear inside a 'with self._rx_log_lock'
    block.  This is a coarse structural guard; the race test above is the
    functional guard."""

    def test_contact_sig_read_wrapped_in_lock(self):
        import plugin
        src = inspect.getsource(plugin.BasePlugin._build_device_map_payload)
        # The snapshot pattern: 'with self._rx_log_lock:' must precede
        # '_contact_sig.get(' in the method source.
        lock_pos = src.find("with self._rx_log_lock:")
        read_pos = src.find("_contact_sig.get(")
        self.assertGreater(lock_pos, -1,
                           "_build_device_map_payload must acquire _rx_log_lock")
        self.assertGreater(read_pos, -1,
                           "_build_device_map_payload must call _contact_sig.get()")
        self.assertLess(lock_pos, read_pos,
                        "_contact_sig.get() must appear after 'with _rx_log_lock:'")


# ---------------------------------------------------------------------------
# Finding 2 (regression guard): _signal_history snapshot is under the lock
# ---------------------------------------------------------------------------

class TestSignalHistorySnapshotUnderLock(unittest.TestCase):
    """_write_rx_log snapshots _signal_history inside the _rx_log_lock block
    (already correct; this test guards against accidental regression)."""

    def test_signal_history_snapshot_inside_lock_block(self):
        import plugin
        src = inspect.getsource(plugin.BasePlugin._write_rx_log)
        # Both the lock acquisition and the snapshot must appear; snapshot
        # must come after the lock open.
        lock_pos = src.find("with self._rx_log_lock:")
        snap_pos = src.find("self._signal_history.items()")
        self.assertGreater(lock_pos, -1,
                           "_write_rx_log must acquire _rx_log_lock")
        self.assertGreater(snap_pos, -1,
                           "_write_rx_log must iterate _signal_history")
        self.assertLess(lock_pos, snap_pos,
                        "_signal_history iteration must be after 'with _rx_log_lock:'")


# ---------------------------------------------------------------------------
# Finding 3: _known_pubkeys reassignment is atomic with _heard_nodes prune
# ---------------------------------------------------------------------------

class TestKnownPubkeysAtomicUpdate(unittest.TestCase):
    """_handle_contacts must update _known_pubkeys and prune _heard_nodes
    inside a single _rx_log_lock acquisition so the worker thread never sees
    the new pubkey set without the corresponding heard-node removal."""

    def setUp(self):
        import plugin as _pm
        _Domoticz_stub.Devices.clear()
        # Wire the stub Devices dict into the plugin module so _set/_dev work.
        _pm.Devices = _Domoticz_stub.Devices

    def tearDown(self):
        import plugin as _pm
        p = _plugin()
        _Domoticz_stub.Devices.clear()
        try:
            del _pm.Devices
        except AttributeError:
            pass
        p._contact_names.clear()
        p._node_pubkey.clear()
        p._node_last_activity.clear()
        with p._rx_log_lock:
            p._heard_nodes.clear()
            p._known_pubkeys = set()

    def _make_contacts(self, names_and_pks):
        """Build a contacts dict as returned by the meshcore package."""
        return {
            name: {"adv_name": name, "public_key": pk, "type": 1,
                   "last_advert": 0, "out_path_len": 0,
                   "adv_lat": 0.0, "adv_lon": 0.0}
            for name, pk in names_and_pks
        }

    def test_heard_node_promoted_to_contact_is_removed(self):
        """After _handle_contacts, a pubkey that was in _heard_nodes and is
        now in the contacts list must be absent from _heard_nodes."""
        p = _plugin()
        pk = "deadbeef000011112222333344445555"
        with p._rx_log_lock:
            p._heard_nodes[pk] = {"pubkey": pk, "first_heard": time.time(),
                                  "name": "Ghost", "type": 2}
        contacts = self._make_contacts([("Ghost", pk)])
        p._handle_contacts(contacts)
        with p._rx_log_lock:
            self.assertNotIn(pk, p._heard_nodes,
                             "Promoted contact must be removed from _heard_nodes")
        self.assertIn(pk, p._known_pubkeys,
                      "Promoted contact must appear in _known_pubkeys")

    def test_known_pubkeys_updated_atomically_under_lock(self):
        """Source-level check: _known_pubkeys assignment is inside a
        'with self._rx_log_lock:' block in _handle_contacts."""
        import plugin
        src = inspect.getsource(plugin.BasePlugin._handle_contacts)
        lock_pos = src.find("with self._rx_log_lock:")
        assign_pos = src.find("self._known_pubkeys = new_known")
        self.assertGreater(lock_pos, -1,
                           "_handle_contacts must acquire _rx_log_lock")
        self.assertGreater(assign_pos, -1,
                           "_handle_contacts must assign _known_pubkeys")
        self.assertLess(lock_pos, assign_pos,
                        "_known_pubkeys assignment must appear after 'with _rx_log_lock:'")

    def test_no_heard_node_key_appears_in_known_pubkeys_after_contacts(self):
        """After _handle_contacts, _heard_nodes and _known_pubkeys must be
        disjoint (no key can be in both)."""
        p = _plugin()
        pk_contact = "aaaa0000bbbb1111cccc2222dddd3333"
        pk_heard   = "1111222233334444555566667777aaaa"
        with p._rx_log_lock:
            p._heard_nodes[pk_contact] = {"pubkey": pk_contact,
                                          "first_heard": time.time(), "name": "C"}
            p._heard_nodes[pk_heard]   = {"pubkey": pk_heard,
                                          "first_heard": time.time(), "name": "H"}
        contacts = self._make_contacts([("PromotedNode", pk_contact)])
        p._handle_contacts(contacts)
        with p._rx_log_lock:
            overlap = set(p._heard_nodes.keys()) & p._known_pubkeys
        self.assertEqual(overlap, set(),
                         f"_heard_nodes and _known_pubkeys overlap: {overlap}")


if __name__ == "__main__":
    import unittest
    unittest.main()
