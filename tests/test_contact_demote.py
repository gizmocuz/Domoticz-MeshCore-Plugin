"""Tests for the contact-demotion path: removing a contact promotes it into
the heard-nodes store with correct metadata and signal data.

The tests are hermetic — they exercise _demote_contact_to_heard() directly
without going through the asyncio worker or the meshcore library.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import time
import unittest


def _plugin_instance():
    import plugin
    return plugin._plugin


class TestDemoteContactToHeard(unittest.TestCase):
    """_demote_contact_to_heard creates/merges a heard entry."""

    # Stable test pubkey (long enough to be realistic)
    PK = "aabbccddeeff00112233445566778899aabbccddeeff0011"
    NAME = "TestContact"

    def setUp(self):
        self.p = _plugin_instance()
        self._orig_write_heard = self.p._write_heard
        self.p._write_heard = lambda: None   # no filesystem side-effects
        # Snapshot state we will restore
        self._orig_heard = dict(self.p._heard_nodes)
        self._orig_purged = set(self.p._heard_purged)
        self._orig_known = set(self.p._known_pubkeys)
        self._orig_types = dict(self.p._node_types)
        self._orig_pubkey = dict(self.p._node_pubkey)
        self._orig_advert = dict(self.p._node_last_advert)
        self._orig_locs = dict(self.p._node_locations)
        self._orig_sig = dict(self.p._contact_sig)
        # Seed a plausible contact state
        ts = int(time.time()) - 120
        self.p._node_pubkey[self.NAME] = self.PK
        self.p._node_types[self.NAME] = 2          # Repeater
        self.p._node_last_advert[self.NAME] = ts
        self.p._node_locations[self.NAME] = {"lat": 52.1, "lon": 5.2}
        self.p._contact_sig[self.PK[:12]] = {
            "snr": -7.5, "rssi": -110, "path_len": 2, "t": ts,
        }
        with self.p._rx_log_lock:
            self.p._heard_nodes.pop(self.PK, None)
            self.p._known_pubkeys.add(self.PK)

    def tearDown(self):
        self.p._heard_nodes = self._orig_heard
        self.p._heard_purged = self._orig_purged
        self.p._known_pubkeys = self._orig_known
        self.p._node_types = self._orig_types
        self.p._node_pubkey = self._orig_pubkey
        self.p._node_last_advert = self._orig_advert
        self.p._node_locations = self._orig_locs
        self.p._contact_sig = self._orig_sig
        self.p._write_heard = self._orig_write_heard

    def test_creates_heard_entry_with_correct_pubkey(self):
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        self.assertIsNotNone(h, "heard entry must be created")
        self.assertEqual(h["pubkey"], self.PK)

    def test_creates_heard_entry_with_correct_name(self):
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        self.assertEqual(h["name"], self.NAME)

    def test_type_preserved(self):
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        self.assertEqual(h["type"], 2, "contact type must be carried over")

    def test_signal_data_populated(self):
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        self.assertAlmostEqual(h["snr"], -7.5)
        self.assertEqual(h["rssi"], -110)
        self.assertEqual(h["path_len"], 2)

    def test_location_populated(self):
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        self.assertAlmostEqual(h["lat"], 52.1)
        self.assertAlmostEqual(h["lon"], 5.2)

    def test_known_pubkeys_cleared(self):
        """Pubkey must be removed from _known_pubkeys so future ADVERTs can
        update the heard entry (the worker's heard-creation gate requires the
        key to NOT be in _known_pubkeys)."""
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            self.assertNotIn(self.PK, self.p._known_pubkeys)

    def test_dirty_flags_set(self):
        self.p._heard_dirty = False
        self.p._ws_heard_dirty = False
        self.p._demote_contact_to_heard(self.NAME)
        self.assertTrue(self.p._heard_dirty)
        self.assertTrue(self.p._ws_heard_dirty)

    def test_merges_with_existing_heard_entry(self):
        """If a heard entry already exists (e.g. node was a contact before
        being removed, re-added, removed again) the existing first_heard
        and count are preserved while newer metadata wins."""
        with self.p._rx_log_lock:
            self.p._heard_nodes[self.PK] = {
                "pubkey": self.PK, "name": self.NAME,
                "first_heard": 1000, "count": 5,
                "snr": None, "rssi": None,
            }
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        # first_heard from original entry must survive (value 1000)
        self.assertEqual(h["first_heard"], 1000, "first_heard must be preserved")
        # count from original entry must survive
        self.assertEqual(h["count"], 5, "count must be preserved")
        # snr from contact_sig should overwrite the None
        self.assertAlmostEqual(h["snr"], -7.5)

    def test_no_pubkey_is_noop(self):
        """If the contact's pubkey is not in _node_pubkey, demote is a silent no-op."""
        self.p._node_pubkey.pop(self.NAME, None)
        before = dict(self.p._heard_nodes)
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            after = dict(self.p._heard_nodes)
        self.assertEqual(before, after, "no-pubkey case must not mutate heard_nodes")


class TestDemoteContactSignalFallbacks(unittest.TestCase):
    """Edge-case signal values: missing contact_sig entries."""

    PK = "ffeeddccbbaa00112233445566778899ffeeddccbbaa0011"
    NAME = "SiglessNode"

    def setUp(self):
        self.p = _plugin_instance()
        self._orig_write_heard = self.p._write_heard
        self.p._write_heard = lambda: None
        self._orig_heard = dict(self.p._heard_nodes)
        self._orig_known = set(self.p._known_pubkeys)
        self._orig_pubkey = dict(self.p._node_pubkey)
        self._orig_types = dict(self.p._node_types)
        self._orig_advert = dict(self.p._node_last_advert)
        self._orig_locs = dict(self.p._node_locations)
        self._orig_sig = dict(self.p._contact_sig)
        self.p._node_pubkey[self.NAME] = self.PK
        self.p._node_types[self.NAME] = 1
        self.p._node_last_advert[self.NAME] = int(time.time())
        self.p._node_locations[self.NAME] = {}
        self.p._contact_sig.pop(self.PK[:12], None)
        with self.p._rx_log_lock:
            self.p._heard_nodes.pop(self.PK, None)
            self.p._known_pubkeys.add(self.PK)

    def tearDown(self):
        self.p._heard_nodes = self._orig_heard
        self.p._known_pubkeys = self._orig_known
        self.p._node_pubkey = self._orig_pubkey
        self.p._node_types = self._orig_types
        self.p._node_last_advert = self._orig_advert
        self.p._node_locations = self._orig_locs
        self.p._contact_sig = self._orig_sig
        self.p._write_heard = self._orig_write_heard

    def test_no_signal_entry_does_not_raise(self):
        """Demote must succeed even when contact_sig has no entry."""
        try:
            self.p._demote_contact_to_heard(self.NAME)
        except Exception as exc:
            self.fail(f"_demote_contact_to_heard raised unexpectedly: {exc}")

    def test_snr_is_none_when_no_signal(self):
        self.p._demote_contact_to_heard(self.NAME)
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(self.PK)
        self.assertIsNone(h["snr"])


if __name__ == "__main__":
    unittest.main()
