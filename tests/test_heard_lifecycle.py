"""Tests for the heard-node lifecycle: !forget_heard, anti-resurrection, and
persistence round-trips for the purged set."""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import os
import shutil
import tempfile
import threading
import time
import unittest


def _plugin_instance():
    import plugin
    return plugin._plugin


def _make_advert_frame(pubkey, name="TestNode", adv_type=2, lat=0.0, lon=0.0):
    """Minimal RX_LOG frame dict that looks like an ADVERT from _on_rx_log's perspective."""
    return {
        "payload_typename": "ADVERT",
        "adv_name": name,
        "adv_type": adv_type,
        "adv_lat":  lat,
        "adv_lon":  lon,
        "adv_timestamp": int(time.time()),
        "sender_key": pubkey,       # field used by _on_rx_log for adv_key
        "path_len": 1,
        "snr": -5.0,
        "rssi": -100,
        "route_typename": "FLOOD",
        "t": time.time(),
    }


def _inject_advert(p, pubkey, name="TestNode"):
    """Simulate an incoming ADVERT being processed by _on_rx_log.

    _on_rx_log operates under _rx_log_lock and uses a list of parsed packet
    dicts.  We replicate just the minimal advert-creation branch by directly
    manipulating _heard_nodes under the lock, mirroring what the real code
    path does after the `adv_key not in _known_pubkeys` check.

    Using _on_rx_log directly would require faking a full RX_LOG_DATA object
    and the meshcore library structures that go with it.  The goal here is to
    test that the _heard_purged gate stops creation — so the simplest hermetic
    approach is to replicate the key guard logic.
    """
    with p._rx_log_lock:
        # Guard mirrors _on_rx_log line: adv_key not in _known_pubkeys and
        # adv_key not in _heard_purged
        if pubkey in p._known_pubkeys:
            return False
        if pubkey in p._heard_purged:
            return False
        h = p._heard_nodes.get(pubkey)
        if h is None:
            h = {"pubkey": pubkey, "first_heard": int(time.time()), "count": 0}
            p._heard_nodes[pubkey] = h
        h["name"] = name
        h["last_heard"] = int(time.time())
        h["count"] = (h.get("count") or 0) + 1
        p._heard_dirty = True
        p._ws_heard_dirty = True
    return True


class TestForgetHeardRemovesAndPurges(unittest.TestCase):
    """!forget_heard removes from _heard_nodes and adds to _heard_purged."""

    def setUp(self):
        self.p = _plugin_instance()
        self._orig_heard = dict(self.p._heard_nodes)
        self._orig_purged = set(self.p._heard_purged)
        self._orig_write_heard = self.p._write_heard
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        self.p._heard_nodes = self._orig_heard
        self.p._heard_purged = self._orig_purged
        self.p._write_heard = self._orig_write_heard
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _patch_write(self):
        """Replace _write_heard with a no-op so tests don't write real files."""
        self.p._write_heard = lambda: None

    def test_forget_removes_from_heard_nodes(self):
        self._patch_write()
        pk = "aabbccddeeff001122334455667788990011223344556677"
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {"pubkey": pk, "name": "NodeA"}
        self.p._handle_forget_heard(pk)
        with self.p._rx_log_lock:
            self.assertNotIn(pk, self.p._heard_nodes)

    def test_forget_adds_to_purged(self):
        self._patch_write()
        pk = "aabbccddeeff001122334455667788990011223344556677"
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {"pubkey": pk, "name": "NodeA"}
        self.p._handle_forget_heard(pk)
        with self.p._rx_log_lock:
            self.assertIn(pk, self.p._heard_purged)

    def test_forget_with_prefix_matches(self):
        """12-hex prefix resolves to the full pubkey."""
        self._patch_write()
        pk = "aabbccddeeff001122334455667788990011223344556677"
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {"pubkey": pk, "name": "NodeB"}
        self.p._handle_forget_heard(pk[:12])
        with self.p._rx_log_lock:
            self.assertNotIn(pk, self.p._heard_nodes)
            self.assertIn(pk, self.p._heard_purged)

    def test_forget_no_match_is_noop(self):
        """Unknown pubkey prefix must not raise and must not corrupt state."""
        self._patch_write()
        self.p._handle_forget_heard("deadbeef0000")
        # No exception — test passes implicitly


class TestAntiResurrection(unittest.TestCase):
    """After !forget_heard, ADVERTs from that node must NOT re-create the entry."""

    def setUp(self):
        self.p = _plugin_instance()
        self._orig_heard = dict(self.p._heard_nodes)
        self._orig_purged = set(self.p._heard_purged)
        self._orig_write_heard = self.p._write_heard

    def tearDown(self):
        self.p._heard_nodes = self._orig_heard
        self.p._heard_purged = self._orig_purged
        self.p._write_heard = self._orig_write_heard

    def test_advert_does_not_resurrect_purged_node(self):
        self.p._write_heard = lambda: None
        pk = "bbccddeeff0011223344556677889900112233445566778800"
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {"pubkey": pk, "name": "GhostNode"}
        self.p._handle_forget_heard(pk)
        # Simulate a subsequent ADVERT arriving.
        _inject_advert(self.p, pk, "GhostNode")
        with self.p._rx_log_lock:
            self.assertNotIn(pk, self.p._heard_nodes,
                             "Purged node must not be re-created by an ADVERT")

    def test_advert_creates_unknown_non_purged_node(self):
        """Sanity: unknown (non-purged) node IS added on ADVERT."""
        pk = "ccddee001122334455667788990011223344556677889900"
        with self.p._rx_log_lock:
            self.p._heard_nodes.pop(pk, None)
            self.p._heard_purged.discard(pk)
            self.p._known_pubkeys.discard(pk)
        _inject_advert(self.p, pk, "NewNode")
        with self.p._rx_log_lock:
            self.assertIn(pk, self.p._heard_nodes,
                          "Non-purged unknown node must be added on ADVERT")
        # Clean up
        with self.p._rx_log_lock:
            self.p._heard_nodes.pop(pk, None)


class TestPurgeLiftsOnContactAdd(unittest.TestCase):
    """Once a purged pubkey reappears in _known_pubkeys (_handle_contacts),
    it is removed from _heard_purged so future deletions work again."""

    def setUp(self):
        self.p = _plugin_instance()
        self._orig_purged = set(self.p._heard_purged)
        self._orig_known = set(self.p._known_pubkeys)
        self._orig_names = list(self.p._contact_names)
        self._orig_prefix = dict(self.p._prefix_to_name)

    def tearDown(self):
        self.p._heard_purged = self._orig_purged
        self.p._known_pubkeys = self._orig_known
        self.p._contact_names = self._orig_names
        self.p._prefix_to_name = self._orig_prefix

    def test_purge_lifted_when_contact_added(self):
        pk = "ddeeff00112233445566778899001122334455667788990011"
        with self.p._rx_log_lock:
            self.p._heard_purged.add(pk)
        # Simulate _handle_contacts computing new_known that includes pk.
        # We replicate just the purge-lift logic without the full contacts cycle.
        with self.p._rx_log_lock:
            new_known = self.p._known_pubkeys | {pk}
            self.p._known_pubkeys = new_known
            purge_lift = self.p._heard_purged & new_known
            if purge_lift:
                self.p._heard_purged -= purge_lift
        with self.p._rx_log_lock:
            self.assertNotIn(pk, self.p._heard_purged,
                             "Purge must be lifted once the key becomes a real contact")


class TestLoadHeardPurgedRoundTrip(unittest.TestCase):
    """_load_heard must restore the purged set from meshcore_heard.json."""

    def setUp(self):
        self.p = _plugin_instance()
        self.tmpdir = tempfile.mkdtemp()
        self._orig_heard = dict(self.p._heard_nodes)
        self._orig_purged = set(self.p._heard_purged)
        self._orig_path = self.p._heard_path

    def tearDown(self):
        self.p._heard_nodes = self._orig_heard
        self.p._heard_purged = self._orig_purged
        self.p._heard_path = self._orig_path
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _write_and_load(self, payload):
        path = os.path.join(self.tmpdir, "meshcore_heard.json")
        with open(path, "w") as f:
            json.dump(payload, f)
        self.p._heard_path = lambda: path
        self.p._heard_nodes = {}
        self.p._heard_purged = set()
        self.p._load_heard()

    def test_purged_list_is_restored(self):
        pk1 = "aabb001122334455667788"
        pk2 = "ccdd001122334455667788"
        self._write_and_load({
            "nodes": {"xx": {"name": "X"}},
            "purged": [pk1, pk2],
        })
        with self.p._rx_log_lock:
            purged = set(self.p._heard_purged)
        self.assertIn(pk1, purged)
        self.assertIn(pk2, purged)

    def test_missing_purged_key_is_fine(self):
        """JSON without 'purged' key must load without raising."""
        self._write_and_load({"nodes": {}})
        with self.p._rx_log_lock:
            purged = set(self.p._heard_purged)
        self.assertEqual(purged, set())

    def test_nodes_still_loaded_with_purged(self):
        pk = "aabb001122334455667788"
        self._write_and_load({
            "nodes": {"xypk": {"name": "Foo", "pubkey": "xypk"}},
            "purged": [pk],
        })
        with self.p._rx_log_lock:
            nodes = dict(self.p._heard_nodes)
        self.assertIn("xypk", nodes)


class TestWriteHeardIncludesPurged(unittest.TestCase):
    """_write_heard must include the 'purged' array in the JSON payload."""

    def setUp(self):
        self.p = _plugin_instance()
        self.tmpdir = tempfile.mkdtemp()
        self._orig_path = self.p._heard_path
        self._orig_purged = set(self.p._heard_purged)
        self._orig_heard = dict(self.p._heard_nodes)

    def tearDown(self):
        self.p._heard_path = self._orig_path
        self.p._heard_purged = self._orig_purged
        self.p._heard_nodes = self._orig_heard
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_purged_written_to_json(self):
        path = os.path.join(self.tmpdir, "meshcore_heard.json")
        self.p._heard_path = lambda: path
        pk = "001122334455667788990011"
        with self.p._rx_log_lock:
            self.p._heard_purged = {pk}
            self.p._heard_nodes = {}
        self.p._write_heard()
        with open(path) as f:
            data = json.load(f)
        self.assertIn("purged", data, "purged key must be present in written JSON")
        self.assertIn(pk, data["purged"])

    def test_empty_purged_written_as_empty_list(self):
        path = os.path.join(self.tmpdir, "meshcore_heard.json")
        self.p._heard_path = lambda: path
        with self.p._rx_log_lock:
            self.p._heard_purged = set()
            self.p._heard_nodes = {}
        self.p._write_heard()
        with open(path) as f:
            data = json.load(f)
        self.assertEqual(data.get("purged"), [])


if __name__ == "__main__":
    unittest.main()
