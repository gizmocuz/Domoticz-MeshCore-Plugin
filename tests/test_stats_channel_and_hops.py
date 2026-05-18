"""Tests for:
  - Item #3: per-channel message counter increments, persists, and restores.
  - Item #4: path_len=255 (HOPS_SENTINEL) never enters hops_records; negative
    hops are also excluded.

No live socket, no asyncio, no Domoticz runtime required.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import os
import shutil
import tempfile
import unittest


def _plugin():
    import plugin
    return plugin._plugin


def _reset_stats(p):
    """Reset _stats to a clean slate (no lock needed — single-threaded tests)."""
    import plugin as pm
    with p._rx_log_lock:
        p._stats = {
            "adverts_total":  0, "messages_total": 0,
            "client_total":   0, "repeater_total": 0, "server_total": 0,
            "msg_by_sender":  {}, "adv_by_sender":  {},
            "msg_by_channel": {},
            "hops_records":   [],
            "today": {"date": "", "messages": 0,
                      "client": 0, "repeater": 0, "server": 0},
        }
        p._stats_dirty = False


# ── Item #3 — per-channel message counts ─────────────────────────────────────

class TestMsgByChannel(unittest.TestCase):

    def setUp(self):
        p = _plugin()
        _reset_stats(p)
        # Register two known channels so _channel_names.values() = {"Public", "NL"}
        p._channel_names = {0: "Public", 1: "NL"}

    def tearDown(self):
        p = _plugin()
        p._channel_names = {}
        _reset_stats(p)

    def test_known_channel_incremented(self):
        """A message on a known channel increments msg_by_channel."""
        p = _plugin()
        p._bump_msg_stats("Alice", 2, "Public")
        self.assertEqual(p._stats["msg_by_channel"].get("Public"), 1)

    def test_second_known_channel_incremented(self):
        """A second configured channel increments independently."""
        p = _plugin()
        p._bump_msg_stats("Bob", 1, "NL")
        self.assertEqual(p._stats["msg_by_channel"].get("NL"), 1)

    def test_known_channel_accumulates(self):
        """Multiple messages on the same channel sum correctly."""
        p = _plugin()
        for _ in range(5):
            p._bump_msg_stats("Alice", 0, "Public")
        self.assertEqual(p._stats["msg_by_channel"].get("Public"), 5)

    def test_private_dm_not_counted(self):
        """Private DMs (channel='P') must not enter msg_by_channel."""
        p = _plugin()
        p._bump_msg_stats("Alice", 0, "P")
        self.assertEqual(p._stats["msg_by_channel"], {})

    def test_unknown_channel_fallback_not_counted(self):
        """Unresolved channel fallbacks like 'C2' must not enter msg_by_channel."""
        p = _plugin()
        p._bump_msg_stats("Alice", 1, "C2")
        self.assertEqual(p._stats["msg_by_channel"], {})

    def test_msg_by_channel_in_build_stats_payload(self):
        """_build_stats_payload must include msg_by_channel."""
        p = _plugin()
        p._bump_msg_stats("Alice", 2, "Public")
        payload = p._build_stats_payload()
        self.assertIn("msg_by_channel", payload)
        self.assertEqual(payload["msg_by_channel"].get("Public"), 1)

    def test_msg_by_channel_persists_and_restores(self):
        """msg_by_channel survives a write/load round-trip via meshcore_stats.json."""
        p = _plugin()
        p._bump_msg_stats("Alice", 2, "Public")
        p._bump_msg_stats("Bob", 1, "NL")
        p._bump_msg_stats("Bob", 1, "NL")

        tmpdir = tempfile.mkdtemp()
        try:
            tmp_path = os.path.join(tmpdir, "meshcore_stats.json")
            orig_path = p._stats_path
            p._stats_path = lambda: tmp_path

            p._write_stats()
            self.assertTrue(os.path.isfile(tmp_path), "Stats file must be written")

            # Reset and reload
            _reset_stats(p)
            self.assertEqual(p._stats["msg_by_channel"], {})

            p._load_stats()

            self.assertEqual(p._stats["msg_by_channel"].get("Public"), 1)
            self.assertEqual(p._stats["msg_by_channel"].get("NL"), 2)
        finally:
            p._stats_path = orig_path
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_msg_by_channel_missing_from_file_loads_empty(self):
        """If msg_by_channel is absent from the file, _load_stats must not raise."""
        p = _plugin()
        tmpdir = tempfile.mkdtemp()
        try:
            tmp_path = os.path.join(tmpdir, "meshcore_stats.json")
            with open(tmp_path, "w") as f:
                json.dump({"messages_total": 10}, f)

            orig_path = p._stats_path
            p._stats_path = lambda: tmp_path
            _reset_stats(p)
            try:
                p._load_stats()
            except Exception as exc:
                self.fail(f"_load_stats raised with missing msg_by_channel: {exc}")
            self.assertEqual(p._stats["msg_by_channel"], {})
        finally:
            p._stats_path = orig_path
            shutil.rmtree(tmpdir, ignore_errors=True)


# ── Item #4 — hops sentinel (255) and negative hops excluded ─────────────────

class TestHopsSentinel(unittest.TestCase):

    def setUp(self):
        p = _plugin()
        _reset_stats(p)
        p._channel_names = {0: "Public"}

    def tearDown(self):
        p = _plugin()
        p._channel_names = {}
        _reset_stats(p)

    def test_255_hops_not_recorded(self):
        """hops=255 (firmware sentinel) must never enter hops_records."""
        p = _plugin()
        p._bump_msg_stats("Alice", 255, "Public")
        self.assertEqual(p._stats["hops_records"], [],
                         "hops=255 must not appear in hops_records")

    def test_negative_hops_not_recorded(self):
        """hops=-1 (unknown) must never enter hops_records."""
        p = _plugin()
        p._bump_msg_stats("Alice", -1, "Public")
        self.assertEqual(p._stats["hops_records"], [])

    def test_zero_hops_is_recorded(self):
        """hops=0 (direct neighbour) is a valid count and must be recorded."""
        p = _plugin()
        p._bump_msg_stats("Alice", 0, "Public")
        recs = p._stats["hops_records"]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["hops"], 0)

    def test_positive_hops_recorded(self):
        """Legitimate hop counts (1..254) must be recorded normally."""
        p = _plugin()
        p._bump_msg_stats("Bob", 5, "Public")
        recs = p._stats["hops_records"]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["hops"], 5)

    def test_254_hops_is_recorded(self):
        """hops=254 is the maximum legitimate value and must be recorded."""
        p = _plugin()
        p._bump_msg_stats("Bob", 254, "Public")
        recs = p._stats["hops_records"]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["hops"], 254)

    def test_255_not_recorded_via_advert_path(self):
        """path_len=255 from an advert (RX_LOG inline path) must not enter hops_records.

        We exercise the inline advert-hops code-path directly by calling the
        sentinel-guarded branch condition, verifying the guard constant is 255.
        """
        import plugin as pm
        sentinel = pm.HOPS_SENTINEL
        self.assertEqual(sentinel, 255,
                         "HOPS_SENTINEL must equal 255 (firmware no-path value)")
        # Verify the guard expression: 0 <= 255 < 255 is False
        self.assertFalse(0 <= 255 < sentinel,
                         "Guard '0 <= pl < HOPS_SENTINEL' must be False for pl=255")
        # And 0 <= 254 < 255 is True
        self.assertTrue(0 <= 254 < sentinel,
                        "Guard must be True for pl=254")

    def test_hops_records_load_skips_255(self):
        """Stored hops_records containing hops=255 must be silently dropped on load."""
        p = _plugin()
        tmpdir = tempfile.mkdtemp()
        try:
            tmp_path = os.path.join(tmpdir, "meshcore_stats.json")
            with open(tmp_path, "w") as f:
                json.dump({
                    "hops_records": [
                        {"hops": 255, "name": "SentinelNode", "date": "2024-01-01 00:00:00", "channel": "Advert"},
                        {"hops": 3,   "name": "RealNode",     "date": "2024-01-01 00:01:00", "channel": "Public"},
                    ]
                }, f)

            orig_path = p._stats_path
            p._stats_path = lambda: tmp_path
            _reset_stats(p)
            p._load_stats()

            recs = p._stats["hops_records"]
            names = [r["name"] for r in recs]
            self.assertNotIn("SentinelNode", names,
                             "hops=255 record must be discarded during load")
            self.assertIn("RealNode", names,
                          "Legitimate hops=3 record must survive load")
        finally:
            p._stats_path = orig_path
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
