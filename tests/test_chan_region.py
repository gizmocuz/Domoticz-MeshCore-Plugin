"""Tests for channels-analytics: hash, region mapping, store helpers, queries."""
import _bootstrap  # noqa: F401
import os
import sqlite3
import tempfile
import time
import unittest

import plugin


def _open_db(path):
    p = plugin.BasePlugin()
    p._msg_store_open(path)
    return p, p._msgdb


def _close(p, path):
    try:
        if p._msgdb is not None:
            p._msgdb.close()
            p._msgdb = None
    except Exception:
        pass
    for suf in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suf)
        except Exception:
            pass


class TestMeshcoreChanHash(unittest.TestCase):
    """_meshcore_chan_hash known-vector tests."""

    def test_public_is_1d(self):
        self.assertEqual(plugin.BasePlugin._meshcore_chan_hash("Public"), "1d")

    def test_mc_radar_is_03(self):
        self.assertEqual(plugin.BasePlugin._meshcore_chan_hash("#mc-radar"), "03")

    def test_bot_is_ca(self):
        self.assertEqual(plugin.BasePlugin._meshcore_chan_hash("#bot"), "ca")

    def test_test_is_d9(self):
        self.assertEqual(plugin.BasePlugin._meshcore_chan_hash("#test"), "d9")

    def test_returns_two_hex_chars(self):
        result = plugin.BasePlugin._meshcore_chan_hash("SomeName")
        self.assertEqual(len(result), 2)
        self.assertTrue(all(c in "0123456789abcdef" for c in result))


class TestChanHashForName(unittest.TestCase):
    """_chan_hash_for_name reverse-lookup tests."""

    def setUp(self):
        self.p = plugin.BasePlugin()

    def test_finds_name_in_map(self):
        self.p._chan_hash_to_name = {"1d": "Public", "03": "#mc-radar"}
        self.assertEqual(self.p._chan_hash_for_name("Public"), "1d")

    def test_computes_when_not_in_map(self):
        self.p._chan_hash_to_name = {}
        self.assertEqual(self.p._chan_hash_for_name("Public"), "1d")

    def test_returns_none_for_empty(self):
        self.p._chan_hash_to_name = {}
        self.assertIsNone(self.p._chan_hash_for_name(""))
        self.assertIsNone(self.p._chan_hash_for_name(None))


class TestRegionForCoord(unittest.TestCase):
    """_region_for_coord mapping tests."""

    def _r(self, lat, lon):
        return plugin.BasePlugin._region_for_coord(lat, lon)

    def test_zero_zero_is_other(self):
        self.assertEqual(self._r(0, 0), "other")

    def test_none_is_other(self):
        self.assertEqual(self._r(None, None), "other")

    def test_far_pacific_is_other(self):
        self.assertEqual(self._r(-33.9, 151.2), "other")  # Sydney

    def test_eu_outside_nl_is_eu(self):
        self.assertEqual(self._r(48.8, 2.3), "eu")   # Paris

    def test_nl_groningen_centroid(self):
        # Centroid of Groningen (53.2357, 6.5591) → nl-gr
        self.assertEqual(self._r(53.2357, 6.5591), "nl-gr")

    def test_nl_zeeland_centroid(self):
        self.assertEqual(self._r(51.4998, 3.6402), "nl-ze")

    def test_nl_limburg_centroid(self):
        self.assertEqual(self._r(50.9604, 5.8924), "nl-li")

    def test_nl_friesland_centroid(self):
        self.assertEqual(self._r(53.1893, 5.7949), "nl-fr")

    def test_nl_gelderland_centroid(self):
        self.assertEqual(self._r(52.0248, 5.8459), "nl-ge")

    def test_nl_flevoland_centroid(self):
        self.assertEqual(self._r(52.4462, 5.3544), "nl-fl")

    def test_nl_north_holland_centroid(self):
        self.assertEqual(self._r(52.4591, 4.8474), "nl-nh")


class TestTsChanObserved(unittest.TestCase):
    """_ts_chan_observed hourly upsert and lifetime total."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)

    def tearDown(self):
        _close(self.p, self.path)

    def _hourly_rows(self):
        with self.p._msgdb_lock:
            return self.db.execute(
                "SELECT hour_ts, chan_hash, count FROM ts_chan_hourly ORDER BY hour_ts, chan_hash"
            ).fetchall()

    def _totals(self):
        with self.p._msgdb_lock:
            return {r[0]: r[1] for r in self.db.execute(
                "SELECT chan_hash, count FROM chan_msg_totals"
            ).fetchall()}

    def test_inserts_hourly_row(self):
        t = 1_700_000_000
        self.p._ts_chan_observed("11", t)
        rows = self._hourly_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], t - (t % 3600))
        self.assertEqual(rows[0][1], "11")
        self.assertEqual(rows[0][2], 1)

    def test_upserts_same_hour(self):
        t = 1_700_000_000
        self.p._ts_chan_observed("11", t)
        self.p._ts_chan_observed("11", t + 1800)
        rows = self._hourly_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], 2)

    def test_separate_rows_across_hour_boundary(self):
        t = 1_700_000_000
        self.p._ts_chan_observed("11", t)
        self.p._ts_chan_observed("11", t + 3601)
        rows = self._hourly_rows()
        self.assertEqual(len(rows), 2)

    def test_lifetime_total_increments(self):
        t = 1_700_000_000
        self.p._ts_chan_observed("11", t)
        self.p._ts_chan_observed("11", t + 3601)
        totals = self._totals()
        self.assertEqual(totals.get("11"), 2)

    def test_marks_dirty_panels(self):
        self.p._ts_dirty_panels.clear()
        self.p._ts_chan_observed("11", int(time.time()))
        self.assertIn("chan_activity", self.p._ts_dirty_panels)
        self.assertIn("chan_regions", self.p._ts_dirty_panels)

    def test_never_raises_on_closed_db(self):
        self.p._msgdb = None
        self.p._ts_chan_observed("11", int(time.time()))  # must not raise


class TestChanRegionObserved(unittest.TestCase):
    """_chan_region_observed upsert tests."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)

    def tearDown(self):
        _close(self.p, self.path)

    def _rows(self):
        with self.p._msgdb_lock:
            return self.db.execute(
                "SELECT chan_hash, region, count FROM chan_region_counts"
            ).fetchall()

    def test_inserts_region_row(self):
        self.p._chan_region_observed("11", "nl-ge")
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], ("11", "nl-ge", 1))

    def test_upserts_same_region(self):
        self.p._chan_region_observed("11", "nl-ge")
        self.p._chan_region_observed("11", "nl-ge")
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][2], 2)

    def test_separate_rows_for_different_regions(self):
        self.p._chan_region_observed("11", "nl-ge")
        self.p._chan_region_observed("11", "nl-nh")
        rows = self._rows()
        self.assertEqual(len(rows), 2)

    def test_marks_chan_regions_dirty(self):
        self.p._ts_dirty_panels.clear()
        self.p._chan_region_observed("11", "eu")
        self.assertIn("chan_regions", self.p._ts_dirty_panels)


class TestQChanActivity(unittest.TestCase):
    """_q_chan_activity shape, ordering, windowing."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)
        self.p._chan_hash_to_name = {"11": "Public", "03": "#mc-radar"}
        # Seed: hash "11" has 5 msgs in h0 and 3 in h1; hash "03" has 4 in h0.
        self.h0 = 1_700_010_000 - (1_700_010_000 % 3600)
        self.h1 = self.h0 + 3600
        with self.p._msgdb_lock:
            self.db.execute(
                "INSERT INTO ts_chan_hourly (hour_ts, chan_hash, count) VALUES (?,?,?)",
                (self.h0, "11", 5),
            )
            self.db.execute(
                "INSERT INTO ts_chan_hourly (hour_ts, chan_hash, count) VALUES (?,?,?)",
                (self.h1, "11", 3),
            )
            self.db.execute(
                "INSERT INTO ts_chan_hourly (hour_ts, chan_hash, count) VALUES (?,?,?)",
                (self.h0, "03", 4),
            )
            self.db.commit()

    def tearDown(self):
        _close(self.p, self.path)

    def test_returns_series_key(self):
        result = self.p._q_chan_activity("chan_activity", self.h0, self.h1 + 3600, 3600, ())
        self.assertIn("series", result)

    def test_series_count(self):
        result = self.p._q_chan_activity("chan_activity", self.h0, self.h1 + 3600, 3600, ())
        self.assertEqual(len(result["series"]), 2)

    def test_sorted_by_total_desc(self):
        result = self.p._q_chan_activity("chan_activity", self.h0, self.h1 + 3600, 3600, ())
        # "11" total=8, "03" total=4 → "11" first
        self.assertEqual(result["series"][0]["hash"], "11")
        self.assertEqual(result["series"][1]["hash"], "03")

    def test_name_resolved(self):
        result = self.p._q_chan_activity("chan_activity", self.h0, self.h1 + 3600, 3600, ())
        s11 = next(s for s in result["series"] if s["hash"] == "11")
        self.assertEqual(s11["name"], "Public")

    def test_name_null_when_unknown(self):
        # "03" IS in the map → "#mc-radar"; test an unknown hash
        with self.p._msgdb_lock:
            self.db.execute(
                "INSERT INTO ts_chan_hourly (hour_ts, chan_hash, count) VALUES (?,?,?)",
                (self.h0, "ff", 1),
            )
            self.db.commit()
        result = self.p._q_chan_activity("chan_activity", self.h0, self.h1 + 3600, 3600, ())
        sff = next((s for s in result["series"] if s["hash"] == "ff"), None)
        self.assertIsNotNone(sff)
        self.assertIsNone(sff["name"])

    def test_windowing_excludes_out_of_range(self):
        old_h = self.h0 - 7200
        with self.p._msgdb_lock:
            self.db.execute(
                "INSERT INTO ts_chan_hourly (hour_ts, chan_hash, count) VALUES (?,?,?)",
                (old_h, "11", 100),
            )
            self.db.commit()
        result = self.p._q_chan_activity("chan_activity", self.h0, self.h1 + 3600, 3600, ())
        s11 = next(s for s in result["series"] if s["hash"] == "11")
        bucket_ts_values = [d[0] for d in s11["data"]]
        self.assertNotIn(old_h, bucket_ts_values)


class TestQChanRegions(unittest.TestCase):
    """_q_chan_regions shape, ordering, totals."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)
        self.p._chan_hash_to_name = {"11": "Public"}
        with self.p._msgdb_lock:
            # chan "11": total 10; regions nl-ge=6, eu=4
            self.db.execute(
                "INSERT INTO chan_msg_totals (chan_hash, count) VALUES ('11', 10)"
            )
            self.db.execute(
                "INSERT INTO chan_region_counts (chan_hash, region, count) VALUES ('11', 'nl-ge', 6)"
            )
            self.db.execute(
                "INSERT INTO chan_region_counts (chan_hash, region, count) VALUES ('11', 'eu', 4)"
            )
            # chan "03": total 5; region unknown=5
            self.db.execute(
                "INSERT INTO chan_msg_totals (chan_hash, count) VALUES ('03', 5)"
            )
            self.db.execute(
                "INSERT INTO chan_region_counts (chan_hash, region, count) VALUES ('03', 'unknown', 5)"
            )
            self.db.commit()

    def tearDown(self):
        _close(self.p, self.path)

    def test_returns_channels_key(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        self.assertIn("channels", result)

    def test_sorted_by_total_desc(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        totals = [c["total"] for c in result["channels"]]
        self.assertEqual(totals, sorted(totals, reverse=True))

    def test_channel_shape(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        ch = result["channels"][0]
        for key in ("hash", "name", "total", "regions"):
            self.assertIn(key, ch)

    def test_regions_sorted_by_count_desc(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        ch11 = next(c for c in result["channels"] if c["hash"] == "11")
        region_counts = [r["count"] for r in ch11["regions"]]
        self.assertEqual(region_counts, sorted(region_counts, reverse=True))

    def test_region_shape(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        ch11 = next(c for c in result["channels"] if c["hash"] == "11")
        reg = ch11["regions"][0]
        self.assertIn("region", reg)
        self.assertIn("count", reg)

    def test_name_resolved(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        ch11 = next(c for c in result["channels"] if c["hash"] == "11")
        self.assertEqual(ch11["name"], "Public")

    def test_name_null_when_unknown(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        ch03 = next(c for c in result["channels"] if c["hash"] == "03")
        self.assertIsNone(ch03["name"])

    def test_totals_match_seeded_values(self):
        result = self.p._q_chan_regions("chan_regions", 0, 1, 3600, ())
        ch11 = next(c for c in result["channels"] if c["hash"] == "11")
        self.assertEqual(ch11["total"], 10)


class TestRegionInferenceInHandleMessage(unittest.TestCase):
    """Verify region inference paths via _chan_region_observed indirectly."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)
        self.p._chan_hash_to_name = {"11": "Public"}
        self.p._node_locations = {}
        self.p._prefix_to_name = {}

    def tearDown(self):
        _close(self.p, self.path)

    def _get_region_rows(self):
        with self.p._msgdb_lock:
            return {
                (r[0], r[1]): r[2]
                for r in self.db.execute(
                    "SELECT chan_hash, region, count FROM chan_region_counts"
                ).fetchall()
            }

    def test_method1_sender_coord(self):
        self.p._node_locations["Alice"] = {"lat": 52.0248, "lon": 5.8459}
        self.p._chan_hash_for_name("Public")  # warm up
        # Call region inference directly as _handle_message would
        _chan_hash = self.p._chan_hash_for_name("Public")
        node_name = "Alice"
        _loc = self.p._node_locations.get(node_name)
        _region = self.p._region_for_coord(_loc.get("lat"), _loc.get("lon"))
        self.p._chan_region_observed(_chan_hash, _region)
        rows = self._get_region_rows()
        self.assertEqual(rows.get(("11", "nl-ge")), 1)

    def test_method2_first_hop_fallback(self):
        # Sender has no coords; first hop "aa" resolves to "RelayNode" which has coords.
        self.p._prefix_to_name = {"aabbccddeeff": "RelayNode"}
        self.p._node_locations["RelayNode"] = {"lat": 53.2357, "lon": 6.5591}  # nl-gr
        # Simulate: first token "aa", resolves via _resolve_relay_key
        _first_tok = "aa"
        _relay_name = self.p._resolve_relay_key(_first_tok)
        _rloc = self.p._node_locations.get(_relay_name)
        _region = self.p._region_for_coord(_rloc.get("lat"), _rloc.get("lon"))
        self.p._chan_region_observed("11", _region)
        rows = self._get_region_rows()
        self.assertEqual(rows.get(("11", "nl-gr")), 1)

    def test_unknown_region_when_no_coords(self):
        _chan_hash = "11"
        _region = "unknown"
        self.p._chan_region_observed(_chan_hash, _region)
        rows = self._get_region_rows()
        self.assertEqual(rows.get(("11", "unknown")), 1)


if __name__ == "__main__":
    unittest.main()
