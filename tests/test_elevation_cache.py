"""Tests for the elevation cache: schema migration, quantise, cache hit/miss,
batch chunking, upstream fallback, and LRU prune.

No live socket, no real MeshCore device, no Domoticz runtime required.
The DomoticzEx stub is injected by _bootstrap.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import os
import sqlite3
import tempfile
import time
import unittest
import unittest.mock
import urllib.error

import plugin


def _open_db(path):
    """Open the message store on a fresh BasePlugin instance and return (plugin, db)."""
    p = plugin.BasePlugin()
    p._msg_store_open(path)
    return p, p._msgdb


def _close_and_unlink(p, path):
    """Close the sqlite connection and delete the temp file (Windows-safe)."""
    try:
        if p._msgdb is not None:
            p._msgdb.close()
            p._msgdb = None
    except Exception:
        pass
    try:
        os.unlink(path)
    except Exception:
        pass
    # WAL side-files
    for ext in ("-wal", "-shm"):
        try:
            os.unlink(path + ext)
        except Exception:
            pass


class TestMigrationV3(unittest.TestCase):
    """Schema migration v2 → v3 creates elevation_cache table and index."""

    def _make_v2_db(self, path):
        """Create a DB that looks like a v2 schema (no elevation_cache)."""
        con = sqlite3.connect(path)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                chan      TEXT    NOT NULL,
                sender    TEXT    NOT NULL,
                epoch     TEXT    NOT NULL,
                bad       INTEGER NOT NULL DEFAULT 0,
                body      TEXT    NOT NULL,
                hops      INTEGER,
                snr       REAL,
                rssi      INTEGER,
                path      TEXT,
                ack       INTEGER,
                direction TEXT    NOT NULL DEFAULT 'in',
                recv_ts   TEXT    NOT NULL,
                peer_key  TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS preferences (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # Record version as 2 so _msg_store_migrate picks up from there
        con.execute("INSERT OR REPLACE INTO preferences VALUES ('db_version', '2')")
        con.commit()
        con.close()

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)  # let sqlite3 create it fresh

    def tearDown(self):
        _close_and_unlink(getattr(self, '_p', None) or plugin.BasePlugin(), self.path)

    def test_migration_creates_table(self):
        self._make_v2_db(self.path)
        self._p, db = _open_db(self.path)
        row = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name='elevation_cache'"
        ).fetchone()
        self.assertIsNotNone(row, "elevation_cache table must exist after v3 migration")

    def test_migration_creates_index(self):
        self._make_v2_db(self.path)
        self._p, db = _open_db(self.path)
        row = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='ix_elev_last_used'"
        ).fetchone()
        self.assertIsNotNone(row, "ix_elev_last_used index must exist after v3 migration")

    def test_migration_records_version_3(self):
        self._make_v2_db(self.path)
        self._p, _ = _open_db(self.path)
        ver = self._p._pref_get("db_version")
        self.assertEqual(ver, "3", "db_version pref must be '3' after migration")

    def test_migration_is_idempotent_on_fresh_db(self):
        """A completely fresh DB should also end up at version 3 without errors."""
        self._p, db = _open_db(self.path)
        tbl = db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
            " AND name='elevation_cache'"
        ).fetchone()
        idx = db.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='ix_elev_last_used'"
        ).fetchone()
        self.assertIsNotNone(tbl)
        self.assertIsNotNone(idx)


class TestElevQuantise(unittest.TestCase):
    """_elev_quantise is deterministic and round-trips cleanly."""

    # Access as an unbound function via __func__ to avoid binding issues
    @staticmethod
    def Q(lat, lon):
        return plugin.BasePlugin._elev_quantise(lat, lon)

    def test_basic(self):
        lat_q, lon_q = self.Q(52.1234, 5.6789)
        self.assertEqual(lat_q, 521234)
        self.assertEqual(lon_q, 56789)

    def test_negative_coords(self):
        lat_q, lon_q = self.Q(-33.8688, 151.2093)
        self.assertEqual(lat_q, round(-33.8688 * 1e4))
        self.assertEqual(lon_q, round(151.2093 * 1e4))

    def test_zero(self):
        lat_q, lon_q = self.Q(0.0, 0.0)
        self.assertEqual(lat_q, 0)
        self.assertEqual(lon_q, 0)

    def test_same_input_same_output(self):
        a = self.Q(52.0, 5.0)
        b = self.Q(52.0, 5.0)
        self.assertEqual(a, b)

    def test_rounding(self):
        # Quantisation must match JavaScript's Math.round semantics
        # (half-away-from-+inf), not Python's banker's rounding, so that
        # frontend-pre-rounded coords always hit the same cache row.
        import math
        for lat in (52.00005, 52.00015, 52.99995, -0.00005, -52.00005):
            lat_q, _ = self.Q(lat, 0.0)
            self.assertEqual(lat_q, math.floor(lat * 1e4 + 0.5),
                             f"quantisation mismatch for {lat}")


class TestCacheHit(unittest.TestCase):
    """A cache hit returns without making any HTTP call and updates last_used."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)
        # Pre-seed one cache entry with an old last_used timestamp
        lat_q, lon_q = plugin.BasePlugin._elev_quantise(52.0, 5.0)
        old_ts = int(time.time()) - 3600
        self.db.execute(
            "INSERT OR REPLACE INTO elevation_cache (lat_q, lon_q, elev_m, last_used)"
            " VALUES (?,?,?,?)",
            (lat_q, lon_q, 12.5, old_ts),
        )
        self.db.commit()
        self._old_ts = old_ts

    def tearDown(self):
        _close_and_unlink(self.p, self.path)

    def test_hit_returns_correct_elevation(self):
        with unittest.mock.patch("urllib.request.urlopen") as mock_open:
            result = self.p._elevation_lookup([(52.0, 5.0)])
        mock_open.assert_not_called()
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0], 12.5)

    def test_hit_updates_last_used(self):
        before = int(time.time())
        with unittest.mock.patch("urllib.request.urlopen"):
            self.p._elevation_lookup([(52.0, 5.0)])
        lat_q, lon_q = plugin.BasePlugin._elev_quantise(52.0, 5.0)
        row = self.db.execute(
            "SELECT last_used FROM elevation_cache WHERE lat_q=? AND lon_q=?",
            (lat_q, lon_q),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertGreaterEqual(row[0], before)
        self.assertGreater(row[0], self._old_ts)

    def test_no_http_call_on_full_hit(self):
        with unittest.mock.patch("urllib.request.urlopen") as mock_open:
            self.p._elevation_lookup([(52.0, 5.0)])
        mock_open.assert_not_called()


class TestBatchChunking(unittest.TestCase):
    """Batches larger than the chunk limit split into multiple upstream HTTP calls."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)

    def tearDown(self):
        _close_and_unlink(self.p, self.path)

    def _make_mock_urlopen(self, elevation=1.0):
        """Return a side_effect for urlopen that responds with the given elevation."""
        http_calls = []

        def fake_urlopen(req, timeout=None):
            if hasattr(req, 'data') and req.data:
                data = json.loads(req.data)
                n_pts = len(data.get("locations", []))
            else:
                # GET request to opentopodata — count | separators + 1
                from urllib.parse import urlparse, parse_qs, unquote
                url = req.full_url if hasattr(req, 'full_url') else str(req)
                qs = parse_qs(urlparse(url).query)
                locs = qs.get("locations", [""])[0]
                n_pts = locs.count("|") + 1 if locs else 1
            http_calls.append(n_pts)
            body = json.dumps({
                "results": [{"elevation": elevation} for _ in range(n_pts)]
            }).encode()
            mock_resp = unittest.mock.MagicMock()
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = unittest.mock.MagicMock(return_value=False)
            mock_resp.status = 200
            mock_resp.read.return_value = body
            return mock_resp

        return fake_urlopen, http_calls

    def test_more_than_499_unique_points_all_resolved(self):
        """600 unique cache-miss points must all be resolved (no silent drops)."""
        n = 600
        points = [(float(i) * 0.001, 0.0) for i in range(n)]
        fake_urlopen, _ = self._make_mock_urlopen()
        with unittest.mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            results = self.p._elevation_lookup(points)
        self.assertEqual(len(results), n,
            "Must return exactly one result per input point")
        non_none = [r for r in results if r is not None]
        self.assertEqual(len(non_none), n,
            "All 600 points must be resolved (no None gaps)")

    def test_more_than_100_points_trigger_multiple_upstream_calls(self):
        """250 cache-miss points must trigger at least 3 upstream HTTP calls (batch ≤100)."""
        n = 250
        points = [(float(i) * 0.001, float(i) * 0.001) for i in range(n)]
        fake_urlopen, http_calls = self._make_mock_urlopen()
        with unittest.mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            self.p._elevation_lookup(points)
        self.assertGreaterEqual(len(http_calls), 3,
            "250 points must trigger >= 3 upstream HTTP batches (batch size 100)")
        self.assertLessEqual(max(http_calls), 100,
            "Each upstream batch must have <= 100 points")

    def test_batch_size_never_exceeds_100(self):
        """Upstream batch size must never exceed 100 regardless of input size."""
        n = 500
        points = [(float(i) * 0.001, float(i) * 0.001) for i in range(n)]
        fake_urlopen, http_calls = self._make_mock_urlopen()
        with unittest.mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            self.p._elevation_lookup(points)
        if http_calls:
            self.assertLessEqual(max(http_calls), 100,
                "Each upstream batch must have <= 100 points")


class TestHttpFallback(unittest.TestCase):
    """HTTP error on open-elevation falls back to opentopodata."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)

    def tearDown(self):
        _close_and_unlink(self.p, self.path)

    def _topodata_response(self, n):
        body = json.dumps({
            "results": [{"elevation": 99.0} for _ in range(n)]
        }).encode()
        mock_resp = unittest.mock.MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = unittest.mock.MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = body
        return mock_resp

    def test_fallback_on_http_error(self):
        """open-elevation raises HTTPError → result comes from opentopodata."""
        call_order = []

        def fake_urlopen(req, timeout=None):
            url = req.full_url if hasattr(req, 'full_url') else str(req)
            if "open-elevation" in url:
                call_order.append("open-elevation")
                raise urllib.error.HTTPError(url, 500, "Server Error", {}, None)
            call_order.append("opentopodata")
            return self._topodata_response(1)

        with unittest.mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.p._elevation_lookup([(52.0, 5.0)])

        self.assertIn("open-elevation", call_order, "open-elevation must be tried first")
        self.assertIn("opentopodata", call_order, "opentopodata must be tried as fallback")
        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0], 99.0)

    def test_fallback_on_urlopen_exception(self):
        """open-elevation raises a generic exception → opentopodata is tried."""
        def fake_urlopen(req, timeout=None):
            url = req.full_url if hasattr(req, 'full_url') else str(req)
            if "open-elevation" in url:
                raise OSError("connection refused")
            return self._topodata_response(1)

        with unittest.mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.p._elevation_lookup([(52.0, 5.0)])

        self.assertEqual(len(result), 1)
        self.assertAlmostEqual(result[0], 99.0)

    def test_both_fail_returns_none(self):
        """Both upstreams fail → result is [None]."""
        def fake_urlopen(req, timeout=None):
            raise OSError("no network")

        with unittest.mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.p._elevation_lookup([(52.0, 5.0)])

        self.assertEqual(len(result), 1)
        self.assertIsNone(result[0])

    def test_fetched_elevation_is_persisted(self):
        """Fetched elevations are persisted to the DB for future cache hits."""
        body = json.dumps({"results": [{"elevation": 42.0}]}).encode()
        mock_resp = unittest.mock.MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = unittest.mock.MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = body

        with unittest.mock.patch("urllib.request.urlopen", return_value=mock_resp):
            self.p._elevation_lookup([(52.0, 5.0)])

        lat_q, lon_q = plugin.BasePlugin._elev_quantise(52.0, 5.0)
        row = self.db.execute(
            "SELECT elev_m FROM elevation_cache WHERE lat_q=? AND lon_q=?",
            (lat_q, lon_q),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertAlmostEqual(row[0], 42.0)

    def test_second_lookup_is_cache_hit(self):
        """After a successful fetch, a repeat lookup must not call HTTP again."""
        body = json.dumps({"results": [{"elevation": 42.0}]}).encode()
        mock_resp = unittest.mock.MagicMock()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = unittest.mock.MagicMock(return_value=False)
        mock_resp.status = 200
        mock_resp.read.return_value = body

        with unittest.mock.patch("urllib.request.urlopen", return_value=mock_resp) as m:
            self.p._elevation_lookup([(52.0, 5.0)])
            call_count_after_first = m.call_count
            self.p._elevation_lookup([(52.0, 5.0)])
            call_count_after_second = m.call_count

        self.assertEqual(call_count_after_first, 1)
        self.assertEqual(call_count_after_second, 1,
            "Second lookup must be served from cache — no extra HTTP call")


class TestLruPrune(unittest.TestCase):
    """LRU prune leaves the most-recently-used CAP rows and drops the rest."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.p, self.db = _open_db(self.path)

    def tearDown(self):
        _close_and_unlink(self.p, self.path)

    def _seed(self, n, cap_override=None):
        """Insert n rows with ascending last_used timestamps."""
        if cap_override is not None:
            self.p._ELEV_PRUNE_CAP = cap_override
        now = int(time.time())
        rows = [
            (i, i, float(i), now - (n - i))
            for i in range(n)
        ]
        self.db.executemany(
            "INSERT OR REPLACE INTO elevation_cache (lat_q, lon_q, elev_m, last_used)"
            " VALUES (?,?,?,?)",
            rows,
        )
        self.db.commit()

    def test_prune_removes_excess_rows(self):
        """Insert CAP+50 rows; after prune only CAP rows remain."""
        cap = 200
        total = cap + 50
        self._seed(total, cap_override=cap)

        count_before = self.db.execute(
            "SELECT COUNT(*) FROM elevation_cache"
        ).fetchone()[0]
        self.assertEqual(count_before, total)

        self.p._elev_prune()

        count_after = self.db.execute(
            "SELECT COUNT(*) FROM elevation_cache"
        ).fetchone()[0]
        self.assertEqual(count_after, cap,
            f"After prune, exactly {cap} rows must remain")

    def test_prune_keeps_newest(self):
        """Rows with the highest last_used values survive the prune."""
        cap = 100
        total = cap + 20
        self._seed(total, cap_override=cap)

        self.p._elev_prune()

        # The 'total - 1' row has the highest last_used and must survive
        lat_q = total - 1
        row = self.db.execute(
            "SELECT lat_q FROM elevation_cache WHERE lat_q=?", (lat_q,)
        ).fetchone()
        self.assertIsNotNone(row, "Newest rows must survive LRU prune")

    def test_prune_removes_oldest_rows(self):
        """Row 0 (oldest last_used) must be evicted when total > cap."""
        cap = 100
        total = cap + 20
        self._seed(total, cap_override=cap)

        self.p._elev_prune()

        row = self.db.execute(
            "SELECT lat_q FROM elevation_cache WHERE lat_q=0"
        ).fetchone()
        self.assertIsNone(row, "Oldest rows must be evicted by LRU prune")

    def test_prune_noop_when_under_cap(self):
        """If row count <= CAP, prune must not remove any rows."""
        cap = 500
        total = 100
        self._seed(total, cap_override=cap)

        self.p._elev_prune()

        count = self.db.execute(
            "SELECT COUNT(*) FROM elevation_cache"
        ).fetchone()[0]
        self.assertEqual(count, total,
            "Prune must be a no-op when row count is below the cap")


if __name__ == "__main__":
    unittest.main()
