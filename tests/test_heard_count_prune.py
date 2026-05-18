"""Tests for heard-node count tracking and !heard_prune command.

Covers:
  (a) count increments on each ADVERT reception.
  (b) Existing nodes loaded from older JSON (no count key) start at 1 on next advert.
  (c) !heard_prune week|month|year removes only entries older than the threshold
      and keeps recent ones.
  (d) !heard_prune once removes entries with count<=1 and keeps count>=2.
  (e) Unknown criteria returns ok:false and nothing is removed.
  (f) Dirty flags are set and _write_heard is called only when >0 nodes are removed.
  (g) Pruned count is returned in the result payload body.
  (h) Entries with no last_heard are kept (skipped) for age-based prune criteria.
"""
import _bootstrap  # noqa: F401
import asyncio
import time
import unittest

import plugin


def _plugin():
    return plugin._plugin


def _make_advert_event(pubkey, name="TestNode", snr=5.0, rssi=-90,
                       path_len=1, adv_lat=None, adv_lon=None):
    """Build a minimal fake rx-log event object matching what _on_rx_log expects."""
    class _Ev:
        pass
    ev = _Ev()
    ev.payload = {
        "payload_typename": "ADVERT",
        "adv_key": pubkey,
        "adv_name": name,
        "adv_type": 2,
        "snr": snr,
        "rssi": rssi,
        "path_len": path_len,
        "adv_lat": adv_lat,
        "adv_lon": adv_lon,
    }
    return ev


def _clear_heard(p):
    with p._rx_log_lock:
        p._heard_nodes.clear()
        p._known_pubkeys = set()
    p._heard_dirty = False
    p._ws_heard_dirty = False


def _run_prune(text):
    """Invoke _send_message with the given text and return the send_result payload
    placed on the queue, without needing a live mc connection.  The !heard_prune
    branch returns early before touching mc, so we pass None."""
    p = _plugin()
    # Drain queue first so we only see the new result.
    while not p._queue.empty():
        try:
            p._queue.get_nowait()
        except Exception:
            break
    asyncio.run(p._send_message(None, text, req_id="test-req"))
    # Pull the first send_result from the queue.
    while not p._queue.empty():
        kind, payload = p._queue.get_nowait()
        if kind == "send_result":
            return payload
    return None


class TestHeardCount(unittest.TestCase):
    """(a)/(b) count increments correctly."""

    def setUp(self):
        self.p = _plugin()
        _clear_heard(self.p)
        # Prevent advert hops from being written to stats
        with self.p._rx_log_lock:
            self.p._rx_log.clear()

    def tearDown(self):
        _clear_heard(self.p)

    def test_new_node_count_is_one_after_first_advert(self):
        pk = "aabbccdd00001111222233334444aaaa"
        self.p._on_rx_log(_make_advert_event(pk))
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(pk)
        self.assertIsNotNone(h, "Node must be recorded")
        self.assertEqual(h["count"], 1)

    def test_count_increments_on_repeated_adverts(self):
        pk = "aabbccdd00001111222233334444bbbb"
        for _ in range(3):
            self.p._on_rx_log(_make_advert_event(pk))
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(pk)
        self.assertEqual(h["count"], 3)

    def test_existing_node_without_count_gets_one_on_next_advert(self):
        """Nodes loaded from old JSON (no 'count' key) start at 1 on next advert."""
        pk = "aabbccdd00001111222233334444cccc"
        with self.p._rx_log_lock:
            # Simulate an older persisted entry with no count field
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": time.time() - 100,
                "name": "Old", "type": 2,
            }
        self.p._on_rx_log(_make_advert_event(pk))
        with self.p._rx_log_lock:
            h = self.p._heard_nodes.get(pk)
        self.assertEqual(h["count"], 1)

    def test_count_in_build_heard_payload(self):
        """count must appear in the payload built for persistence/WS push."""
        pk = "aabbccdd00001111222233334444dddd"
        self.p._on_rx_log(_make_advert_event(pk))
        self.p._on_rx_log(_make_advert_event(pk))
        payload = self.p._build_heard_payload()
        self.assertEqual(payload["nodes"][pk]["count"], 2)


class TestHeardPruneAge(unittest.TestCase):
    """(c) Age-based prune: week, month, year."""

    NOW = time.time()

    def setUp(self):
        self.p = _plugin()
        _clear_heard(self.p)

    def tearDown(self):
        _clear_heard(self.p)

    def _insert(self, pk, last_heard, count=5):
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": last_heard - 10,
                "last_heard": last_heard, "count": count, "name": "X", "type": 2,
            }

    def test_prune_week_removes_old_keeps_recent(self):
        old_pk  = "deadbeef0000111122223333000011aa"
        new_pk  = "deadbeef0000111122223333000022bb"
        self._insert(old_pk, self.NOW - 8 * 86400)   # 8 days ago → remove
        self._insert(new_pk, self.NOW - 3 * 86400)   # 3 days ago → keep
        result = _run_prune("!heard_prune week")
        self.assertIsNotNone(result)
        self.assertTrue(result["ok"])
        self.assertEqual(result["target"], "!heard_prune")
        with self.p._rx_log_lock:
            self.assertNotIn(old_pk, self.p._heard_nodes)
            self.assertIn(new_pk, self.p._heard_nodes)
        self.assertEqual(int(result["body"]), 1)
        self.assertIn("1", result["result"])

    def test_prune_month_removes_old_keeps_recent(self):
        old_pk = "deadbeef000011112222333300003300"
        new_pk = "deadbeef000011112222333300004400"
        self._insert(old_pk, self.NOW - 31 * 86400)  # 31 days → remove
        self._insert(new_pk, self.NOW - 10 * 86400)  # 10 days → keep
        result = _run_prune("!heard_prune month")
        with self.p._rx_log_lock:
            self.assertNotIn(old_pk, self.p._heard_nodes)
            self.assertIn(new_pk, self.p._heard_nodes)
        self.assertEqual(int(result["body"]), 1)

    def test_prune_year_removes_old_keeps_recent(self):
        old_pk = "deadbeef000011112222333300005500"
        new_pk = "deadbeef000011112222333300006600"
        self._insert(old_pk, self.NOW - 366 * 86400)  # >1 year → remove
        self._insert(new_pk, self.NOW - 100 * 86400)  # 100 days → keep
        result = _run_prune("!heard_prune year")
        with self.p._rx_log_lock:
            self.assertNotIn(old_pk, self.p._heard_nodes)
            self.assertIn(new_pk, self.p._heard_nodes)
        self.assertEqual(int(result["body"]), 1)

    def test_prune_no_last_heard_keeps_entry(self):
        """Entries missing last_heard must be skipped (kept) for age criteria."""
        pk = "deadbeef000011112222333300007700"
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": self.NOW - 400 * 86400,
                "count": 3, "name": "NoTs", "type": 2,
                # deliberately omit last_heard
            }
        result = _run_prune("!heard_prune year")
        with self.p._rx_log_lock:
            self.assertIn(pk, self.p._heard_nodes, "Entry without last_heard must be kept")
        self.assertEqual(int(result["body"]), 0)

    def test_prune_returns_correct_count_multiple(self):
        pks = [f"deadbeef0000111122223333{i:08x}" for i in range(4)]
        # 3 old, 1 recent
        for pk in pks[:3]:
            self._insert(pk, self.NOW - 400 * 86400)
        self._insert(pks[3], self.NOW - 1 * 86400)
        result = _run_prune("!heard_prune year")
        self.assertEqual(int(result["body"]), 3)
        with self.p._rx_log_lock:
            self.assertIn(pks[3], self.p._heard_nodes)
            for pk in pks[:3]:
                self.assertNotIn(pk, self.p._heard_nodes)


class TestHeardPruneOnce(unittest.TestCase):
    """(d) once: removes count<=1, keeps count>=2."""

    def setUp(self):
        self.p = _plugin()
        _clear_heard(self.p)

    def tearDown(self):
        _clear_heard(self.p)

    def _insert(self, pk, count):
        now = time.time()
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": now - 100,
                "last_heard": now - 10, "count": count, "name": "X", "type": 2,
            }

    def test_prune_once_removes_count_one(self):
        pk1 = "face000000001111222233334444aa01"
        pk2 = "face000000001111222233334444bb02"
        self._insert(pk1, 1)   # count=1 → remove
        self._insert(pk2, 3)   # count=3 → keep
        result = _run_prune("!heard_prune once")
        self.assertTrue(result["ok"])
        with self.p._rx_log_lock:
            self.assertNotIn(pk1, self.p._heard_nodes)
            self.assertIn(pk2, self.p._heard_nodes)
        self.assertEqual(int(result["body"]), 1)

    def test_prune_once_removes_missing_count(self):
        """Entry with no count key is treated as count=1 (eligible)."""
        pk = "face000000001111222233334444cc03"
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": time.time() - 100,
                "last_heard": time.time() - 10, "name": "X", "type": 2,
                # no count key
            }
        result = _run_prune("!heard_prune once")
        with self.p._rx_log_lock:
            self.assertNotIn(pk, self.p._heard_nodes)
        self.assertEqual(int(result["body"]), 1)

    def test_prune_once_keeps_count_two(self):
        pk = "face000000001111222233334444dd04"
        self._insert(pk, 2)
        result = _run_prune("!heard_prune once")
        with self.p._rx_log_lock:
            self.assertIn(pk, self.p._heard_nodes)
        self.assertEqual(int(result["body"]), 0)


class TestHeardPruneValidation(unittest.TestCase):
    """(e) Unknown criteria → ok:false, nothing removed."""

    def setUp(self):
        self.p = _plugin()
        _clear_heard(self.p)

    def tearDown(self):
        _clear_heard(self.p)

    def _insert(self, pk):
        now = time.time()
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": now - 100,
                "last_heard": now - 400 * 86400, "count": 1, "name": "X", "type": 2,
            }

    def test_unknown_criteria_returns_ok_false(self):
        pk = "baadcafe000000001111222233334444"
        self._insert(pk)
        result = _run_prune("!heard_prune bogus")
        self.assertIsNotNone(result)
        self.assertFalse(result["ok"])
        with self.p._rx_log_lock:
            self.assertIn(pk, self.p._heard_nodes, "Nothing must be removed for unknown criteria")

    def test_unknown_criteria_result_mentions_valid_options(self):
        result = _run_prune("!heard_prune bogus")
        self.assertIn("week", result["result"])

    def test_target_field_is_heard_prune(self):
        result = _run_prune("!heard_prune bogus")
        self.assertEqual(result["target"], "!heard_prune")


class TestHeardPruneDirtyFlags(unittest.TestCase):
    """(f) Dirty flags set + _write_heard called only when >0 removed."""

    def setUp(self):
        self.p = _plugin()
        _clear_heard(self.p)
        self._write_calls = []
        self._orig_write = self.p._write_heard
        self.p._write_heard = lambda: self._write_calls.append(1) or self._orig_write()

    def tearDown(self):
        self.p._write_heard = self._orig_write
        _clear_heard(self.p)

    def _insert_old(self, pk):
        with self.p._rx_log_lock:
            self.p._heard_nodes[pk] = {
                "pubkey": pk, "first_heard": time.time() - 400 * 86400,
                "last_heard": time.time() - 400 * 86400,
                "count": 1, "name": "X", "type": 2,
            }

    def test_dirty_flags_set_when_something_removed(self):
        pk = "dirtyflag000000001111222233334444"
        self._insert_old(pk)
        self.p._heard_dirty = False
        self.p._ws_heard_dirty = False
        _run_prune("!heard_prune year")
        self.assertTrue(self.p._heard_dirty)
        self.assertTrue(self.p._ws_heard_dirty)
        self.assertEqual(len(self._write_calls), 1)

    def test_dirty_flags_not_set_when_nothing_removed(self):
        """No matching entries → flags stay False, _write_heard not called."""
        self.p._heard_dirty = False
        self.p._ws_heard_dirty = False
        _run_prune("!heard_prune year")
        self.assertFalse(self.p._heard_dirty)
        self.assertFalse(self.p._ws_heard_dirty)
        self.assertEqual(len(self._write_calls), 0)


if __name__ == "__main__":
    unittest.main()
