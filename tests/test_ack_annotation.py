"""Tests for delivery-ACK annotation on outgoing DM inbox lines.

Covers:
  - _inbox_line: ~a token round-trip (ack=True, ack=False, ack=None/missing)
  - _inbox_line back-compat: lines without ~a parse identically to before
  - _annotate_sent_line: rewrites existing line with ~a token, idempotent
  - _sweep_pending_acks: timeout → ack_result(delivered=False) queued
  - _on_ack correlation: matched pending record removed, ack_result queued
  - No annotation for channel sends (ack parameter absent on chan lines)
"""
import _bootstrap  # noqa: F401
import time
import unittest

import plugin


L = plugin.BasePlugin._inbox_line
A = plugin.BasePlugin._annotate_sent_line


# ── _inbox_line ~a token ──────────────────────────────────────────────────────

class TestInboxLineAckToken(unittest.TestCase):

    def test_ack_true_appends_a1(self):
        line = L("P", "> Bob", "hi", 1700000000, ack=True)
        self.assertIn("|~a1]", line)

    def test_ack_false_appends_a0(self):
        line = L("P", "> Bob", "hi", 1700000000, ack=False)
        self.assertIn("|~a0]", line)

    def test_ack_none_no_token(self):
        line = L("P", "> Bob", "hi", 1700000000, ack=None)
        self.assertNotIn("~a", line)

    def test_ack_absent_no_token(self):
        """Calling _inbox_line without ack kwarg must never emit ~a (back-compat)."""
        line = L("P", "> Bob", "hi", 1700000000)
        self.assertNotIn("~a", line)

    def test_ack_token_after_path(self):
        """~a must appear after ~p when both are present."""
        line = L("P", "> Bob", "hi", 1700000000, path="aabbcc", ack=True)
        meta = line[1:line.index("]")]
        parts = meta.split("|")
        p_idx = next(i for i, s in enumerate(parts) if s.startswith("~p"))
        a_idx = next(i for i, s in enumerate(parts) if s.startswith("~a"))
        self.assertGreater(a_idx, p_idx)

    def test_full_token_order(self):
        """Token order: epoch [x] ~h ~s ~r ~p ~a."""
        line = L("P", "> B", "body", 1700000000, bad=True,
                 hops=2, snr=5.0, rssi=-80, path="aabb", ack=True)
        meta = line[1:line.index("]")]
        parts = meta.split("|")
        self.assertEqual(parts[0], "P")
        self.assertEqual(parts[1], "> B")
        self.assertEqual(parts[2], "1700000000")
        self.assertEqual(parts[3], "x")
        self.assertIn("~h2",    parts)
        self.assertIn("~s5.0",  parts)
        self.assertIn("~r-80",  parts)
        self.assertIn("~paabb", parts)
        self.assertIn("~a1",    parts)

    def test_back_compat_no_ack_token(self):
        """A line built without ack= must be identical to the pre-ack format."""
        line_new = L("P", "Alice", "hello", 1715800000)
        self.assertEqual(line_new, "[P|Alice|1715800000] hello")

    def test_back_compat_existing_tokens_unchanged(self):
        """Pre-existing tokens (hops, snr, rssi, path) are unaffected when ack is absent."""
        line = L("P", "Alice", "hi", 1700000000, hops=3, snr=6.0, rssi=-75)
        self.assertIn("|~h3", line)
        self.assertIn("|~s6.0", line)
        self.assertIn("|~r-75", line)
        self.assertNotIn("~a", line)


# ── _annotate_sent_line ───────────────────────────────────────────────────────

class TestAnnotateSentLine(unittest.TestCase):

    def _base(self):
        return L("P", "> Bob", "hello", 1700000000)

    def test_adds_a1_to_plain_line(self):
        annotated = A(self._base(), delivered=True)
        self.assertIn("|~a1]", annotated)
        self.assertIn("] hello", annotated)

    def test_adds_a0_to_plain_line(self):
        annotated = A(self._base(), delivered=False)
        self.assertIn("|~a0]", annotated)

    def test_replaces_existing_a1_with_a0(self):
        line = L("P", "> Bob", "hello", 1700000000, ack=True)
        annotated = A(line, delivered=False)
        self.assertIn("|~a0]", annotated)
        self.assertNotIn("~a1", annotated)

    def test_replaces_existing_a0_with_a1(self):
        line = L("P", "> Bob", "hello", 1700000000, ack=False)
        annotated = A(line, delivered=True)
        self.assertIn("|~a1]", annotated)
        self.assertNotIn("~a0", annotated)

    def test_body_preserved(self):
        annotated = A(self._base(), delivered=True)
        self.assertTrue(annotated.endswith("] hello"), annotated)

    def test_all_other_tokens_preserved(self):
        line = L("P", "> Bob", "hello", 1700000000, hops=2, snr=5.0, rssi=-80, path="aabb")
        annotated = A(line, delivered=True)
        self.assertIn("|~h2", annotated)
        self.assertIn("|~s5.0", annotated)
        self.assertIn("|~r-80", annotated)
        self.assertIn("|~paabb", annotated)
        self.assertIn("|~a1]", annotated)

    def test_empty_line_returns_empty(self):
        self.assertEqual(A("", delivered=True), "")

    def test_malformed_line_returned_unchanged(self):
        bad = "no meta block here"
        self.assertEqual(A(bad, delivered=True), bad)

    def test_only_one_a_token_after_annotation(self):
        line = L("P", "> Bob", "hello", 1700000000, ack=True)
        annotated = A(line, delivered=True)
        self.assertEqual(annotated.count("~a"), 1)


# ── _sweep_pending_acks (timeout path) ───────────────────────────────────────

class TestSweepPendingAcks(unittest.TestCase):

    def setUp(self):
        self.p = plugin.BasePlugin.__new__(plugin.BasePlugin)
        # Minimal init for the attributes we need
        import threading
        import queue
        self.p._rx_log_lock   = threading.Lock()
        self.p._pending_acks  = {}
        self.p._queue         = queue.Queue()

    def test_expired_record_removed_and_queued(self):
        """A record older than DM_ACK_TIMEOUT_S is expired → ack_result queued."""
        old_ts = time.time() - plugin.DM_ACK_TIMEOUT_S - 1
        self.p._pending_acks["deadbeef"] = {
            "target": "Bob", "body": "hi",
            "out_ts": old_ts,
            "inbox_line": "[P|> Bob|1700000000] hi",
            "dm_name": "Bob",
        }
        self.p._sweep_pending_acks()
        # Record removed
        self.assertNotIn("deadbeef", self.p._pending_acks)
        # ack_result queued
        item = self.p._queue.get_nowait()
        self.assertEqual(item[0], "ack_result")
        self.assertFalse(item[1]["delivered"])
        self.assertEqual(item[1]["target"], "Bob")

    def test_fresh_record_not_expired(self):
        """A record younger than DM_ACK_TIMEOUT_S is not touched."""
        self.p._pending_acks["cafebabe"] = {
            "target": "Alice", "body": "yo",
            "out_ts": time.time(),
            "inbox_line": "[P|> Alice|1700000000] yo",
            "dm_name": "Alice",
        }
        self.p._sweep_pending_acks()
        self.assertIn("cafebabe", self.p._pending_acks)
        self.assertTrue(self.p._queue.empty())

    def test_record_without_inbox_line_not_queued(self):
        """A record without inbox_line (send_msg returned MSG_SENT before the
        main thread back-filled it) is expired silently without queueing."""
        old_ts = time.time() - plugin.DM_ACK_TIMEOUT_S - 1
        self.p._pending_acks["00000001"] = {
            "target": "Charlie", "body": "hey",
            "out_ts": old_ts,
            # inbox_line absent — send_result echo hasn't run yet
        }
        self.p._sweep_pending_acks()
        self.assertNotIn("00000001", self.p._pending_acks)
        self.assertTrue(self.p._queue.empty())

    def test_multiple_records_mixed_age(self):
        now = time.time()
        self.p._pending_acks["expired1"] = {
            "target": "A", "body": "x",
            "out_ts": now - plugin.DM_ACK_TIMEOUT_S - 5,
            "inbox_line": "[P|> A|1700000000] x", "dm_name": "A",
        }
        self.p._pending_acks["expired2"] = {
            "target": "B", "body": "y",
            "out_ts": now - plugin.DM_ACK_TIMEOUT_S - 1,
            "inbox_line": "[P|> B|1700000000] y", "dm_name": "B",
        }
        self.p._pending_acks["fresh1"] = {
            "target": "C", "body": "z",
            "out_ts": now - 1,
            "inbox_line": "[P|> C|1700000000] z", "dm_name": "C",
        }
        self.p._sweep_pending_acks()
        self.assertNotIn("expired1", self.p._pending_acks)
        self.assertNotIn("expired2", self.p._pending_acks)
        self.assertIn("fresh1", self.p._pending_acks)
        items = []
        while not self.p._queue.empty():
            items.append(self.p._queue.get_nowait())
        self.assertEqual(len(items), 2)
        self.assertTrue(all(it[0] == "ack_result" for it in items))


# ── _on_ack correlation ───────────────────────────────────────────────────────

class TestOnAckCorrelation(unittest.TestCase):
    """_on_ack: matching expected_ack code pops the pending record and queues
    ack_result(delivered=True); non-matching code leaves the record intact."""

    def _make_plugin(self):
        import threading
        import queue
        import collections
        p = plugin.BasePlugin.__new__(plugin.BasePlugin)
        p._rx_log_lock            = threading.Lock()
        p._pending_acks           = {}
        p._queue                  = queue.Queue()
        p._rx_log                 = collections.deque(maxlen=250)
        p._rx_log_total_appended  = 0
        p._payload_type_counts    = collections.defaultdict(int)
        p._packet_times           = collections.deque(maxlen=10000)
        p._rx_log_dirty           = False
        return p

    def _make_event(self, code):
        # Duck-typed stand-in for meshcore.events.Event — _on_ack only reads
        # .payload (and .attributes). Avoids importing the `meshcore` package,
        # which isn't available in the unit-test environment.
        class _Ev:
            def __init__(self, c):
                self.type = "ACK"
                self.payload = {"code": c}
                self.attributes = {"code": c}
        return _Ev(code)

    def test_matching_ack_pops_record_and_queues_result(self):
        p = self._make_plugin()
        p._pending_acks["aabbccdd"] = {
            "target": "Bob", "body": "hello",
            "out_ts": time.time(),
            "inbox_line": "[P|> Bob|1700000000] hello",
            "dm_name": "Bob",
        }
        p._on_ack(self._make_event("aabbccdd"))
        self.assertNotIn("aabbccdd", p._pending_acks)
        item = p._queue.get_nowait()
        self.assertEqual(item[0], "ack_result")
        self.assertTrue(item[1]["delivered"])
        self.assertEqual(item[1]["target"], "Bob")
        self.assertEqual(item[1]["inbox_line"], "[P|> Bob|1700000000] hello")

    def test_non_matching_ack_leaves_pending_intact(self):
        p = self._make_plugin()
        p._pending_acks["aabbccdd"] = {
            "target": "Bob", "body": "hello",
            "out_ts": time.time(),
            "inbox_line": "[P|> Bob|1700000000] hello",
            "dm_name": "Bob",
        }
        p._on_ack(self._make_event("11223344"))   # wrong code
        self.assertIn("aabbccdd", p._pending_acks)
        self.assertTrue(p._queue.empty())

    def test_ack_still_recorded_in_rx_log_regardless_of_match(self):
        p = self._make_plugin()
        p._on_ack(self._make_event("cafecafe"))
        self.assertEqual(len(p._rx_log), 1)
        entry = list(p._rx_log)[0]
        self.assertEqual(entry.get("payload_typename"), "ACK")

    def test_no_pending_records_ack_is_ignored_cleanly(self):
        p = self._make_plugin()
        # Must not raise even with an empty pending dict
        try:
            p._on_ack(self._make_event("00000000"))
        except Exception as exc:
            self.fail(f"_on_ack raised with no pending records: {exc}")
        self.assertTrue(p._queue.empty())


if __name__ == "__main__":
    unittest.main()
