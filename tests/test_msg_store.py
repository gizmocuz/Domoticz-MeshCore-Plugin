"""Tests for the SQLite message store and inbox_query/inbox_page WS protocol.

All DB files land in a temporary directory.  On Windows SQLite holds the file
open until the connection is closed, so every test that creates a BasePlugin
with an open store must call ``_close_store(bp)`` *before* the temp-dir cleanup
runs.  Tests that use setUp/tearDown close in tearDown; inline tests close
explicitly before the TemporaryDirectory context exits.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import os
import shutil
import sqlite3
import tempfile
import unittest

import DomoticzEx as _Domoticz_stub


# ── helpers ───────────────────────────────────────────────────────────────────

def _close_store(bp):
    """Close the SQLite connection on *bp* so Windows releases the file lock."""
    if bp._msgdb is not None:
        try:
            bp._msgdb.close()
        except Exception:
            pass
        bp._msgdb = None


def _make_store(tmp_dir):
    """Return a BasePlugin instance with a fresh in-temp-dir message store."""
    import plugin
    bp = plugin.BasePlugin()
    db_path = os.path.join(tmp_dir, "test_messages.db")
    bp._msg_store_open(db_path)
    return bp


def _insert_n(bp, n, *, chan="General", sender="Alice", body_prefix="msg"):
    """Insert *n* rows and return the list of rowids."""
    rowids = []
    for i in range(n):
        rid = bp._msg_store_add(
            chan=chan, sender=sender,
            body=f"{body_prefix} {i}", epoch=1_700_000_000 + i,
        )
        rowids.append(rid)
    return rowids


def _send_ws(raw):
    """Drive the module-level onWebSocketMessage hook with a JSON string or dict."""
    import plugin
    payload = json.dumps(raw) if isinstance(raw, dict) else raw
    plugin.onWebSocketMessage(payload)


# ── 1. Schema creation ────────────────────────────────────────────────────────

class TestMsgStoreSchema(unittest.TestCase):

    def test_schema_created_idempotent(self):
        """Opening the same DB twice must not raise (CREATE IF NOT EXISTS)."""
        tmp = tempfile.mkdtemp()
        import plugin
        bp = plugin.BasePlugin()
        try:
            db_path = os.path.join(tmp, "msgs.db")
            bp._msg_store_open(db_path)
            self.assertIsNotNone(bp._msgdb)
            # Open again on same path — idempotent.
            bp._msg_store_open(db_path)
            self.assertIsNotNone(bp._msgdb)
        finally:
            _close_store(bp)
            shutil.rmtree(tmp, ignore_errors=True)

    def test_add_returns_increasing_rowids(self):
        tmp = tempfile.mkdtemp()
        bp = _make_store(tmp)
        try:
            r1 = bp._msg_store_add("P", "Alice", "hello", 1_700_000_000)
            r2 = bp._msg_store_add("P", "Bob",   "world", 1_700_000_001)
            r3 = bp._msg_store_add("P", "Alice", "bye",   1_700_000_002)
            self.assertIsNotNone(r1)
            self.assertIsNotNone(r2)
            self.assertIsNotNone(r3)
            self.assertLess(r1, r2)
            self.assertLess(r2, r3)
        finally:
            _close_store(bp)
            shutil.rmtree(tmp, ignore_errors=True)

    def test_recv_ts_is_set_on_insert(self):
        import time
        tmp = tempfile.mkdtemp()
        bp = _make_store(tmp)
        try:
            before = int(time.time())
            bp._msg_store_add("P", "Alice", "ts-test", 1_700_000_000)
            after = int(time.time())
            with bp._msgdb_lock:
                cur = bp._msgdb.execute("SELECT recv_ts FROM messages ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertGreaterEqual(row[0], before)
            self.assertLessEqual(row[0], after)
        finally:
            _close_store(bp)
            shutil.rmtree(tmp, ignore_errors=True)


# ── 2. Pagination ─────────────────────────────────────────────────────────────

class TestMsgStorePagination(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bp = _make_store(self.tmp)
        # Insert 130 rows in order.
        self.rowids = _insert_n(self.bp, 130, chan="General")

    def tearDown(self):
        _close_store(self.bp)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_first_page_newest_50(self):
        page = self.bp._msg_store_query("General", before=None, limit=50)
        self.assertEqual(len(page["rows"]), 50)
        self.assertTrue(page["has_more"])
        # Rows are newest-first: first row should have highest id.
        ids = [r["id"] for r in page["rows"]]
        self.assertEqual(ids, sorted(ids, reverse=True))
        # The newest 50 rowids are the last 50 inserted.
        expected_top = self.rowids[-1]
        self.assertEqual(ids[0], expected_top)

    def test_second_page_no_overlap(self):
        page1 = self.bp._msg_store_query("General", before=None, limit=50)
        oldest1 = page1["oldest_id"]
        page2 = self.bp._msg_store_query("General", before=oldest1, limit=50)
        self.assertEqual(len(page2["rows"]), 50)
        self.assertTrue(page2["has_more"])
        ids1 = {r["id"] for r in page1["rows"]}
        ids2 = {r["id"] for r in page2["rows"]}
        self.assertTrue(ids1.isdisjoint(ids2), "Pages must not overlap")

    def test_third_page_has_more_false(self):
        page1 = self.bp._msg_store_query("General", before=None, limit=50)
        page2 = self.bp._msg_store_query("General", before=page1["oldest_id"], limit=50)
        page3 = self.bp._msg_store_query("General", before=page2["oldest_id"], limit=50)
        # 130 rows, 50 per page → page3 has 30 rows, no more.
        self.assertEqual(len(page3["rows"]), 30)
        self.assertFalse(page3["has_more"])

    def test_three_pages_cover_all_rows_no_gaps(self):
        page1 = self.bp._msg_store_query("General", before=None, limit=50)
        page2 = self.bp._msg_store_query("General", before=page1["oldest_id"], limit=50)
        page3 = self.bp._msg_store_query("General", before=page2["oldest_id"], limit=50)
        all_ids = (
            {r["id"] for r in page1["rows"]}
            | {r["id"] for r in page2["rows"]}
            | {r["id"] for r in page3["rows"]}
        )
        self.assertEqual(len(all_ids), 130)
        self.assertEqual(all_ids, set(self.rowids))

    def test_oldest_id_matches_last_row(self):
        page = self.bp._msg_store_query("General", before=None, limit=50)
        self.assertEqual(page["oldest_id"], page["rows"][-1]["id"])

    def test_empty_store_returns_empty_page(self):
        tmp2 = tempfile.mkdtemp()
        bp2 = _make_store(tmp2)
        try:
            page = bp2._msg_store_query("all", before=None, limit=50)
            self.assertEqual(page["rows"], [])
            self.assertFalse(page["has_more"])
            self.assertIsNone(page["oldest_id"])
        finally:
            _close_store(bp2)
            shutil.rmtree(tmp2, ignore_errors=True)


# ── 3. Scope filter ───────────────────────────────────────────────────────────

class TestMsgStoreScopeFilter(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bp = _make_store(self.tmp)
        _insert_n(self.bp, 5, chan="P",       sender="Bob")
        _insert_n(self.bp, 7, chan="General", sender="Alice")
        _insert_n(self.bp, 3, chan="C0",      sender="Charlie")

    def tearDown(self):
        _close_store(self.bp)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_scope_P_returns_only_P(self):
        page = self.bp._msg_store_query("P")
        self.assertEqual(len(page["rows"]), 5)
        for r in page["rows"]:
            self.assertEqual(r["chan"], "P")

    def test_scope_named_channel(self):
        page = self.bp._msg_store_query("General")
        self.assertEqual(len(page["rows"]), 7)
        for r in page["rows"]:
            self.assertEqual(r["chan"], "General")

    def test_scope_C0(self):
        page = self.bp._msg_store_query("C0")
        self.assertEqual(len(page["rows"]), 3)

    def test_scope_all_returns_all(self):
        page = self.bp._msg_store_query("all", limit=200)
        self.assertEqual(len(page["rows"]), 15)

    def test_scope_nonexistent_returns_empty(self):
        page = self.bp._msg_store_query("NoSuchChan")
        self.assertEqual(page["rows"], [])
        self.assertFalse(page["has_more"])


# ── 4. Search ─────────────────────────────────────────────────────────────────

class TestMsgStoreSearch(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bp = _make_store(self.tmp)
        self.bp._msg_store_add("General", "Alice",   "Hello world",    1_700_000_000)
        self.bp._msg_store_add("General", "Bob",     "Goodbye world",  1_700_000_001)
        self.bp._msg_store_add("General", "Charlie", "Hello Charlie",  1_700_000_002)
        self.bp._msg_store_add("P",       "Alice",   "Private hello",  1_700_000_003)
        # Row with special LIKE metacharacters in body and sender.
        self.bp._msg_store_add("General", "100%_sure", "50% done_now", 1_700_000_004)

    def tearDown(self):
        _close_store(self.bp)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_search_body_substring(self):
        page = self.bp._msg_store_query("all", search="Hello")
        bodies = [r["body"] for r in page["rows"]]
        # Matches "Hello world", "Hello Charlie", "Private hello" (case-insensitive)
        self.assertEqual(len(page["rows"]), 3, f"Expected 3 rows, got: {bodies}")

    def test_search_case_insensitive(self):
        page = self.bp._msg_store_query("all", search="hello")
        self.assertEqual(len(page["rows"]), 3)

    def test_search_sender(self):
        page = self.bp._msg_store_query("all", search="Charlie")
        # Matches sender="Charlie" row (body doesn't contain "Charlie" in row 0/1/3),
        # and body "Hello Charlie" row.
        self.assertGreaterEqual(len(page["rows"]), 1)
        found_names = {r["sender"] for r in page["rows"]} | {r["body"] for r in page["rows"]}
        self.assertTrue(any("Charlie" in x for x in found_names))

    def test_search_no_match_returns_empty(self):
        page = self.bp._msg_store_query("all", search="xyzzy_never_matches_anything")
        self.assertEqual(page["rows"], [])

    def test_search_percent_literal(self):
        """A '%' in the search term must match literal '%', not act as wildcard."""
        page = self.bp._msg_store_query("all", search="50%")
        self.assertEqual(len(page["rows"]), 1)
        self.assertIn("50%", page["rows"][0]["body"])

    def test_search_underscore_literal(self):
        """An '_' in the search term must match literal '_', not any single char."""
        page = self.bp._msg_store_query("all", search="done_now")
        self.assertEqual(len(page["rows"]), 1)
        self.assertIn("done_now", page["rows"][0]["body"])

    def test_search_percent_in_sender(self):
        """'%' in search also matches sender field literally."""
        page = self.bp._msg_store_query("all", search="100%")
        self.assertEqual(len(page["rows"]), 1)
        self.assertIn("100%", page["rows"][0]["sender"])

    def test_search_with_scope_filter(self):
        page = self.bp._msg_store_query("P", search="hello")
        self.assertEqual(len(page["rows"]), 1)
        self.assertEqual(page["rows"][0]["chan"], "P")


# ── 5. ACK update ─────────────────────────────────────────────────────────────

class TestMsgStoreAck(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bp = _make_store(self.tmp)

    def tearDown(self):
        _close_store(self.bp)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _get_ack(self, rowid):
        with self.bp._msgdb_lock:
            cur = self.bp._msgdb.execute("SELECT ack FROM messages WHERE id=?", (rowid,))
            row = cur.fetchone()
        return row[0] if row else None

    def test_ack_initially_none(self):
        rid = self.bp._msg_store_add("P", "Alice", "test", 1_700_000_000)
        self.assertIsNone(self._get_ack(rid))

    def test_set_ack_delivered(self):
        rid = self.bp._msg_store_add("P", "Alice", "test", 1_700_000_000)
        self.bp._msg_store_set_ack(rid, True)
        self.assertEqual(self._get_ack(rid), 1)

    def test_set_ack_not_delivered(self):
        rid = self.bp._msg_store_add("P", "Alice", "test", 1_700_000_000)
        self.bp._msg_store_set_ack(rid, False)
        self.assertEqual(self._get_ack(rid), 0)

    def test_toggle_ack(self):
        rid = self.bp._msg_store_add("P", "Alice", "test", 1_700_000_000)
        self.bp._msg_store_set_ack(rid, True)
        self.assertEqual(self._get_ack(rid), 1)
        self.bp._msg_store_set_ack(rid, False)
        self.assertEqual(self._get_ack(rid), 0)

    def test_set_ack_nonexistent_rowid_does_not_raise(self):
        try:
            self.bp._msg_store_set_ack(999999, True)
        except Exception as exc:
            self.fail(f"_msg_store_set_ack raised on nonexistent rowid: {exc}")

    def test_set_ack_none_rowid_does_not_raise(self):
        try:
            self.bp._msg_store_set_ack(None, True)
        except Exception as exc:
            self.fail(f"_msg_store_set_ack raised on None rowid: {exc}")


# ── 6. Prune cap ──────────────────────────────────────────────────────────────

class TestMsgStorePruneCap(unittest.TestCase):

    def test_prune_keeps_newest_rows(self):
        """After inserting more than CAP rows, only the newest remain."""
        tmp = tempfile.mkdtemp()
        import plugin
        bp = plugin.BasePlugin()
        original_cap = plugin.BasePlugin._MSG_STORE_CAP
        original_every = plugin.BasePlugin._MSG_STORE_PRUNE_EVERY
        try:
            db_path = os.path.join(tmp, "prune.db")
            bp._msg_store_open(db_path)
            plugin.BasePlugin._MSG_STORE_CAP = 10
            plugin.BasePlugin._MSG_STORE_PRUNE_EVERY = 5
            _insert_n(bp, 25)
            # Force one explicit prune.
            with bp._msgdb_lock:
                bp._msgdb.execute(
                    "DELETE FROM messages WHERE id < (SELECT MAX(id) - ? FROM messages)",
                    (plugin.BasePlugin._MSG_STORE_CAP,),
                )
                bp._msgdb.commit()
                cur = bp._msgdb.execute("SELECT COUNT(*) FROM messages")
                count = cur.fetchone()[0]
            # At most CAP + PRUNE_EVERY rows survive (prune fires every PRUNE_EVERY inserts).
            self.assertLessEqual(
                count,
                plugin.BasePlugin._MSG_STORE_CAP + plugin.BasePlugin._MSG_STORE_PRUNE_EVERY,
            )
        finally:
            plugin.BasePlugin._MSG_STORE_CAP = original_cap
            plugin.BasePlugin._MSG_STORE_PRUNE_EVERY = original_every
            _close_store(bp)
            shutil.rmtree(tmp, ignore_errors=True)


# ── 7. DB-failure resilience ──────────────────────────────────────────────────

class TestMsgStoreResilience(unittest.TestCase):

    def test_add_with_no_db_returns_none(self):
        """_msg_store_add must return None (not raise) when _msgdb is None."""
        import plugin
        bp = plugin.BasePlugin()
        bp._msgdb = None
        result = bp._msg_store_add("P", "Alice", "hello", 1_700_000_000)
        self.assertIsNone(result)

    def test_set_ack_with_no_db_does_not_raise(self):
        import plugin
        bp = plugin.BasePlugin()
        bp._msgdb = None
        try:
            bp._msg_store_set_ack(1, True)
        except Exception as exc:
            self.fail(f"_msg_store_set_ack raised with no db: {exc}")

    def test_query_with_no_db_returns_error_page(self):
        import plugin
        bp = plugin.BasePlugin()
        bp._msgdb = None
        page = bp._msg_store_query("all")
        self.assertEqual(page["rows"], [])
        self.assertFalse(page["has_more"])
        self.assertIsNotNone(page.get("error"))

    def test_add_with_closed_db_returns_none_and_does_not_raise(self):
        """Force a sqlite3 error by closing the connection; must return None."""
        tmp = tempfile.mkdtemp()
        import plugin
        bp = plugin.BasePlugin()
        try:
            bp._msg_store_open(os.path.join(tmp, "x.db"))
            # Close the underlying connection to force subsequent operations to fail.
            bp._msgdb.close()
            result = bp._msg_store_add("P", "Alice", "test", 1_700_000_000)
            self.assertIsNone(result)
        finally:
            bp._msgdb = None
            shutil.rmtree(tmp, ignore_errors=True)

    def test_query_with_closed_db_returns_error_page_not_raise(self):
        tmp = tempfile.mkdtemp()
        import plugin
        bp = plugin.BasePlugin()
        try:
            bp._msg_store_open(os.path.join(tmp, "x.db"))
            bp._msgdb.close()
            try:
                page = bp._msg_store_query("all")
            except Exception as exc:
                self.fail(f"_msg_store_query raised on closed db: {exc}")
            self.assertIn("error", page)
            self.assertEqual(page["rows"], [])
        finally:
            bp._msgdb = None
            shutil.rmtree(tmp, ignore_errors=True)


# ── 8. inbox_query → inbox_page WS round-trip ─────────────────────────────────

class TestInboxQueryWS(unittest.TestCase):
    """Drive onWebSocketMessage with inbox_query frames and assert inbox_page replies."""

    def setUp(self):
        _Domoticz_stub.reset_ws()
        import plugin
        self.plugin = plugin
        self._tmp = tempfile.mkdtemp()
        # Point the plugin singleton's store at a temp DB.
        db_path = os.path.join(self._tmp, "ws_test.db")
        plugin._plugin._msg_store_open(db_path)
        # Seed some rows.
        plugin._plugin._msg_store_add("General", "Alice", "hello from alice", 1_700_000_000)
        plugin._plugin._msg_store_add("P",       "Bob",   "private message",  1_700_000_001)
        plugin._plugin._msg_store_add("General", "Alice", "second general",   1_700_000_002)
        # Force _ws_ok = True so _push works.
        plugin._plugin._ws_ok = True

    def tearDown(self):
        # Close the store before removing the temp dir (Windows file lock).
        _close_store(self.plugin._plugin)
        shutil.rmtree(self._tmp, ignore_errors=True)
        _Domoticz_stub.reset_ws()

    def _inbox_pages(self):
        return [r for r in _Domoticz_stub.ws_sent if r.get("t") == "inbox_page"]

    def test_inbox_query_produces_inbox_page(self):
        _send_ws({"t": "inbox_query", "scope": "all", "id": 1})
        pages = self._inbox_pages()
        self.assertEqual(len(pages), 1, f"Expected 1 inbox_page, got: {pages}")

    def test_inbox_page_echoes_id(self):
        _send_ws({"t": "inbox_query", "scope": "all", "id": 42})
        page = self._inbox_pages()[0]
        self.assertEqual(page.get("id"), 42)

    def test_inbox_page_has_required_keys(self):
        _send_ws({"t": "inbox_query", "scope": "all", "id": 1})
        page = self._inbox_pages()[0]
        for key in ("t", "id", "scope", "search", "rows", "has_more", "oldest_id"):
            self.assertIn(key, page, f"inbox_page missing key {key!r}")

    def test_inbox_page_scope_all_returns_all_rows(self):
        _send_ws({"t": "inbox_query", "scope": "all", "id": 1})
        page = self._inbox_pages()[0]
        self.assertEqual(len(page["rows"]), 3)

    def test_inbox_page_scope_filter(self):
        _send_ws({"t": "inbox_query", "scope": "P", "id": 1})
        page = self._inbox_pages()[0]
        self.assertEqual(len(page["rows"]), 1)
        self.assertEqual(page["rows"][0]["chan"], "P")

    def test_inbox_page_search_filter(self):
        _send_ws({"t": "inbox_query", "scope": "all", "search": "private", "id": 1})
        page = self._inbox_pages()[0]
        self.assertEqual(len(page["rows"]), 1)
        self.assertIn("private", page["rows"][0]["body"])

    def test_inbox_page_row_shape(self):
        """Each row must contain the documented fields."""
        _send_ws({"t": "inbox_query", "scope": "all", "id": 1})
        page = self._inbox_pages()[0]
        self.assertGreater(len(page["rows"]), 0)
        row = page["rows"][0]
        for key in ("id", "chan", "sender", "epoch", "bad", "body",
                    "hops", "snr", "rssi", "path", "ack", "dir"):
            self.assertIn(key, row, f"row missing key {key!r}")

    def test_inbox_page_has_more_false_for_small_set(self):
        _send_ws({"t": "inbox_query", "scope": "all", "limit": 50, "id": 1})
        page = self._inbox_pages()[0]
        self.assertFalse(page["has_more"])

    def test_inbox_page_pagination(self):
        """Two pages (limit=2) must cover all 3 rows with correct has_more."""
        _send_ws({"t": "inbox_query", "scope": "all", "limit": 2, "id": 1})
        page1 = self._inbox_pages()[-1]
        self.assertTrue(page1["has_more"])
        self.assertEqual(len(page1["rows"]), 2)

        _Domoticz_stub.reset_ws()
        before = page1["oldest_id"]
        _send_ws({"t": "inbox_query", "scope": "all", "limit": 2, "before": before, "id": 2})
        page2 = self._inbox_pages()[-1]
        self.assertFalse(page2["has_more"])
        self.assertEqual(len(page2["rows"]), 1)

        ids1 = {r["id"] for r in page1["rows"]}
        ids2 = {r["id"] for r in page2["rows"]}
        self.assertTrue(ids1.isdisjoint(ids2))
        self.assertEqual(len(ids1 | ids2), 3)

    def test_inbox_page_no_db_returns_error_frame(self):
        """With no DB open the response frame must contain an 'error' key."""
        orig = self.plugin._plugin._msgdb
        self.plugin._plugin._msgdb = None
        try:
            _send_ws({"t": "inbox_query", "scope": "all", "id": 99})
        finally:
            self.plugin._plugin._msgdb = orig
        pages = self._inbox_pages()
        self.assertEqual(len(pages), 1)
        self.assertIn("error", pages[0])
        self.assertEqual(pages[0]["rows"], [])

    def test_inbox_query_limit_clamped_low(self):
        """limit=0 is clamped to 1; only 1 row returned."""
        _send_ws({"t": "inbox_query", "scope": "all", "limit": 0, "id": 1})
        page = self._inbox_pages()[-1]
        self.assertEqual(len(page["rows"]), 1)

    def test_inbox_query_limit_clamped_high(self):
        """limit=9999 is clamped to 200; all 3 rows returned (< 200)."""
        _send_ws({"t": "inbox_query", "scope": "all", "limit": 9999, "id": 2})
        page = self._inbox_pages()[-1]
        self.assertEqual(len(page["rows"]), 3)


# ── 9. @<name> DM-thread scope ────────────────────────────────────────────────

class TestMsgStoreDMThreadScope(unittest.TestCase):
    """_msg_store_query('@Bob', ...) must return only Bob's DM thread.

    The thread is identified by chan='P' AND sender IN
    ('Bob', '> Bob', '▶Bob', '▶ Bob').  Other contacts' DMs and channel
    messages must be excluded.
    """

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bp = _make_store(self.tmp)
        # Bob's thread — four sender variants.
        self.bp._msg_store_add("P", "Bob",      "hi from bob",       1_700_000_000)
        self.bp._msg_store_add("P", "> Bob",    "reply to bob",      1_700_000_001)
        self.bp._msg_store_add("P", "▶Bob",     "legacy out 1",      1_700_000_002)
        self.bp._msg_store_add("P", "▶ Bob",    "legacy out 2",      1_700_000_003)
        # Unrelated: another contact's DMs.
        self.bp._msg_store_add("P", "Alice",    "hi from alice",     1_700_000_010)
        self.bp._msg_store_add("P", "> Alice",  "reply to alice",    1_700_000_011)
        # Unrelated: channel messages (even if sender happens to be "Bob").
        self.bp._msg_store_add("General", "Bob", "channel msg",      1_700_000_020)
        self.bp._msg_store_add("C0",      "Bob", "another channel",  1_700_000_021)

    def tearDown(self):
        _close_store(self.bp)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_dm_thread_returns_only_bob(self):
        """All four sender variants for Bob are returned; other contacts excluded."""
        page = self.bp._msg_store_query("@Bob", limit=50)
        self.assertEqual(len(page["rows"]), 4, f"Expected 4 rows, got: {[r['sender'] for r in page['rows']]}")
        for r in page["rows"]:
            self.assertEqual(r["chan"], "P")
            self.assertIn(r["sender"], {"Bob", "> Bob", "▶Bob", "▶ Bob"})

    def test_dm_thread_excludes_channel_rows(self):
        """chan='General'/'C0' rows with sender='Bob' must NOT appear in @Bob."""
        page = self.bp._msg_store_query("@Bob", limit=50)
        for r in page["rows"]:
            self.assertNotIn(r["chan"], ("General", "C0"))

    def test_dm_thread_excludes_other_contact(self):
        """Alice's DMs must not appear in @Bob."""
        page = self.bp._msg_store_query("@Bob", limit=50)
        senders = {r["sender"] for r in page["rows"]}
        self.assertNotIn("Alice",    senders)
        self.assertNotIn("> Alice",  senders)

    def test_dm_thread_pagination(self):
        """before / oldest_id pagination must work within the DM thread."""
        page1 = self.bp._msg_store_query("@Bob", limit=2)
        self.assertEqual(len(page1["rows"]), 2)
        self.assertTrue(page1["has_more"])

        page2 = self.bp._msg_store_query("@Bob", limit=2, before=page1["oldest_id"])
        self.assertEqual(len(page2["rows"]), 2)
        self.assertFalse(page2["has_more"])

        ids1 = {r["id"] for r in page1["rows"]}
        ids2 = {r["id"] for r in page2["rows"]}
        self.assertTrue(ids1.isdisjoint(ids2))
        self.assertEqual(len(ids1 | ids2), 4)

    def test_dm_thread_search_within_thread(self):
        """search= must filter within the Bob thread only."""
        page = self.bp._msg_store_query("@Bob", search="legacy", limit=50)
        self.assertEqual(len(page["rows"]), 2)
        for r in page["rows"]:
            self.assertIn("legacy", r["body"])
            self.assertIn(r["sender"], {"▶Bob", "▶ Bob"})

    def test_dm_thread_search_no_match(self):
        page = self.bp._msg_store_query("@Bob", search="xyzzy_never", limit=50)
        self.assertEqual(page["rows"], [])
        self.assertFalse(page["has_more"])

    def test_dm_thread_empty_for_unknown_contact(self):
        """A name that has no messages must return an empty page, not an error."""
        page = self.bp._msg_store_query("@Nobody", limit=50)
        self.assertEqual(page["rows"], [])
        self.assertFalse(page["has_more"])
        self.assertNotIn("error", page)

    def test_dm_thread_oldest_id_is_last_row(self):
        page = self.bp._msg_store_query("@Bob", limit=50)
        self.assertEqual(page["oldest_id"], page["rows"][-1]["id"])

    def test_scope_all_still_returns_all(self):
        """Sanity: adding @-scope rows must not break the 'all' scope."""
        page = self.bp._msg_store_query("all", limit=200)
        self.assertEqual(len(page["rows"]), 8)

    def test_scope_P_still_returns_all_private(self):
        """scope='P' must still return every chan='P' row regardless of sender."""
        page = self.bp._msg_store_query("P", limit=200)
        # 4 Bob + 1 Alice + 1 "> Alice" = 6
        self.assertEqual(len(page["rows"]), 6)
        for r in page["rows"]:
            self.assertEqual(r["chan"], "P")


# ── 10. Preferences table + schema-version migration ─────────────────────────

class TestMsgStorePreferences(unittest.TestCase):
    """Covers preferences table, version seeding, migration, and back-compat."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.bp = _make_store(self.tmp)

    def tearDown(self):
        _close_store(self.bp)
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ── fresh DB ──────────────────────────────────────────────────────────────

    def test_fresh_db_preferences_table_exists(self):
        """After _msg_store_open, the preferences table must exist."""
        with self.bp._msgdb_lock:
            cur = self.bp._msgdb.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='preferences'"
            )
            row = cur.fetchone()
        self.assertIsNotNone(row, "preferences table not found in fresh DB")

    def test_fresh_db_version_is_1(self):
        """A fresh DB must have db_version = '1' after open."""
        ver = self.bp._pref_get("db_version")
        self.assertEqual(ver, "1")

    def test_schema_version_constant_is_1(self):
        import plugin
        self.assertEqual(plugin.BasePlugin.MSG_DB_SCHEMA_VERSION, 1)

    # ── _pref_set / _pref_get round-trip ─────────────────────────────────────

    def test_pref_set_get_roundtrip(self):
        self.bp._pref_set("test_key", "hello")
        self.assertEqual(self.bp._pref_get("test_key"), "hello")

    def test_pref_get_default_on_missing(self):
        result = self.bp._pref_get("nonexistent_key", default="fallback")
        self.assertEqual(result, "fallback")

    def test_pref_get_none_default_on_missing(self):
        result = self.bp._pref_get("nonexistent_key")
        self.assertIsNone(result)

    def test_pref_set_overwrite_on_conflict(self):
        """Setting the same key twice must update, not insert a duplicate."""
        self.bp._pref_set("x", "first")
        self.bp._pref_set("x", "second")
        self.assertEqual(self.bp._pref_get("x"), "second")
        # Confirm only one row for this key.
        with self.bp._msgdb_lock:
            cur = self.bp._msgdb.execute(
                "SELECT COUNT(*) FROM preferences WHERE key='x'"
            )
            count = cur.fetchone()[0]
        self.assertEqual(count, 1)

    # ── back-compat: DB without preferences table ─────────────────────────────

    def test_backcompat_preferences_less_db(self):
        """Simulate the pre-existing dev DB (messages but no preferences).

        Manually drop the preferences table, then re-open the DB.  The plugin
        must recreate the table, set db_version='1', and leave existing
        messages rows untouched.
        """
        # Seed a message so we can verify rows are preserved.
        self.bp._msg_store_add("General", "Alice", "old message", 1_700_000_000)

        # Drop the preferences table to simulate the legacy DB.
        with self.bp._msgdb_lock:
            self.bp._msgdb.execute("DROP TABLE IF EXISTS preferences")
            self.bp._msgdb.commit()

        # Re-open the same DB (idempotent schema creation + migration).
        db_path = self.bp._msgdb.execute("PRAGMA database_list").fetchone()[2]
        _close_store(self.bp)
        self.bp._msg_store_open(db_path)

        # preferences table must now exist.
        with self.bp._msgdb_lock:
            cur = self.bp._msgdb.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='preferences'"
            )
            row = cur.fetchone()
        self.assertIsNotNone(row, "preferences table not recreated after back-compat open")

        # db_version must be '1'.
        self.assertEqual(self.bp._pref_get("db_version"), "1")

        # Existing messages row must still be present.
        with self.bp._msgdb_lock:
            cur = self.bp._msgdb.execute("SELECT COUNT(*) FROM messages")
            count = cur.fetchone()[0]
        self.assertGreaterEqual(count, 1)

    # ── migration idempotency ─────────────────────────────────────────────────

    def test_migration_idempotent(self):
        """Calling _msg_store_migrate twice must still yield version '1', no error."""
        # Seed a message row.
        rid = self.bp._msg_store_add("P", "Bob", "idempotent test", 1_700_000_000)
        self.assertIsNotNone(rid)

        # Run migration a second time explicitly.
        try:
            self.bp._msg_store_migrate()
        except Exception as exc:
            self.fail(f"_msg_store_migrate raised on second call: {exc}")

        self.assertEqual(self.bp._pref_get("db_version"), "1")

        # Message row must be intact.
        with self.bp._msgdb_lock:
            cur = self.bp._msgdb.execute("SELECT COUNT(*) FROM messages WHERE id=?", (rid,))
            count = cur.fetchone()[0]
        self.assertEqual(count, 1)

    # ── _pref_get with no DB ──────────────────────────────────────────────────

    def test_pref_get_no_db_returns_default(self):
        """_pref_get must return the default (not raise) when _msgdb is None."""
        import plugin
        bp = plugin.BasePlugin()
        bp._msgdb = None
        try:
            result = bp._pref_get("db_version", default="sentinel")
        except Exception as exc:
            self.fail(f"_pref_get raised with no db: {exc}")
        self.assertEqual(result, "sentinel")

    def test_pref_get_no_db_none_default(self):
        import plugin
        bp = plugin.BasePlugin()
        bp._msgdb = None
        result = bp._pref_get("anything")
        self.assertIsNone(result)

    def test_pref_set_no_db_does_not_raise(self):
        """_pref_set must silently no-op (not raise) when _msgdb is None."""
        import plugin
        bp = plugin.BasePlugin()
        bp._msgdb = None
        try:
            bp._pref_set("k", "v")
        except Exception as exc:
            self.fail(f"_pref_set raised with no db: {exc}")


if __name__ == "__main__":
    unittest.main()
