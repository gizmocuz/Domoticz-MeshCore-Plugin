"""Unit tests for BasePlugin._inbox_line — the inbox/conversation wire
format. Pure static logic, no Domoticz/socket needed. Guards the
backward-compatible token scheme the dashboard parser relies on.
"""
import _bootstrap  # noqa: F401
import unittest
import plugin

L = plugin.BasePlugin._inbox_line


class InboxLine(unittest.TestCase):
    def test_minimal(self):
        self.assertEqual(L("#test", "Alice", "hi", 1700000000),
                         "[#test|Alice|1700000000] hi")

    def test_private_tag_and_float_ts_coerced(self):
        self.assertEqual(L("P", "Bob", "yo", 1700000000.9),
                         "[P|Bob|1700000000] yo")

    def test_bad_clock_flag(self):
        self.assertEqual(L("P", "Bob", "x", 1700000000, bad=True),
                         "[P|Bob|1700000000|x] x")

    def test_hops_only_when_non_negative_int(self):
        self.assertIn("|~h0]", L("P", "B", "m", 1, hops=0))
        self.assertIn("|~h5]", L("P", "B", "m", 1, hops=5))
        self.assertNotIn("~h", L("P", "B", "m", 1, hops=-1))
        self.assertNotIn("~h", L("P", "B", "m", 1, hops=None))

    def test_snr_rounded_two_dp(self):
        self.assertIn("|~s5.5]", L("P", "B", "m", 1, snr=5.5))
        self.assertIn("|~s4.25]", L("P", "B", "m", 1, snr=4.25))
        self.assertIn("|~s3.0]", L("P", "B", "m", 1, snr="3"))
        self.assertNotIn("~s", L("P", "B", "m", 1, snr=None))
        self.assertNotIn("~s", L("P", "B", "m", 1, snr="not-a-number"))

    def test_rssi_int(self):
        self.assertIn("|~r-88]", L("P", "B", "m", 1, rssi=-88))
        self.assertNotIn("~r", L("P", "B", "m", 1, rssi=None))

    def test_path_hex_sanitized_and_lowercased(self):
        out = L("P", "B", "m", 1, path="66:1B>72-21>CC")
        self.assertIn("|~p661b7221cc]", out)
        # empty/garbage path → no token
        self.assertNotIn("~p", L("P", "B", "m", 1, path=">>--::"))
        self.assertNotIn("~p", L("P", "B", "m", 1, path=None))

    def test_token_order_epoch_x_h_s_r_p(self):
        out = L("#c", "S", "body", 1700000000, bad=True,
                snr=4.25, hops=3, rssi=-90, path="ab>cd")
        meta = out[1:out.index("]")]
        parts = meta.split("|")
        # chan, sender, epoch, then x, ~h, ~s, ~r, ~p in this order
        self.assertEqual(parts[0], "#c")
        self.assertEqual(parts[1], "S")
        self.assertEqual(parts[2], "1700000000")
        self.assertEqual(parts[3:], ["x", "~h3", "~s4.25", "~r-90", "~pabcd"])
        self.assertTrue(out.endswith("] body"))

    def test_backward_compat_no_tokens(self):
        # A line built with no optional args must parse identically to the
        # original pre-token format the dashboard already handled.
        self.assertEqual(L("Public", "NodeX", "hello world", 1715800000),
                         "[Public|NodeX|1715800000] hello world")


if __name__ == "__main__":
    unittest.main()
