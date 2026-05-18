"""Run the MeshCore plugin's standalone unit tests.

Usage:  python tests/run_tests.py

No third-party deps (stdlib ``unittest`` only — Domoticz ships no test
framework). The ``_stubs`` dir is put first on ``sys.path`` so
``import DomoticzEx`` resolves to the stub, and the plugin dir is added so
``import plugin`` works. Exits non-zero if anything fails.
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
PLUGIN_DIR = os.path.dirname(HERE)
STUBS = os.path.join(HERE, "_stubs")

# Stub MUST win over any real DomoticzEx; plugin dir for `import plugin`;
# HERE so test modules can `import _bootstrap`.
sys.path.insert(0, STUBS)
sys.path.insert(1, PLUGIN_DIR)
sys.path.insert(2, HERE)

# These match the test_*.py glob but are NOT unittest tests — they are
# manual hardware probe scripts (need a real serial port + the `meshcore`
# package + a live device; run them by hand, e.g. `python tests/test_serial.py
# COM6 115200`). Exclude them from the automated suite so discovery doesn't
# surface them as import ERRORS.
MANUAL_SCRIPTS = {"test_serial", "test_rx_log", "test_flood_scope"}


def _strip_manual(suite):
    """Return a flat TestSuite with manual hardware scripts removed."""
    keep = unittest.TestSuite()
    for t in suite:
        if isinstance(t, unittest.TestSuite):
            keep.addTest(_strip_manual(t))
        else:
            mod = type(t).__module__ or ""
            tid = t.id()
            if mod in MANUAL_SCRIPTS or any(
                tid == m or tid.endswith("." + m) or m in tid.split(".")
                for m in MANUAL_SCRIPTS
            ):
                continue
            keep.addTest(t)
    return keep


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = _strip_manual(loader.discover(start_dir=HERE, pattern="test_*.py"))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
