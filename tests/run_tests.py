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

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=HERE, pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
