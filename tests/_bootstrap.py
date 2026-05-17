"""Idempotent sys.path fix so each test module also works under a plain
``python -m unittest`` invocation, not only via run_tests.py.
Import this first in every test module.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_PLUGIN_DIR = os.path.dirname(_HERE)
_STUBS = os.path.join(_HERE, "_stubs")

for _p in (_STUBS, _PLUGIN_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Guarantee the stub wins even if a real DomoticzEx is importable.
if sys.path[0] != _STUBS:
    sys.path.insert(0, _STUBS)
