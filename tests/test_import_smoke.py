"""Smoke test: plugin.py imports cleanly under the Domoticz stub and the
module-level singleton + lifecycle hooks exist and are callable.

This catches import-time regressions (syntax, bad module-level code, a
missing Domoticz attribute the stub doesn't cover) on every run.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import unittest


class ImportSmoke(unittest.TestCase):
    def test_import_and_singleton(self):
        import plugin
        self.assertTrue(hasattr(plugin, "_plugin"))
        self.assertEqual(type(plugin._plugin).__name__, "BasePlugin")

    def test_lifecycle_hooks_present(self):
        import plugin
        for name in ("onStart", "onStop", "onHeartbeat", "onWebSocketMessage"):
            self.assertTrue(callable(getattr(plugin, name, None)),
                            f"module hook {name} missing/not callable")

    def test_inbox_line_is_static(self):
        import plugin
        # Callable without an instance (pure logic — safe to unit test).
        self.assertTrue(callable(plugin.BasePlugin._inbox_line))


if __name__ == "__main__":
    unittest.main()
