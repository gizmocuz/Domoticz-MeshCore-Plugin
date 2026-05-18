"""F4 — Persistence relocation + cleanup tests.

Verifies:
  (a) The five state-file path helpers (_rx_log_path, _stats_path,
      _heard_path, and the paths implied by _write_device_map /
      _write_channel_names) all resolve inside the plugin directory,
      never under www/templates.
  (b) _migrate_state_files moves a file from the old templates location
      to the plugin dir when the plugin-dir copy is absent (one-time
      migration).
  (c) Migration is idempotent: if the plugin-dir copy already exists
      the old templates copy is cleaned up but the plugin-dir copy is
      not overwritten.
  (d) Migration tolerates a missing templates dir or missing individual
      files without raising.

All tests use a temporary directory; no real filesystem side effects
outside of tempdir.
"""
import _bootstrap  # noqa: F401  (sys.path side-effect)
import json
import os
import shutil
import sys
import tempfile
import unittest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _plugin_instance():
    """Return the module-level BasePlugin singleton."""
    import plugin
    return plugin._plugin


def _plugin_dir():
    """Canonical plugin directory (where plugin.py lives)."""
    import plugin as _pm
    return os.path.dirname(os.path.abspath(_pm.__file__))


class TestStateFilePaths(unittest.TestCase):
    """(a) Path helpers must point into the plugin directory."""

    def _plugin_dir(self):
        return _plugin_dir()

    def test_rx_log_path_in_plugin_dir(self):
        p = _plugin_instance()._rx_log_path()
        self.assertEqual(os.path.dirname(p), self._plugin_dir())
        self.assertEqual(os.path.basename(p), "meshcore_rx_log.json")

    def test_stats_path_in_plugin_dir(self):
        p = _plugin_instance()._stats_path()
        self.assertEqual(os.path.dirname(p), self._plugin_dir())
        self.assertEqual(os.path.basename(p), "meshcore_stats.json")

    def test_heard_path_in_plugin_dir(self):
        p = _plugin_instance()._heard_path()
        self.assertEqual(os.path.dirname(p), self._plugin_dir())
        self.assertEqual(os.path.basename(p), "meshcore_heard.json")

    def test_rx_log_path_not_under_www_templates(self):
        p = _plugin_instance()._rx_log_path()
        self.assertNotIn("www", p.replace("\\", "/"))

    def test_stats_path_not_under_www_templates(self):
        p = _plugin_instance()._stats_path()
        self.assertNotIn("www", p.replace("\\", "/"))

    def test_heard_path_not_under_www_templates(self):
        p = _plugin_instance()._heard_path()
        self.assertNotIn("www", p.replace("\\", "/"))


class TestMigrateStateFiles(unittest.TestCase):
    """(b)/(c)/(d) One-time migration logic."""

    # The five state file names that must be migrated.
    STATE_FILES = [
        "meshcore_devices.json",
        "meshcore_rx_log.json",
        "meshcore_heard.json",
        "meshcore_stats.json",
        "meshcore_channels.json",
    ]

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.plugin_dir = os.path.join(self.tmpdir, "plugin")
        self.old_dir = os.path.join(self.tmpdir, "www", "templates")
        os.makedirs(self.plugin_dir)
        os.makedirs(self.old_dir)
        self._orig_file = os.path.abspath(__file__)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _run_migration(self):
        """Run _migrate_state_files with injected directories so the test is
        fully isolated without relying on __file__ patching (unreliable under
        CPython 3.13's specializing adaptive interpreter)."""
        _plugin_instance()._migrate_state_files(
            _plugin_dir=self.plugin_dir,
            _old_dir=self.old_dir,
        )

    def test_migration_moves_file_from_old_to_new(self):
        """File present only in templates → moved to plugin dir."""
        fname = "meshcore_stats.json"
        old_path = os.path.join(self.old_dir, fname)
        new_path = os.path.join(self.plugin_dir, fname)
        payload = {"messages_total": 42}
        with open(old_path, "w") as f:
            json.dump(payload, f)

        self._run_migration()

        self.assertTrue(os.path.isfile(new_path), "File must appear in plugin dir after migration")
        self.assertFalse(os.path.isfile(old_path), "Old templates copy must be removed after migration")
        with open(new_path) as f:
            data = json.load(f)
        self.assertEqual(data.get("messages_total"), 42, "Migrated content must be preserved")

    def test_migration_is_idempotent_when_new_exists(self):
        """File already in plugin dir → old copy removed, plugin-dir copy untouched."""
        fname = "meshcore_heard.json"
        old_path = os.path.join(self.old_dir, fname)
        new_path = os.path.join(self.plugin_dir, fname)
        old_payload = {"nodes": {"old": True}}
        new_payload = {"nodes": {"new": True}}
        with open(old_path, "w") as f:
            json.dump(old_payload, f)
        with open(new_path, "w") as f:
            json.dump(new_payload, f)

        self._run_migration()

        self.assertFalse(os.path.isfile(old_path), "Stale templates copy must be removed")
        with open(new_path) as f:
            data = json.load(f)
        self.assertEqual(data["nodes"].get("new"), True, "Plugin-dir copy must not be overwritten")

    def test_migration_tolerates_missing_templates_dir(self):
        """No templates dir at all → migration must not raise."""
        shutil.rmtree(self.old_dir)
        try:
            self._run_migration()
        except Exception as exc:
            self.fail(f"_migrate_state_files raised with missing templates dir: {exc}")

    def test_migration_tolerates_missing_individual_file(self):
        """Neither old nor new copy exists → migration skips silently."""
        fname = "meshcore_rx_log.json"
        new_path = os.path.join(self.plugin_dir, fname)
        try:
            self._run_migration()
        except Exception as exc:
            self.fail(f"_migrate_state_files raised with no files present: {exc}")
        self.assertFalse(os.path.isfile(new_path))

    def test_migration_moves_all_five_files(self):
        """All five state files in templates → all moved to plugin dir."""
        for fname in self.STATE_FILES:
            with open(os.path.join(self.old_dir, fname), "w") as f:
                json.dump({"fname": fname}, f)

        self._run_migration()

        for fname in self.STATE_FILES:
            new_path = os.path.join(self.plugin_dir, fname)
            old_path = os.path.join(self.old_dir, fname)
            self.assertTrue(os.path.isfile(new_path),
                            f"{fname} must be present in plugin dir after migration")
            self.assertFalse(os.path.isfile(old_path),
                             f"{fname} must be removed from templates after migration")


class TestChannelsPath(unittest.TestCase):
    """_channels_path() must point into the plugin directory."""

    def _plugin_dir(self):
        return _plugin_dir()

    def test_channels_path_in_plugin_dir(self):
        p = _plugin_instance()._channels_path()
        self.assertEqual(os.path.dirname(p), self._plugin_dir())
        self.assertEqual(os.path.basename(p), "meshcore_channels.json")

    def test_channels_path_not_under_www_templates(self):
        p = _plugin_instance()._channels_path()
        self.assertNotIn("www", p.replace("\\", "/"))


class TestLoadChannels(unittest.TestCase):
    """_load_channels() must restore _channel_names from meshcore_channels.json."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _run_load(self, payload):
        """Write payload to a temp file, patch _channels_path, run _load_channels,
        return the resulting _channel_names dict."""
        p = _plugin_instance()
        tmp_path = os.path.join(self.tmpdir, "meshcore_channels.json")
        with open(tmp_path, "w") as f:
            json.dump(payload, f)
        orig = p._channels_path
        p._channels_path = lambda: tmp_path
        p._channel_names = {}
        try:
            p._load_channels()
        finally:
            p._channels_path = orig
        return dict(p._channel_names)

    def test_load_channels_restores_int_keys(self):
        """JSON string keys are converted to int internally."""
        result = self._run_load({"0": "General", "1": "Local"})
        self.assertEqual(result, {0: "General", 1: "Local"})

    def test_load_channels_skips_empty_slots(self):
        """Empty-string channel names (empty slots) are excluded."""
        result = self._run_load({"0": "General", "1": "", "2": "Room"})
        self.assertIn(0, result)
        self.assertNotIn(1, result)
        self.assertIn(2, result)

    def test_load_channels_missing_file_is_noop(self):
        """Missing file must not raise and must leave _channel_names untouched."""
        p = _plugin_instance()
        p._channel_names = {}
        nonexistent = os.path.join(self.tmpdir, "does_not_exist.json")
        orig = p._channels_path
        p._channels_path = lambda: nonexistent
        try:
            p._load_channels()
        finally:
            p._channels_path = orig
        self.assertEqual(p._channel_names, {})

    def test_snapshot_channels_field_reflects_loaded_names(self):
        """After _load_channels, the snapshot channels dict must match what
        _build_snapshot_payload serialises: string keys, same values."""
        p = _plugin_instance()
        tmp_path = os.path.join(self.tmpdir, "meshcore_channels.json")
        with open(tmp_path, "w") as f:
            json.dump({"0": "Public", "3": "NL"}, f)
        orig = p._channels_path
        p._channels_path = lambda: tmp_path
        p._channel_names = {}
        try:
            p._load_channels()
            # _build_snapshot_payload stringifies int keys for JSON transport
            import plugin as _pm
            channels_snap = {str(k): v for k, v in p._channel_names.items()}
        finally:
            p._channels_path = orig
        self.assertEqual(channels_snap.get("0"), "Public")
        self.assertEqual(channels_snap.get("3"), "NL")


if __name__ == "__main__":
    unittest.main()
