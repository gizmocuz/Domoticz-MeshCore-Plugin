# F4 — Persistence relocation + cleanup

No JSON in `www/templates`; restore-on-start files become plugin-private;
dead code removed.

## Dependencies
**F1, F2, F3** (no fetch/file consumers may remain). The command-hack
removal portion can begin right after **F1**.

## Scope

### plugin.py
- Move `meshcore_devices / rx_log / heard / stats / channels.json` writes
  from `www/templates` to the **plugin folder**; used only for
  restore-on-start, never HTTP-served.
- `meshcore_locations.json` stays user-owned in the plugin folder;
  `meshcore_repeaters.json` likewise. Stop copying any of these into
  `www/templates`.
- `meshcore.html` (and `leaflet/`) is still copied to `www/templates`
  (must be HTTP-served) but reads no sibling JSON.
- Simplify `onStop`: only html/leaflet template assets need removal — drop
  all the JSON-in-templates juggling and the associated restart-race
  comments.
- Delete dead code: file-emit-to-templates paths, the Mesh Send device +
  `onDeviceModified` command branch (if not already gone in F1), any
  cache-busting helpers.

### meshcore.html
- Remove the entire `fetch('/templates/*.json')` layer and poll timers
  (already replaced by F2/F3). Ensure nothing references template JSON.

## Acceptance
- `www/templates` contains only `meshcore.html` + `leaflet/` while running.
- Plugin restart restores heatmap/heard/stats from the plugin-folder files.
- `onStop` is materially simpler; no JSON template files created/removed.
- `grep` shows no remaining `/templates/*.json` fetch or file-emit code.
