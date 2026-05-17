# F1 — Command channel

Replace the device-`sValue`/`onDeviceModified` `!cmd` hack with the native
WebSocket command path.

## Dependencies
None. Foundational — start immediately. Establishes the transport + the
`_push()` helper that F2/F3 build on.

## Scope

### plugin.py
- Implement `onWebSocketMessage(self, Connection, Data)` (module hook →
  `BasePlugin`). Parse JSON; dispatch on `t`:
  - `hello` → mark a subscriber present, reply `snapshot` (F2 fills the
    payload; in F1 just echo `{t:'cmd_result',ok:true}` / minimal).
  - `cmd` → feed `data.cmd` to the existing `!`-command handler (reuse the
    current parser used by the Mesh Send device path).
  - `sub` → record desired feeds (consumed by F3; store now).
- Add `_push(t, payload)` → `Domoticz.WebSocketSend({...})`; feature-detect
  `hasattr(Domoticz,'WebSocketSend')`, log once + set a `_ws_ok=False` flag
  if absent (min-version guard; F5 surfaces the banner).
- After running a command, `_push('cmd_result', {...ok,target,result})`.

### meshcore.html
- On load: `livesocket.subscribePlugin('MeshCore')`,
  `livesocket.onPluginMessage('MeshCore', cb)`, send `{t:'hello'}`.
- Replace `_sendDeviceCmd()` internals with
  `livesocket.sendPluginCommand('MeshCore', {t:'cmd', cmd})`; resolve the
  returned promise/awaiter on the matching `cmd_result`.
- Keep the old device path temporarily behind a guard until F2 lands, OR
  remove immediately (decision: remove — WebSocket-only, no fallback).

## Acceptance
- A `!`-command issued from the dashboard reaches the plugin via
  `onWebSocketMessage` and its real success/error shows in the UI.
- The Mesh Send device + `onDeviceModified` command branch are deleted and
  nothing else references them.
- Missing `Domoticz.WebSocketSend` is detected and logged once.
