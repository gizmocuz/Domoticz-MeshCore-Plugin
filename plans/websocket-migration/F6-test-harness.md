# F6 — Python test harness

Standalone Python tests for the plugin's pure logic, runnable without a
Domoticz runtime, executed during development and after each feature.

## Dependencies
Scaffolding (Domoticz stub + runner + current-logic tests) has **no deps**
— build it first so F1–F5 can be validated as they land. Per-feature test
additions track their feature (F1 tests after F1, etc.).

## Constraint

`plugin.py` does `import DomoticzEx as Domoticz` and instantiates
`BasePlugin()` at import. The `DomoticzEx` module only exists inside the
Domoticz runtime, so tests must inject a **stub module** before importing
`plugin`. Only pure logic (no live socket / asyncio / Domoticz API) is
unit-tested; transport behavior is covered by F5's manual matrix.

## Scope

### `tests/_stubs/DomoticzEx.py`
Minimal fake: `Debug/Log/Error/Status/Heartbeat` no-ops,
`Devices={}`, `Parameters={}`, `Settings={}`, stub `Unit`/`Device`/
`Connection`/`Image` and (added in F1) a `WebSocketSend` spy that records
calls so push/protocol can be asserted.

### `tests/run_tests.py`
Puts `_stubs` on `sys.path`, runs `unittest discover tests`. Stdlib only
(no pytest — none configured). Exit non-zero on failure.

### Test modules (grow with the migration)
- `test_import_smoke.py` — `import plugin` with stubs succeeds; `_plugin`
  is a `BasePlugin`; module hooks (`onStart/onStop/onHeartbeat/
  onWebSocketMessage`) are callable.
- `test_inbox_line.py` — `BasePlugin._inbox_line` for every token combo
  (`~h/~s/~r/~p`, `x` flag), ordering after epoch, hex sanitization of
  `path`, backward-compat (no tokens / legacy form).
- `test_protocol.py` (F1+) — feed JSON commands to `onWebSocketMessage`
  with the stub; assert `WebSocketSend` spy got the right `t`-typed
  replies (`cmd_result`, `snapshot`, deltas).
- `test_stats_migration.py` (F2+) — legacy `hops_record` → `hops_records`
  list migration and top-5 trimming.
- `test_rxlog_seq.py` (F3+) — delta seq increments; gap → window resend.

## Execution policy
- **During development**: run `python tests/run_tests.py` after each
  change to touched logic.
- **After each feature (F1–F5)**: full run must be green before that
  feature is considered done; record the run in the feature's acceptance.
- Manual live matrix (F5) still required for transport/multi-instance.

## Acceptance
`python tests/run_tests.py` is green; smoke + inbox-line tests pass on the
current codebase before F1 starts; each later feature adds its module and
keeps the suite green.
