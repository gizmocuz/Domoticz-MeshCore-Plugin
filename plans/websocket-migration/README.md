# Plan — WebSocket transport migration

Replace the "plugin writes JSON into `www/templates`, dashboard polls with
`fetch()`" architecture with Domoticz's native bidirectional plugin↔frontend
WebSocket channel.

## Why / when

The channel was added to Domoticz in commit `264e527`
("feat(plugins): add bidirectional plugin-frontend WebSocket channel"),
**2026-05-16**, build **`17956`** = version **`2025.2.17956`**. README now
states this as the minimum for the dashboard.

## Locked decisions

- **WebSocket-only** — no JSON-file fallback. Old Domoticz keeps devices,
  loses the dashboard (explicit upgrade banner).
- **In-memory first** — live state lives in plugin memory and is *pushed*;
  it is no longer HTTP-served. Restore-on-start persistence files move to
  the **plugin folder** (plugin-private, never fetched by the browser).
- **rx-log on-demand + deltas** — small state pushed on change; bulky
  rx-log/firehose/heatmap pushed only while a panel needs it, then as
  sequence-numbered deltas.

## Building blocks (Domoticz)

`Domoticz.WebSocketSend(dict|str)`, `onWebSocketMessage(...)`; frontend
`livesocket.subscribePlugin/onPluginMessage/sendPluginCommand/unsubscribePlugin`,
topic `plugin:MeshCore`, multi-instance via `hwid`. Reference impl:
`S:\domoticz\plugins\examples\WebSocketChannelTest\`.

## Message protocol

JSON objects with a `t` (type) field.

Plugin → frontend: `snapshot`, `devices` (delta), `stats`, `heard`,
`channels`, `rxlog` (window), `rxlog_delta` (seq'd), `cmd_result`.
Frontend → plugin: `hello`, `sub {feed:'rxlog'|'none'}`, `cmd {cmd:'!…'}`,
`snapshot` (re-request). Seq numbers on rxlog so a gap → re-request;
always re-`hello` on reconnect.

## Features & dependency graph

| ID | Feature | Depends on | Parallelizable |
|----|---------|-----------|----------------|
| F1 | [Command channel](F1-command-channel.md) | — | start immediately |
| F2 | [State push + reducer](F2-state-push.md) | F1 | plugin-push vs frontend-reducer can split across 2 agents |
| F3 | [rx-log on-demand + deltas](F3-rxlog-ondemand.md) | F2 | sequential after F2 |
| F4 | [Persistence relocation + cleanup](F4-persistence-cleanup.md) | F1, F2, F3 | cmd-hack removal can begin once F1 done |
| F5 | [Verification](F5-verification.md) | F1–F4 | last |
| F6 | [Python test harness](F6-test-harness.md) | scaffolding: none (build first) | runs alongside every feature |

```
F6 (scaffold) ─┐
               ▼
F1 ──► F2 ──► F3 ──► F4 ──► F5
        │             ▲
        └─ (F4 cmd-hack removal may begin after F1) ─┘
   (F6 adds per-feature tests in lock-step with F1..F5)
```

Sequencing: build the **F6 scaffold first** (Domoticz stub + runner +
tests for today's pure logic), then **F1 → F2 → F3 → F4 → F5**, with F6
gaining a test module per feature and the suite kept green as a gate. The
shared transport layer forces mostly-sequential feature work. Intra-feature
parallelism: F2's plugin-push vs frontend-reducer sides, and the early
start of F4's command-hack removal once F1 lands.

## Out of scope (separate Todo entries)

- Reliable retry send (`send_msg_with_retry`).
- Repeater-directory path-hop resolution + periodic poll.
