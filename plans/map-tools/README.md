# Map Tools — Implementation Plan

Adds four new capabilities to the Mesh Topology side panel, inspired by
[live.valleirug.nl](https://live.valleirug.nl/):

| # | Feature | File |
|---|---|---|
| 1 | Dark / light map toggle | [01-tile-toggle.md](01-tile-toggle.md) |
| 2 | Recent-activity heatmap (10 min) | [02-recent-activity-heatmap.md](02-recent-activity-heatmap.md) |
| 3 | Line-of-sight (LoS) tool | [03-los-tool.md](03-los-tool.md) |
| 4 | Propagation / coverage heatmap | [04-propagation-heatmap.md](04-propagation-heatmap.md) |

## Goals

- Make the topology map a genuine planning tool, not just a node viewer.
- Every overlay/tool must coexist cleanly with the existing
  `_topologyLayer` + multi-segment polyline drawing and the popup delete
  button. None of them break the radar pane.
- All elevation / propagation math runs **client-side**; the plugin stays
  thin and the only new server-side surface is a tiny elevation proxy
  (cached in `meshcore_elevation_cache.json`) so we don't hammer the
  upstream tile/elevation service from every browser tab.

## Current state (verified in repo)

- Leaflet base layer is `tile.openstreetmap.org` (light) at
  `meshcore.html:5721`, `:6311`, `:7215`, `:7629`. CLAUDE.md's "dark CARTO
  tiles" note is stale; **default is light** today.
- `_renderTopology()` (~`meshcore.html:6291`) tears down and rebuilds
  `_topologyLayer` on every refresh. Any new overlay layer must follow
  the same lifecycle (rebuild on each render) or be parented to a separate
  `L.LayerGroup` that survives across renders.
- Toolbar HTML at `meshcore.html:7342` (`#topo-toolbar`); button wiring at
  `:7400` (`_applyTopoToolbarState`). State is persisted via
  `_topoGet`/`_topoSet` to `localStorage` keyed `topo:<flag>`.
- `_rx_log` (`plugin.py:320`, `RX_LOG_BUFFER = 250`) holds the recent rx
  feed already exposed to the dashboard via the `rxlog` / `rxlog_delta`
  websocket frames. The recent-activity heatmap feeds off this.
- Pubkey-keyed node lat/lon is on `_deviceMap.nodes[name]` (`lat`, `lon`)
  and `_heard.nodes[pk]` (`lat`, `lon`). Observer location uses the
  3-tier fallback in `_renderTopology()` (`self_info` → self node →
  Domoticz Settings).

## Dependency graph

```
                                  ┌────────────────────────┐
                                  │ existing topology map  │
                                  │  (_leafletMap, panel)  │
                                  └───────────┬────────────┘
                                              │
        ┌────────────────────┬────────────────┼────────────────────┐
        │                    │                │                    │
        ▼                    ▼                ▼                    ▼
  ┌───────────┐      ┌──────────────┐  ┌─────────────┐    (depends on 3) 
  │ 1 Tile    │      │ 2 Recent     │  │ 3 LoS tool  │    ┌─────────────┐
  │ toggle    │      │   activity   │  │ (elevation  │    │ 4 Propagat. │
  │           │      │   heatmap    │  │  + Fresnel) │◄───┤   heatmap   │
  └───────────┘      └──────┬───────┘  └─────┬───────┘    └─────────────┘
                            │                │                  ▲
                            └────────────────┴──── overlay helper ─┘
                                  (shared canvas/heat plugin)
```

### Sequencing

**Parallel-safe set A** — three pieces with no cross-dependencies, each
can be picked up by a separate session/PR:

- **#1 Tile toggle** — pure CSS + L.layerGroup swap. Tiny, lands first or
  alongside anything else.
- **#2 Recent activity heatmap** — pulls leaflet.heat plugin, consumes
  existing `_rx_log` data. Independent.
- **#3 LoS tool** — pin-drop click handler, elevation proxy in
  `plugin.py`, Highcharts area chart. Independent.

**Depends on #3**:

- **#4 Propagation heatmap** — reuses #3's elevation proxy for terrain
  attenuation. Can be started in parallel with #1, but **must merge after
  #3** so the elevation client is in place. If #3 slips, #4 can ship in
  free-space-only mode as a fallback (clearly marked "no terrain") — see
  the §"Fallback ship" section in [04-propagation-heatmap.md](04-propagation-heatmap.md).

Suggested order if working one-at-a-time:

1. #1 Tile toggle (quick win, low risk)
2. #2 Recent-activity heatmap (introduces leaflet.heat, reused by #4)
3. #3 LoS tool (largest UX surface, sets up elevation API)
4. #4 Propagation heatmap (composes everything)

## Cross-cutting concerns

| Concern | Approach |
|---|---|
| External CDNs | All third-party JS (leaflet.heat) is **mirrored once** into the plugin directory and copied to `www/templates/` on `onStart`, matching the existing pattern for `leaflet/`. No new CDN dependencies on the dashboard. |
| Caching elevation lookups | New `elevation_cache` table inside the existing `meshcore_messages.db`, keyed by quantised `(lat_q, lon_q)` at 4 dp (~11 m grid). LRU-capped at 100 k rows. Schema added under the regular migration ladder — no new file, no new lock, no extra RAM (SQLite serves point queries off disk). See [03-los-tool.md](03-los-tool.md). |
| Toolbar real-estate | Toolbar already has 7 buttons. Add #1 (tile), #2 (heatmap), #3 (LoS) as buttons; #4 ("Coverage") as a button that opens a small inline params drawer. Keep `icon-only` styling consistent. |
| State persistence | Each new toggle gets a `topo:<key>` localStorage entry via `_topoGet`/`_topoSet`. Defaults documented in `_applyTopoToolbarState`. |
| Mobile / narrow viewports | Toolbar already wraps. LoS pin-drop must work with touch (`click` is fine, but the cursor crosshair won't apply). Verify on the 360 px width breakpoint. |
| Plugin tests | Each plan that touches `plugin.py` ships matching tests in `tests/`. Elevation proxy (3) and propagation math, if server-side, are unit-tested with stubbed HTTP. |
| Build version | None of these need Domoticz build bumps — we already require ≥ `17956` for the WebSocket transport. |

## Out of scope (deferred)

- WebGPU-accelerated propagation grids (valleirug's auto-resolution).
  Canvas + a fixed 200×200 sample grid is plenty for our scale.
- Auto-resolution / dynamic grid sampling.
- Multi-origin propagation overlap visualisation.
- Weather radar / wind tile overlays (KNMI / RainViewer).
- Topographic basemap (only light/dark for now — easy to extend later).
