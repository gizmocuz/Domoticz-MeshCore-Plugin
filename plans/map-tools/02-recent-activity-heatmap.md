# Feature 2 — Recent Activity Heatmap (10-minute window)

## Goal

Toggleable Leaflet overlay that paints a heatmap of node activity over the
last **10 minutes** (configurable window: 5 / 10 / 30 / 60 min). Each
event in `_rx_log` whose source node has known coords contributes a heat
point sized by recency.

Complements the existing 24 h heatmap in the stats side card (which is a
list, not a map).

## Why

- valleirug shows a "last 10 minutes" overlay; ours is missing.
- Helps spot where mesh activity is concentrated right now.

## Data sources (all already present)

- `_rx_log[]` — pushed via `rxlog` / `rxlog_delta` frames. Each entry has
  `ts`, `from`/`src` (resolved to a known node name or hex prefix), and
  `payload` info. Plugin source: `_on_rx_log` (`plugin.py:2920`).
- Node coords: `_deviceMap.nodes[name]` (`lat`, `lon`), self via
  `_deviceMap.self`, heard nodes via `_heard.nodes[pk]`.

The dashboard **subscribes to rxlog** only while a heavy panel is open
(see `CLAUDE.md` → "rxlog / rxlog_delta — on-demand"). The topology view
**must subscribe to rxlog** while the heatmap toggle is on. Easiest: add
`"topology-heatmap"` as a subscriber tag the existing sub/unsub plumbing
recognises, or sub from `_renderTopology` whenever the heatmap flag is
true and the rxlog subscription isn't already active.

## Library

[`leaflet.heat`](https://github.com/Leaflet/Leaflet.heat) — single file,
~7 kB minified, no API key. Mirror it into the plugin dir as
`leaflet/leaflet-heat.js` and copy to `www/templates/leaflet/` on
`onStart` (same pattern as the existing `leaflet/` bundle).

## Implementation

### 1. Add `leaflet/leaflet-heat.js`

- Vendor in the plugin tree under `leaflet/leaflet-heat.js`.
- Extend the `LEAFLET_FILES` list (search `plugin.py` for the existing
  static-file copy loop — same one that copies `leaflet/leaflet.js`).
- Add `<script src="leaflet/leaflet-heat.js"></script>` after the
  existing Leaflet script tag in `meshcore.html`.

### 2. Toolbar button

```html
<button class="topo-map-tool-btn" id="topo-btn-heatmap" title="10-minute activity heatmap">
    <i class="fa-solid fa-fire"></i>
    <span class="topo-tool-label">Heat</span>
</button>
```

Inserted in `#topo-toolbar` (`meshcore.html:7342`) between Lines and Live
buttons. Default off.

### 3. Window selector

When the heatmap button is **active**, render a small inline `<select>`
next to it with options `5 / 10 / 30 / 60 min`. Persisted as
`topo:heatmap-window` (default `10`).

### 4. Renderer

```js
let _heatmapLayer = null;

function _renderActivityHeatmap() {
    if (!_leafletMap) return;
    if (_heatmapLayer) { _leafletMap.removeLayer(_heatmapLayer); _heatmapLayer = null; }
    if (!_topoGet("heatmap", false)) return;

    const windowMs = _topoGet("heatmap-window", 10) * 60_000;
    const now = Date.now();
    const cutoff = now - windowMs;

    // index node coords by name AND pubkey-prefix for rxlog source resolution
    const coordIdx = _buildNodeCoordIndex();
    const points = [];

    for (const entry of _rx_log_buffer) {
        if (!entry.ts || entry.ts * 1000 < cutoff) continue;
        const src = entry.src || entry.from;
        const xy = coordIdx[src];
        if (!xy) continue;
        // Recency weight: 1.0 for now, fades to 0.2 at the window edge
        const age = (now - entry.ts * 1000) / windowMs;
        const w = Math.max(0.2, 1 - age);
        points.push([xy.lat, xy.lon, w]);
    }

    _heatmapLayer = L.heatLayer(points, {
        radius: 22, blur: 18, maxZoom: 17,
        gradient: { 0.2: "#3b82f6", 0.5: "#22c55e", 0.8: "#f59e0b", 1.0: "#ef4444" },
    }).addTo(_leafletMap);
}
```

Hook:

- Called once at the end of `_renderTopology()` (so it survives every
  layer rebuild — heatmap lives **outside** `_topologyLayer` for that
  reason; do not put it inside the same `LayerGroup`).
- Re-run whenever:
  - the heatmap button toggles;
  - the window selector changes;
  - a new `rxlog_delta` frame lands **AND** the heatmap is on
    (throttled to ≤ 1 Hz).

### 5. Auto-refresh

Add a `_heatmapTimer = setInterval(_renderActivityHeatmap, 30_000)` while
the heatmap is on so the gradient fades naturally as events age out of
the window. Cleared when the topology view closes or the toggle goes off.

### 6. Empty-state handling

If `points.length === 0`, still create an empty `heatLayer` (so removing
it later is uniform) and surface a small inline note in the toolbar:
"No traffic in the last X min" — for clarity.

## Edge cases

- **Heard-only nodes:** rxlog source may resolve to a 1-byte path hash
  (not a known pubkey). Use the existing `_resolvePathHop()` helper to
  upgrade hash → name → coords. Skip points that don't resolve.
- **Observer self:** include self in the index so frames originated by
  our node contribute too (e.g. our `flood_tx`).
- **Zoom interplay:** heatmap radius is pixel-based, so at very low
  zoom it can blob the entire Netherlands. The `maxZoom: 17` clamp + a
  zoom-aware radius (radius = `max(8, 22 - (15-z)*2)`) keeps it readable.
- **Toggle order with tile switch:** confirmed — heatmap lives on a
  separate `L.layerGroup`; removing the base tile via #1 doesn't
  re-parent it.

## Tests

Manual:

- [ ] Heat button toggles overlay on/off, persisted across reload.
- [ ] Window selector switches between 5/10/30/60 min and re-renders.
- [ ] Activity from a node we know shows up at its lat/lon.
- [ ] Removing a heard node (popup delete) immediately clears its heat
      blob.
- [ ] No JS errors when `_rx_log_buffer` is empty.
- [ ] Heatmap survives a `_renderTopology()` rebuild (e.g. toggling
      Companions/Repeaters).

No new automated tests — the plugin side is unchanged except for the new
static-file copy entry, which inherits coverage from the existing
file-deploy tests.

## Effort

~3 h including library vendor, toolbar wiring, fade-with-age curve, and
zoom-aware radius.

## Dependencies

- None. Land independently of #1, #3, #4.
- **Provides reusable pattern** for #4 (overlay lives outside
  `_topologyLayer`, throttled redraw on data churn).
