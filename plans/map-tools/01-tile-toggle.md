# Feature 1 — Dark / Light Map Toggle

## Goal

Add a one-click base-tile toggle to the topology toolbar. **Default = light
(current OpenStreetMap tiles)**. Toggling switches the base tile layer to a
dark theme without touching markers, polylines, popups or the heatmap.

## Why

- One user-visible setting many people want.
- The "dark CARTO tiles" note in `CLAUDE.md` is stale — confirms the default
  has been light for a while and we just don't expose a switch.
- Establishes the per-user tile-preference plumbing that future basemaps
  (topographic, satellite) can plug into.

## Scope

- `meshcore.html` only. No `plugin.py` change.

## Tile sources (no API key)

| Mode | URL template | Attribution |
|---|---|---|
| Light (default) | `https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png` | © OpenStreetMap contributors |
| Dark | `https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png` | © OpenStreetMap, © CARTO |

`{s}` subdomains: `a`,`b`,`c` (OSM); `a`,`b`,`c`,`d` (CARTO). `maxZoom = 19`.

## Implementation

### 1. New helper

```js
const _TILE_LAYERS = {
    light: () => L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 19,
    }),
    dark: () => L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
        attribution: '&copy; OpenStreetMap, &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: "abcd",
        maxZoom: 19,
    }),
};
let _currentBaseLayer = null;

function _applyTileMode(map, mode) {
    if (!map) return;
    if (_currentBaseLayer) { map.removeLayer(_currentBaseLayer); _currentBaseLayer = null; }
    _currentBaseLayer = _TILE_LAYERS[mode] ? _TILE_LAYERS[mode]() : _TILE_LAYERS.light();
    _currentBaseLayer.addTo(map);
}
```

Replace the four inline `L.tileLayer(...)` calls at `meshcore.html:5721`,
`:6311`, `:7215`, `:7629` with `_applyTileMode(map, _topoGet("tile", "light"))`.

### 2. Toolbar button

After the existing `#topo-btn-audio` in `meshcore.html:7367`, add:

```html
<button class="topo-map-tool-btn icon-only" id="topo-btn-tile" title="Toggle dark/light map">
    <i class="fa-solid fa-moon"></i>
</button>
```

### 3. Wiring

In `_applyTopoToolbarState` (`meshcore.html:7400`) extend the defs dict
with `tile: false` (false = light). The handler block at `~:7419` already
toggles `topo:<key>` for every `topo-btn-*` button — extend it with a
special case so when the tile button is clicked we:

- flip the stored value between `"light"` and `"dark"` (string, not bool);
- swap the icon class between `fa-moon` and `fa-sun`;
- call `_applyTileMode(_leafletMap, newMode)`.

`_topoGet`/`_topoSet` accept arbitrary string values today, so the existing
helpers handle it. Keep boolean buttons untouched.

### 4. CSS

No new rules — `.topo-map-tool-btn.icon-only` already covers it.

### 5. Tile preference applies to all map panels

Apply `_applyTileMode` to **all four** Leaflet instances so the
single-node map panel and heard map honour the same setting. That avoids
"light single-node map next to dark topology" inconsistency.

## Edge cases

- Switching tile mode while a heat overlay (#2 or #4) is on top: the
  base layer is removed first, then re-added; overlays survive because
  they live on different layer groups. Verify the z-order stays correct
  (heat overlay should stay above base tiles).
- Network failures on the CARTO CDN: Leaflet falls back to the
  attribution control with no tiles. Acceptable; user can toggle back.

## Tests

Manual:

- [ ] Toggle button flips between moon and sun icons.
- [ ] Setting persists across page reload.
- [ ] All four map panels (topology, heard map, single-node) honour the
      preference.
- [ ] Toggling while topology polylines / markers are drawn does not
      reset zoom or pan.
- [ ] Toggling while a delete-popup is open does not crash (popup stays).

No automated tests — pure UI.

## Effort

~1 h including a deploy + smoke test.

## Dependencies

None. Land first.
