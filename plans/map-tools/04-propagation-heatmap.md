# Feature 4 — Propagation / Coverage Heatmap

## Goal

For a chosen **origin node** (default: observer; user can pick any
repeater/contact), paint a coverage prediction onto the topology map as
a heat-tinted canvas overlay. Colour = expected received signal level in
dB above the noise floor. Areas where the predicted SNR is below the
LoRa decode threshold are transparent.

## Why

- valleirug ships this and it's their biggest USP.
- Gives users a quick "where could I put a new repeater" answer.
- Reuses the LoS feature's elevation data — very little new server-side
  surface.

## Inputs (settings drawer in the topology side panel)

| Param | Default | Notes |
|---|---|---|
| Origin | observer | Drop-down: observer, every known contact, every heard node with coords |
| TX power | 20 dBm | EU LoRa max |
| Antenna gain (TX + RX) | 2 dBi each | Combined applied as `+G_tx + G_rx` |
| RX antenna height | 2 m AGL | Receiver assumed terrestrial |
| Noise figure | 6 dB | Stock SX1276 |
| Fade margin | 10 dB | |
| Frequency | 910.525 MHz | |
| Bandwidth / SF / CR | 62.5 kHz / SF7 / 4/8 | Determines decode threshold (≈ −123 dBm) |
| Model | Free-space + ITU terrain | Auto when elevation is available, free-space-only fallback |
| Grid size | 200 × 200 | Centred on origin, span = 2× the LoRa horizon at this power |
| Show terrain attenuation | on | Off ⇒ pure free-space (FSPL) |

LoRa decode threshold table (dBm, SF × BW) baked into the dashboard JS:

```
SF7,  62.5 kHz  → −123
SF8,  62.5 kHz  → −126
SF9,  62.5 kHz  → −129
SF10, 62.5 kHz  → −132
SF11, 62.5 kHz  → −134
SF12, 62.5 kHz  → −136
SF7, 125  kHz   → −120
... (full SX1276 table)
```

## Math

Path loss combines:

1. **Free-space path loss** (FSPL): `Lp = 20 log10(d_m) + 20 log10(f_Hz) − 147.55`
2. **ITU-R P.1812 terrain factor** (lightweight: knife-edge diffraction
   over each obstacle in the profile, summed). For a v1 we use the
   simpler **Bullington construction** — single equivalent knife-edge
   replacing the chain.
3. **Optional extra clutter loss** (suburban / urban / indoor) — same
   bucketing as valleirug. Applied uniformly over the cell.

Per grid cell:

```
RX_dBm = TX_dBm + G_tx + G_rx − Lp − L_terrain − L_clutter
SNR_dB = RX_dBm − noise_floor_dBm − fade_margin
```

If `SNR_dB > 0` → cell renders with the gradient; below threshold → α=0.

## Implementation

### 1. Settings drawer

Open via a "Coverage…" button in the topology toolbar:

```html
<button class="topo-map-tool-btn" id="topo-btn-coverage" title="Predicted radio coverage">
    <i class="fa-solid fa-broadcast-tower"></i>
    <span class="topo-tool-label">Coverage</span>
</button>
```

Click toggles an inline `<div>` between toolbar and map containing the
controls listed above. State persisted under `topo:coverage:*` keys.

### 2. Canvas overlay (`L.GridLayer` subclass)

Each Leaflet tile (256 × 256 px) gets a 2D canvas. For every pixel:

- convert pixel → lat/lon via `map.containerPointToLatLng`.
- look up the precomputed `(distance, bearing)` from origin → cell to
  index into the 200 × 200 propagation grid (bilinear interp).
- colour the pixel via the gradient.

Implementation detail: precompute the **200 × 200 grid in a Web Worker**
once per settings change. The worker:

- batches all 40 000 sample coordinates;
- sends them in chunks of ~2 000 to the plugin's `elevation` cmd (reused
  from #3 — this is the cross-feature dependency);
- runs the propagation math per cell;
- returns a `Float32Array(40_000)` of SNR values.

The grid layer reads from the latest worker output. Recomputation is
debounced 500 ms after the last settings change. Whilst recomputing,
a small spinner overlays the toolbar.

### 3. Fallback ship (no elevation)

If #3 hasn't landed yet, ship #4 in **free-space-only** mode:

- No elevation cmd needed; per-cell `L_terrain = 0`.
- A banner reads "Free-space model only — terrain not considered.
  Enable Terrain when available."
- Free-space alone gives a clean circle around the origin; still useful.

When #3 merges, flip the `useTerrain` default to on and remove the
banner.

### 4. Origin marker styling

Selected origin gets a pulsing halo on the map so the user knows which
node the heatmap is rooted at. Switching origin re-runs the grid.

### 5. Legend

Bottom-right of the map: a vertical gradient bar 0 → 30 dB above
threshold, with marks at 0 / 10 / 20 / 30. Same gradient as the canvas.

### 6. Performance notes

- 40 000 grid cells × ~1 ms math each = 40 s if done naively. The worker
  must compute in **tight loops** (no allocations inside loop, prefilled
  typed arrays). Target < 2 s per recompute on a modest laptop.
- Tile redraw is GPU-accelerated; we keep the canvas overlay
  semi-opaque (`globalAlpha = 0.55`) so basemap roads stay visible.
- The grid is **anchored at the origin** and re-projected on map drag —
  not recomputed. Only re-computed on settings or origin change.

### 7. Plugin side

Reuses #3's `elevation` WebSocket cmd. No new server-side code unless
#3 hasn't landed (in which case #4 ships free-space-only and skips the
cmd entirely).

## Edge cases

- Origin without coords: button greyed out; tooltip explains.
- Computation race: if the user changes settings during a worker run,
  cancel the in-flight worker (`worker.terminate()`) and start a new
  one — easier than coalescing partial results.
- Off-screen drag: don't recompute; just let `L.GridLayer` handle
  re-projection.
- Map at world-zoom: cap the rendered area to ±50 km from origin (LoRa
  horizon at any power is well under 50 km for the worst-case TX/RX
  combo). Cells outside that radius are α=0.

## Tests

- Unit (Python): no new server-side code if #3 is already merged. If
  shipping free-space first: still no server code.
- Unit (JS, optional): factor the propagation math (`computeSnrGrid`)
  into a pure function and add a small Karma-free test that runs in
  Node (`node tests/js/test_propagation.mjs`):
  - FSPL at 1 km, 910 MHz ≈ −91 dB.
  - Bullington with a single 100 m knife-edge midpath adds ~12 dB.
  - Decode threshold table lookup.
- Manual UI:
  - Open coverage settings, change origin to a repeater — heatmap
    re-centres.
  - Toggle terrain — circle pattern → terrain-shaped on hilly areas
    (visible in Veluwe / Limburg).
  - Increase TX power — bloom expands.
  - Decrease bandwidth / increase SF — bloom expands (lower threshold).

## Effort

- With #3 elevation proxy in place: ~1.5 days (worker + math: 1 day,
  grid layer + UI: 0.5 day).
- Without #3 (free-space-only ship): ~0.5 day.

## Dependencies

- **Hard dep on #3 for terrain-aware mode.** Can ship free-space-only
  fallback without #3.
- Soft dep on #2: reuses the "overlay outside `_topologyLayer`,
  throttled redraw" pattern.
- Independent of #1.
