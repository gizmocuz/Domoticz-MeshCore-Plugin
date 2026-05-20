# Feature 3 — Line-of-Sight (LoS) Tool

## Goal

User clicks **LoS** in the topology toolbar, then drops two-or-more pins
on the map. The dashboard fetches the elevation profile between each
consecutive pin pair and draws:

1. A polyline on the map between the pins.
2. A side-panel **elevation profile chart** (Highcharts area) showing the
   terrain cross-section, the LoS straight line between antenna tops,
   the 60% Fresnel zone envelope, and obstacle highlights.
3. A clearance verdict per segment: `Clear` / `Grazing` / `Blocked`,
   based on whether the LoS line clears terrain + 60% of the first
   Fresnel radius at the LoRa frequency (910.525 MHz default,
   configurable).

Pin antenna heights are editable per pin (default observer = 2 m,
repeater = 8 m).

## Why

- Big win for site-planning, fits cleanly into the existing topology view.
- Establishes the elevation API + cache that #4 reuses.

## Inputs

- Pin coords from map clicks.
- Per-pin antenna height (m AGL).
- Frequency (default 910.525 MHz; can be overridden in a small settings
  popover — same band table as the propagation tool to be).
- Sample resolution (default 256 samples per segment).

## Elevation data source

- **Primary:** `api.open-elevation.com/api/v1/lookup` — free, no key,
  global SRTM 30 m resolution.
- **Fallback:** `api.opentopodata.org/v1/srtm30m` — also free, no key,
  better rate limits.
- Both accept up to 100 points per POST; we batch.

### Server-side proxy + cache (in `plugin.py`)

Browsers shouldn't talk to elevation services directly:

- they may be on a LAN without internet egress;
- we'd burn through rate limits per tab;
- we want sample caching across sessions.

Add a tiny WebSocket command:

```
{ t: "cmd", id: <n>, cmd: "elevation", points: [[lat, lon], ...] }
   → cmd_result: { id, ok: true, elevations: [m, m, ...] }
```

#### Storage: SQLite (not JSON)

Elevation samples are a textbook keyed lookup: 100 000+ rows, point
queries by quantised `(lat, lon)`, no scanning. JSON would mean loading
~3 MB into a Python dict on startup and rewriting the whole file
periodically. SQLite is a clean fit and we already have the
infrastructure (`meshcore_messages.db`, WAL mode, `_msgdb_lock`,
versioned migration ladder).

If the [historical-analytics plan](../historical-analytics/) lands
first, this bumps `MSG_DB_SCHEMA_VERSION` from 3 → 4. If it ships first,
2 → 3. Either way the migration adds:

```sql
CREATE TABLE IF NOT EXISTS elevation_cache (
    lat_q     INTEGER NOT NULL,      -- round(lat * 1e4)   (≈11 m grid)
    lon_q     INTEGER NOT NULL,
    elev_m    REAL    NOT NULL,
    last_used INTEGER NOT NULL,      -- unix seconds, drives LRU eviction
    PRIMARY KEY (lat_q, lon_q)
);
CREATE INDEX IF NOT EXISTS ix_elev_last_used ON elevation_cache (last_used);
```

`plugin.py` implementation sketch:

- New `_elevation_lookup(points: list[tuple[float, float]])`:
  - quantise each point to `(lat_q, lon_q)`;
  - `SELECT elev_m FROM elevation_cache WHERE (lat_q, lon_q) IN (...)`
    in one query — note SQLite has a 999-parameter limit, so chunk
    larger batches;
  - update `last_used` for hit rows (one `UPDATE … WHERE …`);
  - for misses, POST to open-elevation (batch ≤100), fall back to
    opentopodata on HTTP error; `INSERT OR REPLACE` results;
  - return the assembled array in input order.
- HTTP runs on the worker asyncio loop via `loop.run_in_executor`
  (using `urllib.request` — no new dependency).
- LRU eviction: `_elev_prune()` runs every 5 min from the heartbeat,
  `DELETE FROM elevation_cache WHERE rowid NOT IN (SELECT rowid FROM
  elevation_cache ORDER BY last_used DESC LIMIT 100000)`. Cap matches
  the original JSON-design number.
- No load-on-startup needed — SQLite serves point queries directly off
  disk via WAL. Saves ~3 MB resident memory vs the JSON approach.

### Tests for the elevation proxy

`tests/test_elevation_cache.py`:

- migration adds the `elevation_cache` table and index;
- cache hit returns without an HTTP call and updates `last_used`;
- batch >100 splits correctly across upstream API limit;
- HTTP error on open-elevation falls back to opentopodata;
- LRU eviction at the cap leaves the most-recently-used 100 000 rows;
- batch >999 splits correctly across SQLite parameter limit.

## Frontend implementation

### 1. Toolbar button + active state

```html
<button class="topo-map-tool-btn" id="topo-btn-los" title="Line-of-sight tool">
    <i class="fa-solid fa-ruler"></i>
    <span class="topo-tool-label">LoS</span>
</button>
```

When active, the map cursor switches to `crosshair`, the radar pane is
replaced by the **LoS panel** (elevation chart + per-pin controls), and
existing markers fade to 60% opacity to signal that the map is in
input-collection mode. Click on a marker still opens its popup; the
popup gains a `+ Use as LoS pin` button.

State stored as `topo:los` (bool); pins live in `_losPins` (array, not
persisted — session-only).

### 2. Pin drop handler

```js
_leafletMap.on("click", (ev) => {
    if (!_topoGet("los", false)) return;
    const ll = ev.latlng;
    _losPins.push({ lat: ll.lat, lon: ll.lng, h: _losPins.length ? 8 : 2, label: "" });
    _renderLosOverlay();
    _renderLosProfile();
});
```

Helpers:

- `_renderLosOverlay()` — draws numbered circle markers + connecting
  polyline on a dedicated `_losLayer` (not inside `_topologyLayer`).
- `_renderLosProfile()` — calls `_pluginWS.sendCmdAndWait({cmd:"elevation",points})`
  with `samples * (pins-1)` interpolated points along each segment,
  then draws the chart.

### 3. Per-pin controls

A small list rendered in the LoS side panel:

```
#1  52.0915, 5.1212   ↕ 2 m   [×]
#2  52.1331, 5.0894   ↕ 8 m   [×]
+ Add pin (click on map)   |   Clear all
```

Antenna height input snaps to 0.5 m steps, range 0–100 m. Editing it
re-runs the profile chart (no extra elevation fetch — only the LoS line
moves).

### 4. Profile chart

Highcharts area chart, similar visual style to the existing packet-type
donut. Series:

| Name | Type | Style |
|---|---|---|
| Terrain | `area` | filled brown gradient, in front |
| LoS straight line | `line` | solid red between antenna tops |
| Fresnel 60% lower | `line` | dashed yellow |
| Pin antennae | `column` | thin vertical stub at each pin |

X axis: distance in km along the great-circle path. Y axis: elevation in
m AMSL. Tooltip shows distance, terrain m, LoS m, clearance margin.

### 5. Fresnel radius (60%)

First Fresnel radius (m) at the midpoint between two antennas, distances
`d1` and `d2` (km), frequency `f` (GHz):

```
r1 = 17.31 * sqrt((d1 * d2) / ((d1 + d2) * f))    # in metres
clearance_required = 0.6 * r1
```

Sample-wise: for each profile sample at distance `s` from start,
clearance margin = (LoS line height at `s`) − (terrain at `s`) − 0.6·r1.
Per-segment verdict:

- `Clear` → all samples have margin > 0
- `Grazing` → smallest margin ∈ [−3 m, 0]
- `Blocked` → smallest margin < −3 m

Verdict chips render under the chart and as a tiny badge on each map
polyline segment.

### 6. Quick-path button

Inside any contact / heard popup add a `LoS to here` button. Clicking it
fast-paths to a two-pin LoS check (observer → that node) using the same
side panel.

## Edge cases

- Observer coords missing — disable the LoS button with a tooltip
  ("Set observer location in Domoticz Settings").
- Pins straddling the antimeridian — refuse pins more than 1 000 km
  apart (no sane LoRa LoS at that range; also keeps sample count sane).
- Elevation service unavailable — show a banner "Elevation service
  unreachable; profile unavailable" but keep the on-map pins/polyline so
  the user's work isn't lost.
- Rapid clicks — debounce profile fetches (250 ms after the last edit).

## Tests

- Unit (Python): elevation cache hit/miss/eviction/persistence/fallback.
- Manual UI:
  - drop 2 pins; profile chart renders.
  - drop 4 pins; chart shows 3 segments concatenated.
  - drag a pin marker (Leaflet `draggable: true`); chart updates.
  - Editing antenna height re-runs only the chart, no HTTP.
  - `LoS to here` from a contact popup pre-populates 2 pins.
  - Clear all empties pins and chart.

## Effort

~1.5 days end-to-end (elevation proxy: 3 h, frontend pins + overlay:
4 h, chart + Fresnel: 4 h, polish + tests: 3 h).

## Dependencies

- None on other features in this plan.
- **Provides** the elevation proxy that #4 reuses for terrain
  attenuation.
