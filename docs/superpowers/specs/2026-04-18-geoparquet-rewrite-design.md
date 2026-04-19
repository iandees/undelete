# OSM Changes GeoParquet Rewrite

## Overview

Rewrite the OSM undelete tool from a deletion-only PMTiles viewer into a general-purpose OSM change explorer. The daemon captures all adiff actions (create, modify, delete) and produces daily GeoParquet files uploaded to R2. The frontend provides a DuckDB WASM SQL query interface with map visualization and a results table.

## Backend: Adiff Ingest & Parquet Pipeline

### Adiff Parser

Modify `daemon/adiff_parser.py` to capture all action types (create, modify, delete), not just deletes.

Each action produces a record with:
- Action type (create, modify, delete)
- Old state: tags and geometry (null for creates)
- New state: tags and geometry (null for deletes... though deletes have the "old" as the final known state)
- Metadata: user, uid, changeset, timestamp, version, osm_type, osm_id

Geometry construction per element type:
- **Nodes:** Point from lon/lat
- **Ways:** LineString or Polygon (if closed ring)
- **Relations:** MultiPolygon if `type=multipolygon` or `type=boundary` (assembled from member way geometries), otherwise Point from bounds center

All geometries stored as WKB.

### GeoJSON Writer

Continue using `daemon/geojson_writer.py` to append line-delimited JSON to daily `YYYY-MM-DD.geojsonl` files. Each line now includes the full old+new record for all action types. This acts as a write-ahead log for the Parquet conversion step.

### Parquet Conversion

New module `pipeline/build_parquet.py` replaces `pipeline/build_tiles.py`.

- Runs periodically (~every 5 minutes)
- Reads the day's `.geojsonl` file
- Converts to GeoParquet with WKB geometry columns and proper GeoParquet metadata (CRS=WGS84)
- Uses `geopandas` + `pyarrow` for GeoParquet writing with standard metadata
- Uses `shapely` for WKB geometry construction and multipolygon assembly

### Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `action` | string | `create`, `modify`, `delete` |
| `osm_type` | string | `node`, `way`, `relation` |
| `osm_id` | int64 | OSM element ID |
| `version` | int32 | Version number (new state) |
| `changeset` | int64 | Changeset ID |
| `user` | string | Username who made this change |
| `uid` | int64 | User ID |
| `timestamp` | string | ISO 8601 timestamp of the change |
| `tags` | string | JSON object of new/current tags. For creates: the created tags. For modifies: the new tags. For deletes: the tags at time of deletion. |
| `old_tags` | string | JSON object of previous tags. Null for creates. For modifies: the pre-modification tags. For deletes: null (tags has the final state). |
| `geometry` | binary (WKB) | New/current geometry. For creates: the created geometry. For modifies: the new geometry. For deletes: the geometry at time of deletion. |
| `old_geometry` | binary (WKB) | Previous geometry. Null for creates. For modifies: the pre-modification geometry. For deletes: null (geometry has the final state). |

### Storage on R2

Hive-partitioned layout:

```
osm-changes/
  date=2026-04-17/data.parquet
  date=2026-04-18/data.parquet
  metadata.json
```

DuckDB queries across partitions:
```sql
SELECT * FROM read_parquet('https://r2.example.com/osm-changes/date=*/data.parquet', hive_partitioning=true)
WHERE date = '2026-04-18'
```

No manifest needed for DuckDB — it discovers partitions from the path structure. A small `metadata.json` provides the available date range and R2 base URL so the frontend can populate example queries.

### Orchestration (main.py)

Same loop structure as current:
- Poll adiff stream every 60 seconds
- Parse all actions, append to daily `.geojsonl`
- Every ~5 minutes: convert today's `.geojsonl` to GeoParquet, upload to R2, update `metadata.json`
- Prune files older than 90 days (same logic, `.parquet` files instead of `.pmtiles`)

## Frontend: DuckDB WASM Query Interface

### Stack

- DuckDB WASM for querying remote Parquet files
- MapLibre GL JS for map rendering
- Vanilla JS (no framework)
- Plain `<textarea>` or lightweight editor for SQL input

### Layout (stacked vertical)

1. **Query bar** at top — multi-line SQL editor with "Run" button, resizable
2. **Map** in the middle — renders query result geometries
3. **Results table** at bottom — scrollable data table, resizable split with map

### Query Helpers

- Clickable example queries: "Today's deletes", "Changes by user", "Modified buildings in view"
- "Filter by map bounds" button — inserts `ST_Within(geometry, ST_MakeEnvelope(...))` using current viewport
- Example queries pre-filled with R2 base URL and current date from `metadata.json`

### Result Rendering

- DuckDB query runs entirely client-side via WASM
- Results with a `geometry` column rendered on map as GeoJSON (WKB→GeoJSON conversion in JS)
- All result columns shown in the scrollable table
- Click row in table → highlight/zoom feature on map
- Click feature on map → highlight row in table
- Map popups show key metadata + link to OSM history

### DuckDB WASM Setup

- Load `spatial` extension for `ST_*` functions and WKB handling
- Configure `httpfs` for remote Parquet access via R2
- Async query execution with loading indicator

### R2 CORS

Must expose headers for DuckDB WASM HTTP range requests: `Range`, `Content-Range`, `Content-Length`.

## Deployment & Dependencies

### New Python Dependencies

- `geopandas` — GeoParquet writing with proper metadata
- `pyarrow` — Parquet file format
- `shapely` — WKB geometry construction, multipolygon assembly

### Removed Dependencies

- `tippecanoe` system dependency (no more PMTiles)

### Docker

- Remove tippecanoe build stage (significant image simplification)
- Add Python geo packages

### Files Changed

| File | Change |
|------|--------|
| `daemon/adiff_parser.py` | Capture all actions + multipolygon assembly |
| `daemon/geojson_writer.py` | Updated schema with old+new fields |
| `pipeline/build_tiles.py` | Removed, replaced by `pipeline/build_parquet.py` |
| `pipeline/build_parquet.py` | New: GeoJSONL → GeoParquet conversion |
| `pipeline/merge_upload.py` | Updated for Hive-partitioned path structure |
| `pipeline/prune.py` | Same logic, `.parquet` extension |
| `main.py` | Swap tile build for parquet build, adjust intervals |
| `web/index.html` | Complete rewrite: DuckDB WASM + map + table |
| `serve.py` | Kept as-is for local dev |
| `Dockerfile` | Remove tippecanoe stage, add geo packages |
| `pyproject.toml` | Add geopandas, pyarrow, shapely |

### Unchanged

- `daemon/watcher.py` — polling logic
- State tracking via `last_seq.txt`
- `.env` config pattern
- R2 upload credentials/config
