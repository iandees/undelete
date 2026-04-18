# OSM Undelete — Design Spec

## Problem

It's difficult to find objects that people delete in OpenStreetMap. There's no easy way to browse recent deletions geographically or recover deleted data.

## Solution

A system that watches OSM augmented diffs, extracts deleted objects, and presents them on an interactive map where users can browse, inspect, and download deleted objects to re-add them via their map editor.

## Architecture Overview

```
adiffs.osmcha.org
       │ poll
       ▼
  Python Daemon ──► daily GeoJSON (local)
                         │
                    ┌────┴────┐
                    ▼         ▼
              tippecanoe   today.geojson → R2
                    │
                    ▼
               tile-join
                    │
                    ▼
            merged.pmtiles → R2
                    │
                    ▼
          Static Web Map (MapLibre + PMTiles JS)
                    │
                    ▼
          Click → popup → JOSM link / .osm download
```

## Component 1: Adiff Watcher Daemon

A Python long-running process.

### Responsibilities

- Poll `https://adiffs.osmcha.org/` for new augmented diff files
- Track the last-seen sequence number in `data/state/last_seq.txt`
- Parse each adiff XML, extract deleted objects (nodes, ways, relations)
- For each deletion, extract all OSM fields: object type, ID, version, all tags, full resolved geometry, changeset ID, user, uid, timestamp
- Append deleted objects as line-delimited GeoJSON features to `data/deletions/YYYY-MM-DD.geojson`
- Upload `data/deletions/today.geojson` to R2 frequently (every ~1 minute) so recent deletions appear quickly on the map
- Handle restarts by resuming from the last-seen sequence number

### Adiff Format

Augmented diffs from osmcha provide XML containing `<action type="delete">` elements with the full object data including resolved coordinates for ways and relations. The daemon parses these to extract geometry and metadata.

### GeoJSON Output

Each deleted object becomes a GeoJSON Feature with:

```json
{
  "type": "Feature",
  "geometry": { "type": "Point|LineString|Polygon|...", "coordinates": [...] },
  "properties": {
    "osm_type": "node|way|relation",
    "osm_id": 123456789,
    "version": 5,
    "changeset": 987654,
    "user": "username",
    "uid": 12345,
    "timestamp": "2026-04-17T12:00:00Z",
    "tags": { "name": "Example", "highway": "residential", ... }
  }
}
```

For ways: geometry is LineString or Polygon (closed ways). For relations: geometry is the resolved multipolygon or geometry collection. For nodes: geometry is Point.

### Dependencies

- `requests` — HTTP polling
- `lxml` — XML parsing

## Component 2: PMTiles Generation Pipeline

Runs periodically within the daemon process (e.g., every 10 minutes).

### Responsibilities

- Check for new/updated daily GeoJSON files in `data/deletions/`
- Run `tippecanoe` on each day's GeoJSON to produce `data/tiles/YYYY-MM-DD.pmtiles`
  - Preserve all features at high zoom levels
  - Drop features at lower zooms to keep tile sizes reasonable
  - Store all properties in tiles so the frontend can display them
- Run `tile-join` to merge all daily PMTiles into `data/tiles/merged.pmtiles`
- Upload `merged.pmtiles` to R2 via boto3 (S3-compatible API)
- Prune daily GeoJSON and PMTiles files older than a configurable retention period (default: 90 days)

### Today's Data

The daemon uploads today's GeoJSON to R2 on a fast cadence (~1 minute) so the frontend can show very recent deletions without waiting for a tile rebuild. At the end of the day, today's file becomes the date-stamped daily file and enters the normal tile pipeline.

### Configuration

Stored in `.env`:

- `R2_ENDPOINT_URL` — Cloudflare R2 S3-compatible endpoint
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET_NAME`
- `TILE_RETENTION_DAYS` — how many days of tiles to keep (default: 90)
- `TILE_BUILD_INTERVAL` — seconds between tile rebuilds (default: 600)
- `TODAY_UPLOAD_INTERVAL` — seconds between today.geojson uploads (default: 60)

## Component 3: Static Web Map

A single `index.html` file, deployable anywhere (R2 static hosting, GitHub Pages, etc.).

### Map Rendering

- MapLibre GL JS as the map renderer
- pmtiles JS library to read `merged.pmtiles` from R2 via HTTP range requests
- Today's GeoJSON loaded as a separate MapLibre source, auto-refreshed every 60 seconds
- Deleted objects styled in red, with visual distinction between nodes (circles), ways (lines), and relations (polygons)

### Click Interaction

Clicking a deleted object shows a popup with:

- Object type and ID (e.g., "way/123456789"), linked to OSM history
- All tags displayed in a table
- Deletion timestamp, changeset (linked), and user (linked)
- **JOSM remote control link** — opens the object's location in JOSM via `http://localhost:8111/load_and_zoom`
- **Download .osm XML** — generates a minimal `.osm` file from the stored feature data, suitable for opening in JOSM or other editors

### Dependencies (CDN)

- MapLibre GL JS
- PMTiles JS

## Component 4: Project Structure

```
undelete/
├── daemon/
│   ├── watcher.py          # Main daemon loop (polling + scheduling)
│   ├── adiff_parser.py     # Parse augmented diff XML, extract deletions
│   └── geojson_writer.py   # Write deletions to daily GeoJSON files
├── pipeline/
│   ├── build_tiles.py      # Run tippecanoe on daily GeoJSON files
│   ├── merge_upload.py     # tile-join + upload merged PMTiles to R2
│   └── prune.py            # Delete old daily files past retention
├── web/
│   └── index.html          # Static map page
├── data/
│   ├── deletions/          # Daily line-delimited GeoJSON files
│   ├── tiles/              # Daily + merged PMTiles files
│   └── state/              # last_seq.txt
├── .env.example            # R2 credentials template
├── requirements.txt
└── README.md
```

The daemon runs as a single process. It polls for adiffs on its main loop and triggers the tile build pipeline on a timer (every 10 minutes by default). Today's GeoJSON is uploaded on a faster timer (~1 minute).

## Out of Scope (for now)

- User accounts or authentication
- Filtering by object type, tags, or geographic area on the map (can add later)
- Automatic re-adding of objects (users do this manually via JOSM)
- Monitoring/alerting on the daemon
- Multiple output formats beyond PMTiles
