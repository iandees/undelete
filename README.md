# OSM Undelete

A system that watches OpenStreetMap augmented diffs, extracts deleted objects, and presents them on an interactive map. Users can browse recent deletions, inspect tags and metadata, and download deleted objects to re-add them via JOSM or other editors.

## How it works

1. A Python daemon polls [augmented diffs](https://adiffs.osmcha.org/) from OSM's minutely replication feed
2. Deleted objects (nodes, ways, relations) are extracted with their full geometry and tags
3. Deletions are stored in daily line-delimited GeoJSON files
4. [Tippecanoe](https://github.com/felt/tippecanoe) periodically converts the GeoJSON into PMTiles
5. The merged PMTiles file and today's GeoJSON are uploaded to Cloudflare R2
6. A static web map displays the data using MapLibre GL JS and the PMTiles protocol

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
- [tippecanoe](https://github.com/felt/tippecanoe) for tile generation
- Cloudflare R2 bucket (optional — works locally without it)

## Setup

```bash
# Install dependencies
uv sync

# Copy and configure environment
cp .env.example .env
# Edit .env with your R2 credentials (optional)
```

## Running the daemon

```bash
uv run python main.py
```

The daemon will:
- Poll for new augmented diffs every 5 seconds
- Write deleted objects to `data/deletions/YYYY-MM-DD.geojsonl`
- Upload today's GeoJSON to R2 every 60 seconds (if configured)
- Rebuild PMTiles every 10 minutes (if tippecanoe is installed)
- Prune files older than 90 days

All intervals are configurable via `.env`.

## Local development

To test the web map locally without R2:

```bash
# Generate some data (fetches the last hour of deletions)
uv run python -c "
from daemon.watcher import Watcher
from pipeline.build_tiles import TileBuilder
from pathlib import Path

w = Watcher(Path('./data'))
seq = w.get_latest_sequence()
for s in range(seq - 60, seq + 1):
    w.fetch_and_process(s)

tb = TileBuilder(Path('./data/deletions'), Path('./data/tiles'))
tb.build_daily_tiles()
tb.merge_tiles()
"

# Start the dev server (serves map + tiles with CORS and byte range support)
uv run python serve.py
```

Open http://localhost:8080/web/ and enter `http://localhost:8080/data/tiles/merged.pmtiles` in the config bar.

## Web map features

- Click any deleted object to see its tags, deletion timestamp, changeset, and user
- **History** link to view the object's history on openstreetmap.org
- **Open in JOSM** to load the area in JOSM via remote control
- **Download .osm** to get a minimal OSM XML file for re-adding the object

## Configuration

See `.env.example` for all available settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` | Local data directory |
| `R2_ENDPOINT_URL` | | Cloudflare R2 S3-compatible endpoint |
| `R2_ACCESS_KEY_ID` | | R2 access key |
| `R2_SECRET_ACCESS_KEY` | | R2 secret key |
| `R2_BUCKET_NAME` | `osm-undelete` | R2 bucket name |
| `TILE_RETENTION_DAYS` | `90` | Days to keep old tile/GeoJSON files |
| `TILE_BUILD_INTERVAL` | `600` | Seconds between tile rebuilds |
| `TODAY_UPLOAD_INTERVAL` | `60` | Seconds between today.geojson uploads |

## Running tests

```bash
uv run pytest
```

## Project structure

```
├── daemon/
│   ├── adiff_parser.py      # Parse augmented diff XML, extract deletions
│   ├── geojson_writer.py    # Write features to daily GeoJSON files
│   └── watcher.py           # Poll adiffs, track sequence state
├── pipeline/
│   ├── build_tiles.py       # tippecanoe + tile-join
│   ├── merge_upload.py      # Upload to R2 via boto3
│   └── prune.py             # Delete old files past retention
├── web/
│   └── index.html           # Static map (MapLibre + PMTiles)
├── main.py                  # Daemon entry point
└── serve.py                 # Local dev server
```
