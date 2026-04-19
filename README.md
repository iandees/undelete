# OSM Undelete

A system that watches OpenStreetMap augmented diffs, extracts changes (creates, modifies, and deletes), and presents them on an interactive map. Users can browse recent changes, inspect tags and metadata, and use the built-in SQL query editor to filter by action type, tags, area, and more.

## How it works

1. A Python daemon polls [augmented diffs](https://adiffs.osmcha.org/) from OSM's minutely replication feed
2. Changed objects (nodes, ways, relations) are extracted with their full geometry, current tags, and previous tags
3. Changes are stored as daily GeoParquet files with MAP-typed tag columns and Hilbert-sorted geometries
4. GeoParquet files are uploaded to Cloudflare R2
5. A static web app queries the data in-browser using DuckDB-WASM and displays results on a MapLibre GL JS map

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for dependency management
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
- Write changed objects to daily GeoParquet files in `data/deletions/`
- Upload today's Parquet file to R2 every 60 seconds (if configured)
- Prune files older than 90 days

All intervals are configurable via `.env`.

## Local development

To test the web app locally without R2:

```bash
# Generate some data (fetches the last hour of changes)
uv run python -c "
from daemon.watcher import Watcher
from pathlib import Path

w = Watcher(Path('./data'))
seq = w.get_latest_sequence()
for s in range(seq - 60, seq + 1):
    w.fetch_and_process(s)
"

# Start the dev server (serves web app + data with CORS)
uv run python serve.py
```

Open http://localhost:8080/web/ and use the SQL query editor to explore the data. The web app uses DuckDB-WASM to query GeoParquet files directly in the browser.

## Web map features

- **SQL query editor** with example queries for filtering by action, tags, area, and date
- Click any object to see its tags, timestamp, changeset, and user
- **Tag diff view** for modified objects — added tags highlighted green, removed tags in red with strikethrough, changed values shown as old → new in yellow
- **History** link to view the object's history on openstreetmap.org
- Query results persisted in localStorage across page reloads

## Configuration

See `.env.example` for all available settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./data` | Local data directory |
| `R2_ENDPOINT_URL` | | Cloudflare R2 S3-compatible endpoint |
| `R2_ACCESS_KEY_ID` | | R2 access key |
| `R2_SECRET_ACCESS_KEY` | | R2 secret key |
| `R2_BUCKET_NAME` | `osm-undelete` | R2 bucket name |
| `TILE_RETENTION_DAYS` | `90` | Days to keep old data files |
| `TILE_BUILD_INTERVAL` | `600` | Seconds between Parquet rebuilds |
| `TODAY_UPLOAD_INTERVAL` | `60` | Seconds between uploads of today's data |

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
│   ├── build_parquet.py     # Convert GeoJSON to GeoParquet with MAP tags
│   ├── merge_upload.py      # Upload to R2 via boto3
│   └── prune.py             # Delete old files past retention
├── web/
│   └── index.html           # Web app (DuckDB-WASM + MapLibre GL JS)
├── main.py                  # Daemon entry point
└── serve.py                 # Local dev server
```
