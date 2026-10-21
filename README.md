# OSM Undelete

A system that watches OpenStreetMap replication, extracts changes (creates, modifies, and deletes), and presents them on an interactive map. Users can browse recent changes, inspect tags and metadata, and use the built-in SQL query editor to filter by action type, tags, area, and more.

## How it works

1. A Python daemon follows planet's [replication diffs](https://planet.openstreetmap.org/replication/). It plans the fewest files that get from the last processed time to "now": minute diffs up to the next hour, hour diffs up to midnight, whole day diffs, then back down. Catching up on a month takes about 110 files instead of 43,000
2. Planet diffs only contain the new state of each element, so the daemon fills in old tags and old/new geometry with batch lookups against the OSM API (rate limited), using a throwaway SQLite file so memory stays small however large the diff is
3. Changed objects (nodes, ways, relations) are written with their full geometry, current tags, and previous tags
4. Changes are stored as daily GeoParquet files with MAP-typed tag columns and Hilbert-sorted geometries
5. GeoParquet files are uploaded to Cloudflare R2
6. A static web app queries the data in-browser using DuckDB-WASM and displays results on a MapLibre GL JS map

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
- Follow element changes in a background thread, using day/hour/minute diffs depending on how far behind it is
- Follow changeset replication (catching up on its small static files with parallel downloads, committed strictly in order), and write both to daily files in `data/deletions/` and `data/changesets/`
- Build GeoParquet every `PARQUET_BUILD_INTERVAL` seconds and upload it to R2 (if configured)
- Delete a day's raw file only once that day is settled and its parquet was built and uploaded (changesets are filed by the date they were created, and a date settles two days after it starts)
- Merge rows that arrive later for a day that is already published into that day's parquet, instead of rebuilding it from the late rows alone: a new comment on a months-old changeset replaces that changeset's row, and elements already in the file are skipped. R2 is treated as the source of truth for what is published, so the old file is downloaded from there when the local copy has been pruned. Late rows for one day are merged at most once per `LATE_MERGE_INTERVAL`
- Prune local parquet older than `RETENTION_DAYS`. This applies to local files only: nothing is ever deleted from R2, and raw files are only removed after they have been published

It resumes from `data/state/osc_position.json` (or an older `last_seq.txt`) and by default captures every change since then, however long it was down. Catching up costs about 3.5 hours of OSM API time per day of backlog; set `CATCHUP_MAX_DAYS` to bound it, at the price of a gap in the data.

All intervals are configurable via `.env`.

To see how it would catch up without processing anything:

```bash
uv run python -m daemon.watcher                 # route from the saved position
uv run python -m daemon.watcher --days-back 30  # pretend to be 30 days behind
```

## Local development

To test the web app locally without R2:

```bash
# Generate some data (the last few minutes of changes)
uv run python -c "
from daemon.watcher import Watcher
from pathlib import Path

Watcher(Path('./data')).catch_up(max_steps=5)
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
| `DATA_DIR` | `./data` | Local data directory (also holds temp files; a day diff needs several GB free) |
| `R2_ENDPOINT_URL` | | Cloudflare R2 S3-compatible endpoint |
| `R2_ACCESS_KEY_ID` | | R2 access key |
| `R2_SECRET_ACCESS_KEY` | | R2 secret key |
| `R2_BUCKET_NAME` | `osm-undelete` | R2 bucket name |
| `R2_PUBLIC_URL` | | Public base URL written into `metadata.json` |
| `RETENTION_DAYS` | `90` | Days to keep parquet locally (R2 keeps everything; it is also what late rows are merged into) |
| `PARQUET_BUILD_INTERVAL` | `300` | Seconds between Parquet rebuilds |
| `CATCHUP_MAX_DAYS` | unlimited | Optional: skip ahead rather than catch up on more than this many days, which leaves a gap in the data. Unset (or `0`/`none`) captures everything |
| `REPLICATION_MIRROR` | `s3` | Where replication files are downloaded from: `s3` (the AWS bucket planet redirects to) or `planet` (planet.openstreetmap.org, which times out under parallel connections) |
| `CHANGESET_WORKERS` | `4` | Parallel changeset-file downloads while catching up (about 23 files/s from the S3 mirror, 43/s at 8). Keep it at 4 or lower with `REPLICATION_MIRROR=planet` |
| `LOG_LEVEL` | `INFO` | Set to `DEBUG` for per-sequence changeset detail |
| `CHANGESET_SETTLE_DAYS` | `2` | Changesets are filed by creation date; a date is published this long after it starts (open changesets can change for about 24h). Later updates, mostly new comments on old changesets, are merged into the published file. Old name: `CHANGESET_FREEZE_DAYS` |
| `LATE_MERGE_INTERVAL` | `3600` | Seconds between merges of late rows into the same published day |
| `OSM_API_MIN_INTERVAL` | `0.5` | Minimum seconds between OSM API requests |
| `OSM_USER_AGENT` | `osm-undelete/0.1 (+https://github.com/iandees/undelete)` | User-Agent for OSM API requests; add your own contact |

API volume is driven by how many distinct nodes were edited, not by diff size: roughly 1,000 API requests per hour of OSM edits, so at the default throttle each hour of data costs about 9 minutes and a full day about 3.5 hours.

## Running tests

```bash
uv run pytest
```

## Project structure

```
├── daemon/
│   ├── mirror.py               # Where replication files are downloaded from (S3 or planet)
│   ├── replication_planner.py  # Pick day/hour/minute diffs that get to "now" fastest
│   ├── osc_augmenter.py        # osmChange + OSM API -> augmented-diff-style features (SQLite-backed)
│   ├── watcher.py              # Follow replication, stage and write features, track position
│   ├── adiff_parser.py         # Geometry building (and parsing of augmented diff XML)
│   ├── geojson_writer.py       # Write features to daily GeoJSON files
│   ├── changeset_parser.py     # Parse changeset replication
│   └── changeset_watcher.py    # Poll changeset replication
├── pipeline/
│   ├── build_parquet.py     # Convert GeoJSON to GeoParquet with MAP tags
│   ├── build_changeset_parquet.py
│   ├── merge_upload.py      # Upload to R2 via boto3
│   ├── publish.py           # Publish daily raw files; merge late rows into published parquet
│   └── prune.py             # Delete old local parquet past retention
├── web/
│   └── index.html           # Web app (DuckDB-WASM + MapLibre GL JS)
├── main.py                  # Daemon entry point
└── serve.py                 # Local dev server
```
