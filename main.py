"""OSM Undelete — main entry point.

Runs the watcher daemon with periodic tile builds and R2 uploads.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from daemon.watcher import Watcher
from pipeline.build_tiles import TileBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

POLL_INTERVAL = 60  # seconds between checks for new sequences


def write_and_upload_manifest(data_dir: Path, uploader: R2Uploader | None):
    """Write manifest.json listing all daily tiles and upload it."""
    daily_tiles = sorted((data_dir / "tiles").glob("????-??-??.pmtiles"))
    if daily_tiles:
        manifest = {
            "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "files": [f.name for f in daily_tiles],
        }
        manifest_path = data_dir / "tiles" / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))
        if uploader:
            logger.debug("Uploading manifest.json")
            uploader.upload_file(manifest_path, "manifest.json")
        logger.info("Wrote manifest with %d files", len(daily_tiles))


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))

    watcher = Watcher(data_dir)
    tile_builder = TileBuilder(data_dir / "deletions", data_dir / "tiles")

    # R2 uploader (optional — skip if not configured)
    uploader = None
    r2_endpoint = os.environ.get("R2_ENDPOINT_URL")
    if r2_endpoint:
        uploader = R2Uploader(
            endpoint_url=r2_endpoint,
            access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            bucket_name=os.environ["R2_BUCKET_NAME"],
        )
        logger.info("R2 upload enabled: %s/%s", r2_endpoint, os.environ["R2_BUCKET_NAME"])
    else:
        logger.info("R2 upload disabled (R2_ENDPOINT_URL not set)")

    tile_build_interval = int(os.environ.get("TILE_BUILD_INTERVAL", "600"))
    today_build_interval = int(os.environ.get("TODAY_BUILD_INTERVAL", "60"))
    retention_days = int(os.environ.get("TILE_RETENTION_DAYS", "90"))

    # Determine starting sequence
    last_seq = watcher.load_state()
    if last_seq is None:
        last_seq = watcher.get_latest_sequence()
        logger.info("No saved state, starting from latest sequence: %d", last_seq)
    else:
        logger.info("Resuming from saved sequence: %d", last_seq)

    last_tile_build = 0.0
    last_today_build = 0.0
    last_today_mtime = 0.0

    logger.info("Starting watcher daemon (poll=%ds, tile_build=%ds, today_build=%ds)",
                POLL_INTERVAL, tile_build_interval, today_build_interval)

    next_poll = 0.0

    while True:
        now = time.time()
        sleep_for = next_poll - now
        if sleep_for > 0:
            logger.debug("Sleeping %.1fs until next poll", sleep_for)
            time.sleep(sleep_for)
        now = time.time()
        next_poll = now + POLL_INTERVAL
        logger.debug("Loop tick: last_seq=%d", last_seq)

        # Poll for new adiffs
        try:
            logger.debug("Fetching latest sequence number")
            latest_seq = watcher.get_latest_sequence()
            logger.debug("Latest sequence: %d", latest_seq)
        except Exception:
            logger.exception("Failed to get latest sequence")
            latest_seq = last_seq  # fall through to tile builds

        # Process all available sequences before polling again
        while last_seq < latest_seq:
            next_seq = last_seq + 1
            try:
                logger.debug("Fetching and processing seq %d (latest=%d)", next_seq, latest_seq)
                count = watcher.fetch_and_process(next_seq)
                if count is None:
                    logger.debug("Seq %d not yet available (404), will retry", next_seq)
                    break
                logger.debug("Processed seq %d: %d deletions", next_seq, count)
                if count > 0:
                    logger.info("Seq %d: %d deletions", next_seq, count)
                last_seq = next_seq
                watcher.save_state(last_seq)
            except Exception:
                logger.exception("Failed to process seq %d", next_seq)
                break

        # Periodic: rebuild today's PMTiles from today's geojsonl
        if (now - last_today_build) >= today_build_interval:
            last_today_build = now
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                today_geojsonl = data_dir / "deletions" / f"{today_str}.geojsonl"
                if today_geojsonl.exists():
                    mtime = today_geojsonl.stat().st_mtime
                    if mtime > last_today_mtime:
                        today_pmtiles = data_dir / "tiles" / f"{today_str}.pmtiles"
                        logger.debug("Building today's tiles: %s", today_pmtiles)
                        tile_builder.build_tiles(today_geojsonl, today_pmtiles)
                        if uploader:
                            logger.debug("Uploading today's tiles: %s", today_str)
                            uploader.upload_file(today_pmtiles, f"{today_str}.pmtiles")
                            write_and_upload_manifest(data_dir, uploader)
                        logger.info("Built and uploaded %s.pmtiles", today_str)
                        last_today_mtime = mtime
            except Exception:
                logger.exception("Failed to build/upload today's tiles")

        # Periodic: build tiles for older days + write manifest
        if (now - last_tile_build) >= tile_build_interval:
            last_tile_build = now
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                logger.debug("Building daily tiles")
                built = tile_builder.build_daily_tiles()
                if built:
                    logger.info("Built tiles for: %s", ", ".join(built))
                    if uploader:
                        for date_str in built:
                            pmtiles_file = data_dir / "tiles" / f"{date_str}.pmtiles"
                            logger.debug("Uploading %s", pmtiles_file.name)
                            uploader.upload_file(pmtiles_file, f"{date_str}.pmtiles")

                # Delete geojsonl files for past days that have been tiled
                for geojsonl in (data_dir / "deletions").glob("*.geojsonl"):
                    date_str = geojsonl.stem
                    if date_str == today_str:
                        continue
                    pmtiles_file = data_dir / "tiles" / f"{date_str}.pmtiles"
                    if pmtiles_file.exists():
                        geojsonl.unlink()
                        logger.info("Cleaned up %s", geojsonl.name)

                # Write and upload manifest
                write_and_upload_manifest(data_dir, uploader)

                # Prune old pmtiles
                logger.debug("Pruning old tiles")
                prune_old_files(data_dir / "tiles", retention_days)
            except Exception:
                logger.exception("Failed to build/upload tiles")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
