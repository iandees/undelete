"""OSM Undelete — main entry point.

Runs the watcher daemon with periodic tile builds and R2 uploads.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

from dotenv import load_dotenv

from daemon.watcher import Watcher
from daemon.geojson_writer import GeoJSONWriter
from pipeline.build_tiles import TileBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = 5  # seconds between adiff polls


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
    today_upload_interval = int(os.environ.get("TODAY_UPLOAD_INTERVAL", "60"))
    retention_days = int(os.environ.get("TILE_RETENTION_DAYS", "90"))

    # Determine starting sequence
    last_seq = watcher.load_state()
    if last_seq is None:
        last_seq = watcher.get_latest_sequence()
        logger.info("No saved state, starting from latest sequence: %d", last_seq)
    else:
        logger.info("Resuming from saved sequence: %d", last_seq)

    last_tile_build = 0.0
    last_today_upload = 0.0

    logger.info("Starting watcher daemon (poll=%ds, tile_build=%ds, today_upload=%ds)",
                POLL_INTERVAL, tile_build_interval, today_upload_interval)

    while True:
        now = time.time()

        # Poll for new adiffs
        try:
            latest_seq = watcher.get_latest_sequence()
        except Exception:
            logger.exception("Failed to get latest sequence, retrying in %ds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        if last_seq < latest_seq:
            next_seq = last_seq + 1
            try:
                count = watcher.fetch_and_process(next_seq)
                if count > 0:
                    logger.info("Seq %d: %d deletions", next_seq, count)
                last_seq = next_seq
                watcher.save_state(last_seq)
            except Exception:
                logger.exception("Failed to process seq %d", next_seq)
                time.sleep(POLL_INTERVAL)
                continue
        else:
            time.sleep(POLL_INTERVAL)

        # Periodic: upload today's GeoJSON
        if uploader and (now - last_today_upload) >= today_upload_interval:
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                writer = GeoJSONWriter(data_dir / "deletions")
                feature_count = writer.get_feature_count(today_str)
                if feature_count > 0:
                    tmp_path = data_dir / "today.geojson.tmp"
                    writer.write_feature_collection(today_str, tmp_path)
                    uploader.upload_today_geojson(tmp_path, feature_count)
                    tmp_path.unlink(missing_ok=True)
                last_today_upload = now
            except Exception:
                logger.exception("Failed to upload today.geojson")

        # Periodic: build tiles and upload
        if (now - last_tile_build) >= tile_build_interval:
            try:
                built = tile_builder.build_daily_tiles()
                if built:
                    logger.info("Built tiles for: %s", ", ".join(built))
                    tile_builder.merge_tiles()
                    if uploader:
                        merged = data_dir / "tiles" / "merged.pmtiles"
                        if merged.exists():
                            uploader.upload_file(merged, "merged.pmtiles")

                # Prune old files
                prune_old_files(data_dir / "deletions", retention_days)
                prune_old_files(data_dir / "tiles", retention_days)

                last_tile_build = now
            except Exception:
                logger.exception("Failed to build/upload tiles")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
