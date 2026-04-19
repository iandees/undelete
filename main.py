"""OSM Changes — main entry point.

Runs the watcher daemon with periodic Parquet builds and R2 uploads.
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
from pipeline.build_parquet import ParquetBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

POLL_INTERVAL = 60


def write_and_upload_metadata(data_dir: Path, uploader: R2Uploader | None, r2_public_url: str):
    """Write metadata.json with available date range and upload it."""
    parquet_dir = data_dir / "parquet"
    if not parquet_dir.exists():
        return
    date_dirs = sorted(
        d.name.split("=", 1)[1]
        for d in parquet_dir.iterdir()
        if d.is_dir() and d.name.startswith("date=")
    )
    if not date_dirs:
        return

    metadata = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base_url": r2_public_url,
        "min_date": date_dirs[0],
        "max_date": date_dirs[-1],
        "dates": date_dirs,
    }
    metadata_path = parquet_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))
    if uploader:
        uploader.upload_file(metadata_path, "osm-changes/metadata.json")
    logger.info("Wrote metadata: %s to %s (%d dates)", date_dirs[0], date_dirs[-1], len(date_dirs))


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))

    watcher = Watcher(data_dir)
    parquet_builder = ParquetBuilder(data_dir / "deletions", data_dir / "parquet")

    uploader = None
    r2_endpoint = os.environ.get("R2_ENDPOINT_URL")
    r2_public_url = os.environ.get("R2_PUBLIC_URL", "")
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

    parquet_build_interval = int(os.environ.get("PARQUET_BUILD_INTERVAL", "300"))
    retention_days = int(os.environ.get("RETENTION_DAYS", "90"))

    last_seq = watcher.load_state()
    if last_seq is None:
        last_seq = watcher.get_latest_sequence()
        logger.info("No saved state, starting from latest sequence: %d", last_seq)
    else:
        logger.info("Resuming from saved sequence: %d", last_seq)

    last_parquet_build = 0.0

    logger.info("Starting watcher daemon (poll=%ds, parquet_build=%ds)",
                POLL_INTERVAL, parquet_build_interval)

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

        try:
            latest_seq = watcher.get_latest_sequence()
        except Exception:
            logger.exception("Failed to get latest sequence")
            latest_seq = last_seq

        while last_seq < latest_seq:
            next_seq = last_seq + 1
            try:
                count = watcher.fetch_and_process(next_seq)
                if count is None:
                    break
                if count > 0:
                    logger.info("Seq %d: %d changes", next_seq, count)
                last_seq = next_seq
                watcher.save_state(last_seq)
            except Exception:
                logger.exception("Failed to process seq %d", next_seq)
                break

        if (now - last_parquet_build) >= parquet_build_interval:
            last_parquet_build = now
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                built = parquet_builder.build(today_str)
                if built:
                    logger.info("Built parquet for %s", today_str)
                    if uploader:
                        parquet_file = data_dir / "parquet" / f"date={today_str}" / "data.parquet"
                        uploader.upload_file(parquet_file, f"osm-changes/date={today_str}/data.parquet")

                for geojsonl in sorted((data_dir / "deletions").glob("*.geojsonl")):
                    date_str = geojsonl.stem
                    if date_str == today_str:
                        continue
                    if parquet_builder.build(date_str):
                        logger.info("Built parquet for %s", date_str)
                        if uploader:
                            pf = data_dir / "parquet" / f"date={date_str}" / "data.parquet"
                            uploader.upload_file(pf, f"osm-changes/date={date_str}/data.parquet")
                        geojsonl.unlink()
                        logger.info("Cleaned up %s", geojsonl.name)

                write_and_upload_metadata(data_dir, uploader, r2_public_url)
                prune_old_files(data_dir / "parquet", retention_days)
                prune_old_files(data_dir / "deletions", retention_days)
            except Exception:
                logger.exception("Failed to build/upload parquet")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
