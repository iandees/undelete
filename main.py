"""OSM Changes — main entry point.

Runs the replication watcher (background thread) and changeset watcher, with periodic
Parquet builds and R2 uploads.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from daemon.changeset_watcher import ChangesetWatcher
from daemon.watcher import Watcher
from pipeline.build_changeset_parquet import ChangesetParquetBuilder
from pipeline.build_parquet import ParquetBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files
from pipeline.publish import DailyPublisher

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
# LOG_LEVEL applies to this project's loggers only, so DEBUG doesn't also turn on urllib3's
# per-connection chatter
for _name in ("osm-changes", "daemon", "pipeline"):
    logging.getLogger(_name).setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger("osm-changes")

POLL_INTERVAL = 60
# Max changeset sequences to process per loop tick before yielding for parquet builds.
# Prevents unbounded memory growth during catch-up.
MAX_SEQS_PER_TICK = 100


def write_and_upload_metadata(
    parquet_dir: Path, uploader: R2Uploader | None, r2_public_url: str, r2_prefix: str,
):
    """Write metadata.json with available date range and upload it.

    With R2 the dates are what is published there: local folders are pruned after the
    retention period and may hold a date whose upload hasn't happened, and the site fails
    to load a date that is listed but missing.
    """
    if uploader:
        date_dirs = uploader.list_dates(r2_prefix)
    elif parquet_dir.exists():
        date_dirs = sorted(
            d.name.split("=", 1)[1]
            for d in parquet_dir.iterdir()
            if d.is_dir() and d.name.startswith("date=")
        )
    else:
        return
    if not date_dirs:
        return
    parquet_dir.mkdir(parents=True, exist_ok=True)

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
        uploader.upload_file(metadata_path, f"{r2_prefix}/metadata.json")
    logger.info("Wrote %s metadata: %s to %s (%d dates)", r2_prefix, date_dirs[0], date_dirs[-1], len(date_dirs))


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))

    watcher = Watcher(data_dir)
    parquet_builder = ParquetBuilder(data_dir / "deletions", data_dir / "parquet")

    cs_watcher = ChangesetWatcher(data_dir, workers=int(os.environ.get("CHANGESET_WORKERS", "4")))
    cs_parquet_builder = ChangesetParquetBuilder(data_dir / "changesets", data_dir / "changeset_parquet")

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

    cs_last_seq = cs_watcher.load_state()
    if cs_last_seq is None:
        cs_last_seq = cs_watcher.get_latest_sequence()
        logger.info("No saved changeset state, starting from latest sequence: %d", cs_last_seq)
    else:
        logger.info("Resuming changesets from saved sequence: %d", cs_last_seq)

    last_parquet_build = 0.0
    merge_interval = int(os.environ.get("LATE_MERGE_INTERVAL", "3600"))
    work_dir = data_dir / "work"
    element_publisher = DailyPublisher(
        "elements", data_dir / "deletions", ".geojsonl", data_dir / "parquet", parquet_builder,
        "osm-changes", uploader=uploader, work_dir=work_dir, merge_interval=merge_interval)
    changeset_publisher = DailyPublisher(
        "changesets", data_dir / "changesets", ".jsonl", data_dir / "changeset_parquet",
        cs_parquet_builder, "osm-changesets", uploader=uploader, work_dir=work_dir,
        merge_interval=merge_interval)

    logger.info("Starting watcher daemon (poll=%ds, parquet_build=%ds)",
                POLL_INTERVAL, parquet_build_interval)
    # Element catch-up runs in its own thread: a single day diff takes hours to process
    watcher.start(POLL_INTERVAL)

    next_poll = 0.0

    while True:
        now = time.time()
        sleep_for = next_poll - now
        if sleep_for > 0:
            time.sleep(sleep_for)
        now = time.time()
        next_poll = now + POLL_INTERVAL

        # Fetch changeset replication
        try:
            cs_latest_seq = cs_watcher.get_latest_sequence()
        except Exception:
            logger.exception("Failed to get latest changeset sequence")
            cs_latest_seq = cs_last_seq

        cs_last_seq, cs_seqs_processed = cs_watcher.catch_up(
            cs_last_seq, cs_latest_seq, MAX_SEQS_PER_TICK)

        # If we're still catching up, skip the sleep and loop immediately
        still_catching_up = cs_seqs_processed >= MAX_SEQS_PER_TICK
        if still_catching_up:
            next_poll = time.time()  # don't sleep, loop immediately

        if (now - last_parquet_build) >= parquet_build_interval:
            last_parquet_build = now
            try:
                keep_from = (datetime.now(timezone.utc).date() - timedelta(days=retention_days)).isoformat()

                # Element changes. The publisher builds each live day's parquet from its raw
                # file, publishes a settled day and deletes its raw file, and merges rows that
                # arrive later for a published day into its parquet (see pipeline/publish.py).
                # It runs under the watcher's lock so it never reads a half-written batch.
                with watcher.lock:
                    element_publisher.run(watcher.complete_before(), keep_from)

                # Prune before writing metadata so it never lists a date that is about to go
                prune_old_files(data_dir / "parquet", retention_days)
                write_and_upload_metadata(data_dir / "parquet", uploader, r2_public_url, "osm-changes")

                # Changesets are filed by the date they were created, so several recent
                # dates are live at once.
                changeset_publisher.run(cs_watcher.complete_before(), keep_from)

                prune_old_files(data_dir / "changeset_parquet", retention_days)
                write_and_upload_metadata(data_dir / "changeset_parquet", uploader, r2_public_url, "osm-changesets")
                # Raw files are not pruned by age: each is deleted once it has been published
                # (or dropped when older than the retention and there is no R2), and an
                # unpublished one holds changes that exist nowhere else.
            except Exception:
                logger.exception("Failed to build/upload parquet")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
