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

from daemon.changeset_watcher import ChangesetWatcher
from daemon.watcher import Watcher
from pipeline.build_changeset_parquet import ChangesetParquetBuilder
from pipeline.build_parquet import ParquetBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("osm-changes")
logger.setLevel(logging.DEBUG)

POLL_INTERVAL = 60
# Max sequences to process per loop tick before yielding for parquet builds.
# Prevents unbounded memory growth during catch-up.
MAX_SEQS_PER_TICK = 100


def write_and_upload_metadata(
    parquet_dir: Path, uploader: R2Uploader | None, r2_public_url: str, r2_prefix: str,
):
    """Write metadata.json with available date range and upload it."""
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
        uploader.upload_file(metadata_path, f"{r2_prefix}/metadata.json")
    logger.info("Wrote %s metadata: %s to %s (%d dates)", r2_prefix, date_dirs[0], date_dirs[-1], len(date_dirs))


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))

    watcher = Watcher(data_dir)
    parquet_builder = ParquetBuilder(data_dir / "deletions", data_dir / "parquet")

    cs_watcher = ChangesetWatcher(data_dir)
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

    last_seq = watcher.load_state()
    if last_seq is None:
        last_seq = watcher.get_latest_sequence()
        logger.info("No saved state, starting from latest sequence: %d", last_seq)
    else:
        logger.info("Resuming from saved sequence: %d", last_seq)

    cs_last_seq = cs_watcher.load_state()
    if cs_last_seq is None:
        cs_last_seq = cs_watcher.get_latest_sequence()
        logger.info("No saved changeset state, starting from latest sequence: %d", cs_last_seq)
    else:
        logger.info("Resuming changesets from saved sequence: %d", cs_last_seq)

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

        seqs_processed = 0
        while last_seq < latest_seq and seqs_processed < MAX_SEQS_PER_TICK:
            next_seq = last_seq + 1
            try:
                count = watcher.fetch_and_process(next_seq)
                if count is None:
                    break
                if count > 0:
                    logger.info("Seq %d: %d changes", next_seq, count)
                last_seq = next_seq
                watcher.save_state(last_seq)
                seqs_processed += 1
            except Exception:
                logger.exception("Failed to process seq %d", next_seq)
                break

        # Fetch changeset replication
        try:
            cs_latest_seq = cs_watcher.get_latest_sequence()
        except Exception:
            logger.exception("Failed to get latest changeset sequence")
            cs_latest_seq = cs_last_seq

        cs_seqs_processed = 0
        while cs_last_seq < cs_latest_seq and cs_seqs_processed < MAX_SEQS_PER_TICK:
            cs_next_seq = cs_last_seq + 1
            try:
                count = cs_watcher.fetch_and_process(cs_next_seq)
                if count is None:
                    break
                if count > 0:
                    logger.info("Changeset seq %d: %d changesets", cs_next_seq, count)
                cs_last_seq = cs_next_seq
                cs_watcher.save_state(cs_last_seq)
                cs_seqs_processed += 1
            except Exception:
                logger.exception("Failed to process changeset seq %d", cs_next_seq)
                break

        # If we're still catching up, skip the sleep and loop immediately
        still_catching_up = (seqs_processed >= MAX_SEQS_PER_TICK or
                             cs_seqs_processed >= MAX_SEQS_PER_TICK)
        if still_catching_up:
            next_poll = time.time()  # don't sleep, loop immediately

        if (now - last_parquet_build) >= parquet_build_interval:
            last_parquet_build = now
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                # Element changes parquet
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

                write_and_upload_metadata(data_dir / "parquet", uploader, r2_public_url, "osm-changes")
                prune_old_files(data_dir / "parquet", retention_days)
                prune_old_files(data_dir / "deletions", retention_days)

                # Changeset parquet
                cs_built = cs_parquet_builder.build(today_str)
                if cs_built:
                    logger.info("Built changeset parquet for %s", today_str)
                    if uploader:
                        pf = data_dir / "changeset_parquet" / f"date={today_str}" / "data.parquet"
                        uploader.upload_file(pf, f"osm-changesets/date={today_str}/data.parquet")

                for jsonl in sorted((data_dir / "changesets").glob("*.jsonl")):
                    date_str = jsonl.stem
                    if date_str == today_str:
                        continue
                    if cs_parquet_builder.build(date_str):
                        logger.info("Built changeset parquet for %s", date_str)
                        if uploader:
                            pf = data_dir / "changeset_parquet" / f"date={date_str}" / "data.parquet"
                            uploader.upload_file(pf, f"osm-changesets/date={date_str}/data.parquet")
                        jsonl.unlink()
                        logger.info("Cleaned up %s", jsonl.name)

                write_and_upload_metadata(data_dir / "changeset_parquet", uploader, r2_public_url, "osm-changesets")
                prune_old_files(data_dir / "changeset_parquet", retention_days)
                prune_old_files(data_dir / "changesets", retention_days)
            except Exception:
                logger.exception("Failed to build/upload parquet")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
