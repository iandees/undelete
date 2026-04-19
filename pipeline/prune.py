"""Prune old daily GeoJSON and Parquet files past retention."""

import logging
import shutil
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def prune_old_files(directory: Path, retention_days: int, today: date | None = None) -> list[Path]:
    """Delete files/dirs older than retention_days. Returns list of deleted paths."""
    if today is None:
        today = date.today()

    cutoff = today - timedelta(days=retention_days)
    pruned = []

    for f in sorted(directory.iterdir()):
        # Handle .geojsonl files (date is the stem)
        if f.is_file() and f.suffix in (".geojsonl", ".pmtiles"):
            try:
                file_date = date.fromisoformat(f.stem)
            except ValueError:
                continue
            if file_date < cutoff:
                logger.info("Pruning old file: %s", f.name)
                f.unlink()
                pruned.append(f)

        # Handle Hive-partitioned dirs like date=2025-01-14
        elif f.is_dir() and f.name.startswith("date="):
            try:
                dir_date = date.fromisoformat(f.name.split("=", 1)[1])
            except ValueError:
                continue
            if dir_date < cutoff:
                logger.info("Pruning old partition: %s", f.name)
                shutil.rmtree(f)
                pruned.append(f)

    return pruned
