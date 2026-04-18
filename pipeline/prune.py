"""Prune old daily GeoJSON and PMTiles files past retention."""

import logging
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def prune_old_files(directory: Path, retention_days: int, today: date | None = None) -> list[Path]:
    """Delete files older than retention_days. Returns list of deleted paths."""
    if today is None:
        today = date.today()

    cutoff = today - timedelta(days=retention_days)
    pruned = []

    for f in sorted(directory.iterdir()):
        if f.suffix not in (".geojsonl", ".pmtiles"):
            continue
        try:
            file_date = date.fromisoformat(f.stem)
        except ValueError:
            continue
        if file_date < cutoff:
            logger.info("Pruning old file: %s", f.name)
            f.unlink()
            pruned.append(f)

    return pruned
