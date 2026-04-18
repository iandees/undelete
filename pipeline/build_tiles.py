"""Build PMTiles from daily GeoJSON files using tippecanoe and tile-join."""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class TileBuilder:
    def __init__(self, deletions_dir: Path, tiles_dir: Path):
        self.deletions_dir = Path(deletions_dir)
        self.tiles_dir = Path(tiles_dir)
        self.tiles_dir.mkdir(parents=True, exist_ok=True)

    def build_daily_tiles(self) -> list[str]:
        """Build PMTiles for any daily GeoJSON files that are new or updated.
        Returns list of date strings that were built."""
        built = []
        for geojsonl_file in sorted(self.deletions_dir.glob("*.geojsonl")):
            date_str = geojsonl_file.stem
            pmtiles_file = self.tiles_dir / f"{date_str}.pmtiles"
            if pmtiles_file.exists():
                if pmtiles_file.stat().st_mtime > geojsonl_file.stat().st_mtime:
                    continue
            self._run_tippecanoe(geojsonl_file, pmtiles_file)
            built.append(date_str)
        return built

    def merge_tiles(self):
        """Merge all daily PMTiles into a single merged.pmtiles file."""
        daily_files = sorted(self.tiles_dir.glob("????-??-??.pmtiles"))
        if not daily_files:
            logger.info("No daily PMTiles files to merge")
            return
        merged_file = self.tiles_dir / "merged.pmtiles"
        cmd = [
            "tile-join", "--force", "--no-tile-size-limit",
            "-o", str(merged_file),
        ] + [str(f) for f in daily_files]
        logger.info("Merging %d daily files into %s", len(daily_files), merged_file)
        subprocess.run(cmd, check=True)

    def _run_tippecanoe(self, input_file: Path, output_file: Path):
        """Run tippecanoe on a single GeoJSON file."""
        cmd = [
            "tippecanoe", "--force", "--no-tile-size-limit",
            "-o", str(output_file), "-l", "deletions",
            "--drop-densest-as-needed", "--extend-zooms-if-still-dropping",
            str(input_file),
        ]
        logger.info("Building tiles: %s -> %s", input_file.name, output_file.name)
        subprocess.run(cmd, check=True)
