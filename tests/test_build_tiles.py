import json
import subprocess
from pathlib import Path
from unittest.mock import patch

from pipeline.build_tiles import TileBuilder


def _write_geojsonl(path: Path, features: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for feat in features:
            f.write(json.dumps(feat) + "\n")


def _make_feature(osm_id=1):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"osm_type": "node", "osm_id": osm_id},
    }


def test_build_daily_tiles(tmp_path):
    deletions_dir = tmp_path / "deletions"
    tiles_dir = tmp_path / "tiles"
    _write_geojsonl(deletions_dir / "2025-01-14.geojsonl", [_make_feature(1)])

    builder = TileBuilder(deletions_dir, tiles_dir)

    with patch("pipeline.build_tiles.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        built = builder.build_daily_tiles()

    assert len(built) == 1
    assert "2025-01-14" in built
    assert mock_run.called
    cmd = mock_run.call_args[0][0]
    assert "tippecanoe" in cmd[0]


def test_skip_already_built(tmp_path):
    deletions_dir = tmp_path / "deletions"
    tiles_dir = tmp_path / "tiles"
    tiles_dir.mkdir(parents=True)
    _write_geojsonl(deletions_dir / "2025-01-14.geojsonl", [_make_feature(1)])
    pmtiles_file = tiles_dir / "2025-01-14.pmtiles"
    pmtiles_file.write_bytes(b"fake")
    import os, time
    future_time = time.time() + 10
    os.utime(pmtiles_file, (future_time, future_time))

    builder = TileBuilder(deletions_dir, tiles_dir)

    with patch("pipeline.build_tiles.subprocess.run") as mock_run:
        built = builder.build_daily_tiles()

    assert len(built) == 0
    assert not mock_run.called


