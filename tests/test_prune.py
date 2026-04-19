from datetime import date
from pathlib import Path

from pipeline.prune import prune_old_files


def test_prune_removes_old_geojsonl(tmp_path):
    old_file = tmp_path / "2025-01-01.geojsonl"
    new_file = tmp_path / "2025-01-20.geojsonl"
    old_file.touch()
    new_file.touch()

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 21))
    assert old_file in pruned
    assert not old_file.exists()
    assert new_file.exists()


def test_prune_removes_old_parquet_dirs(tmp_path):
    old_dir = tmp_path / "date=2025-01-01"
    old_dir.mkdir()
    (old_dir / "data.parquet").touch()

    new_dir = tmp_path / "date=2025-01-20"
    new_dir.mkdir()
    (new_dir / "data.parquet").touch()

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 21))
    assert old_dir in pruned
    assert not old_dir.exists()
    assert new_dir.exists()


def test_prune_keeps_non_date_files(tmp_path):
    metadata = tmp_path / "metadata.json"
    metadata.touch()

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 21))
    assert pruned == []
    assert metadata.exists()
