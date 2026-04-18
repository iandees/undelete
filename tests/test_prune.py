from datetime import date
from pathlib import Path

from pipeline.prune import prune_old_files


def test_prune_old_files(tmp_path):
    for d in ["2025-01-01", "2025-01-10", "2025-01-14"]:
        (tmp_path / f"{d}.geojsonl").write_text("data")
        (tmp_path / f"{d}.pmtiles").write_bytes(b"data")

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 14))

    assert len(pruned) == 2  # both .geojsonl and .pmtiles
    assert not (tmp_path / "2025-01-01.geojsonl").exists()
    assert not (tmp_path / "2025-01-01.pmtiles").exists()
    assert (tmp_path / "2025-01-10.geojsonl").exists()
    assert (tmp_path / "2025-01-14.geojsonl").exists()


def test_prune_nothing_to_prune(tmp_path):
    (tmp_path / "2025-01-14.geojsonl").write_text("data")
    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 14))
    assert len(pruned) == 0
