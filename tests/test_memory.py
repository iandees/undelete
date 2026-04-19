"""Tests that verify memory usage stays bounded during processing.

These tests generate large-ish datasets and check that peak memory stays
within expected limits, catching regressions like accumulating all features
in a list or loading an entire day's file into memory at once.
"""

import json
import os
import random
import tracemalloc

import pytest

from daemon.adiff_parser import parse_adiff
from pipeline.build_parquet import ParquetBuilder, ROW_GROUP_SIZE
from pipeline.build_changeset_parquet import ChangesetParquetBuilder


def _make_adiff_xml(n_actions):
    """Generate an adiff XML with n_actions create actions for nodes."""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<augmentedDiff>"]
    for i in range(n_actions):
        lon = random.uniform(-180, 180)
        lat = random.uniform(-90, 90)
        lines.append(f'<action type="create"><new>')
        lines.append(
            f'<node id="{i}" version="1" changeset="1" '
            f'user="test" uid="1" timestamp="2025-01-01T00:00:00Z" '
            f'lon="{lon}" lat="{lat}">'
        )
        lines.append(f'<tag k="name" v="node {i}"/>')
        lines.append("</node></new></action>")
    lines.append("</augmentedDiff>")
    return "\n".join(lines).encode()


def _make_geojsonl_features(n_features):
    """Generate n_features GeoJSON feature dicts."""
    features = []
    for i in range(n_features):
        lon = random.uniform(-180, 180)
        lat = random.uniform(-90, 90)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "action": "create",
                "osm_type": "node",
                "osm_id": i,
                "version": 1,
                "changeset": 1,
                "user": "test",
                "uid": 1,
                "timestamp": "2025-01-01T00:00:00Z",
                "tags": {"name": f"node {i}"},
                "old_tags": None,
            },
        })
    return features


def _make_changeset_jsonl_entries(n_changesets):
    """Generate n_changesets changeset dicts."""
    entries = []
    for i in range(n_changesets):
        lon = random.uniform(-180, 180)
        lat = random.uniform(-90, 90)
        entries.append({
            "id": i,
            "created_at": "2025-01-01T00:00:00Z",
            "closed_at": "2025-01-01T00:01:00Z",
            "open": False,
            "num_changes": 10,
            "user": "test",
            "uid": 1,
            "comments_count": 0,
            "tags": {"comment": f"changeset {i}"},
            "min_lon": lon,
            "min_lat": lat,
            "max_lon": lon + 0.01,
            "max_lat": lat + 0.01,
        })
    return entries


class TestAdiffParserMemory:
    """parse_adiff should yield features without accumulating them all."""

    def test_generator_does_not_accumulate(self):
        n = 5_000
        xml_bytes = _make_adiff_xml(n)

        tracemalloc.start()
        tracemalloc.reset_peak()

        count = 0
        for feature in parse_adiff(xml_bytes):
            count += 1
            # Don't hold references — just count
        assert count == n

        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # The XML itself is ~300 bytes per action, so 5000 actions ≈ 1.5MB input.
        # If features accumulate, each is ~500 bytes → 2.5MB extra.
        # With streaming, peak should stay well under the "all accumulated" case.
        # Allow 4MB total (XML parsing buffers + thread overhead), but not the
        # ~6MB+ we'd see if all 5000 features were held in memory simultaneously.
        assert peak < 4 * 1024 * 1024, (
            f"Peak memory {peak / 1024 / 1024:.1f}MB suggests features are "
            f"being accumulated instead of yielded"
        )

    def test_yields_correct_features(self):
        """Sanity check that the generator yields the right data."""
        n = 100
        xml_bytes = _make_adiff_xml(n)
        features = list(parse_adiff(xml_bytes))
        assert len(features) == n
        assert all(f["properties"]["action"] == "create" for f in features)
        ids = {f["properties"]["osm_id"] for f in features}
        assert ids == set(range(n))


class TestParquetBuilderMemory:
    """ParquetBuilder.build should process in chunks, not load everything."""

    def test_chunked_build_memory(self, tmp_path):
        n = ROW_GROUP_SIZE + 1_000  # just over one chunk boundary
        features = _make_geojsonl_features(n)

        geojsonl_dir = tmp_path / "geojsonl"
        geojsonl_dir.mkdir()
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        geojsonl_file = geojsonl_dir / "2025-01-01.geojsonl"
        with open(geojsonl_file, "w") as f:
            for feat in features:
                f.write(json.dumps(feat, separators=(",", ":")) + "\n")
        del features  # free the input list

        builder = ParquetBuilder(geojsonl_dir, parquet_dir)

        tracemalloc.start()
        tracemalloc.reset_peak()

        result = builder.build("2025-01-01")
        assert result is True

        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # With chunked processing (ROW_GROUP_SIZE=50k), peak memory should
        # reflect only one chunk. Without chunking, all 51k features would
        # be in memory at once. We allow generous headroom for pyarrow/shapely
        # overhead but check it's not absurdly high.
        #
        # One chunk of 50k features at ~2KB each ≈ 100MB.
        # Full file at 51k features ≈ 102MB — the difference is small for this
        # test size, so we verify the build produced multiple row groups instead.
        import pyarrow.parquet as pq
        parquet_path = parquet_dir / "date=2025-01-01" / "data.parquet"
        pf = pq.ParquetFile(parquet_path)
        assert pf.metadata.num_row_groups == 2, (
            f"Expected 2 row groups for {n} features with "
            f"ROW_GROUP_SIZE={ROW_GROUP_SIZE}, got {pf.metadata.num_row_groups}"
        )
        assert pf.metadata.num_rows == n

    def test_single_chunk_still_works(self, tmp_path):
        """A file smaller than ROW_GROUP_SIZE should produce one row group."""
        n = 100
        features = _make_geojsonl_features(n)

        geojsonl_dir = tmp_path / "geojsonl"
        geojsonl_dir.mkdir()
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        geojsonl_file = geojsonl_dir / "2025-01-01.geojsonl"
        with open(geojsonl_file, "w") as f:
            for feat in features:
                f.write(json.dumps(feat, separators=(",", ":")) + "\n")

        builder = ParquetBuilder(geojsonl_dir, parquet_dir)
        assert builder.build("2025-01-01") is True

        import pyarrow.parquet as pq
        pf = pq.ParquetFile(parquet_dir / "date=2025-01-01" / "data.parquet")
        assert pf.metadata.num_row_groups == 1
        assert pf.metadata.num_rows == n


class TestChangesetParquetBuilderMemory:
    """ChangesetParquetBuilder.build should process in chunks."""

    def test_chunked_changeset_build(self, tmp_path):
        n = ROW_GROUP_SIZE + 1_000
        entries = _make_changeset_jsonl_entries(n)

        jsonl_dir = tmp_path / "changesets"
        jsonl_dir.mkdir()
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        jsonl_file = jsonl_dir / "2025-01-01.jsonl"
        with open(jsonl_file, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry, separators=(",", ":")) + "\n")
        del entries

        builder = ChangesetParquetBuilder(jsonl_dir, parquet_dir)

        result = builder.build("2025-01-01")
        assert result is True

        import pyarrow.parquet as pq
        parquet_path = parquet_dir / "date=2025-01-01" / "data.parquet"
        pf = pq.ParquetFile(parquet_path)
        assert pf.metadata.num_row_groups == 2
        assert pf.metadata.num_rows == n

    def test_deduplication_still_works(self, tmp_path):
        """Duplicate changeset IDs should be deduplicated (keep latest)."""
        jsonl_dir = tmp_path / "changesets"
        jsonl_dir.mkdir()
        parquet_dir = tmp_path / "parquet"
        parquet_dir.mkdir()

        entries = [
            {
                "id": 1, "created_at": "2025-01-01T00:00:00Z",
                "closed_at": "2025-01-01T00:01:00Z", "open": False,
                "num_changes": 5, "user": "alice", "uid": 1,
                "comments_count": 0, "tags": {"comment": "first"},
                "min_lon": -77.0, "min_lat": 38.0,
                "max_lon": -76.0, "max_lat": 39.0,
            },
            {
                "id": 1, "created_at": "2025-01-01T00:00:00Z",
                "closed_at": "2025-01-01T00:02:00Z", "open": False,
                "num_changes": 10, "user": "alice", "uid": 1,
                "comments_count": 1, "tags": {"comment": "updated"},
                "min_lon": -77.0, "min_lat": 38.0,
                "max_lon": -76.0, "max_lat": 39.0,
            },
        ]
        jsonl_file = jsonl_dir / "2025-01-01.jsonl"
        with open(jsonl_file, "w") as f:
            for e in entries:
                f.write(json.dumps(e, separators=(",", ":")) + "\n")

        builder = ChangesetParquetBuilder(jsonl_dir, parquet_dir)
        builder.build("2025-01-01")

        import pyarrow.parquet as pq
        table = pq.read_table(parquet_dir / "date=2025-01-01" / "data.parquet")
        assert table.num_rows == 1
        assert table.column("num_changes").to_pylist() == [10]
