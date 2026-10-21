import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.build_changeset_parquet import ChangesetParquetBuilder
from pipeline.build_parquet import ParquetBuilder
from tests.test_build_parquet import _make_feature

DATE = "2025-01-01"


def write_raw(path: Path, features):
    path.write_text("".join(json.dumps(f) + "\n" for f in features))


def geometry_types(parquet_path):
    """The geometry types declared in the file footer, which is where readers look."""
    geo = json.loads(pq.read_metadata(parquet_path).metadata[b"geo"])
    return geo["columns"]["geometry"]["geometry_types"]


def element_keys(parquet_path):
    t = pq.read_table(parquet_path).to_pydict()
    return sorted(zip(t["osm_type"], t["osm_id"], t["version"], t["action"]))


@pytest.fixture
def elements(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    raw.mkdir()
    builder = ParquetBuilder(raw, out)
    base = out / f"date={DATE}" / "data.parquet"
    return builder, raw / f"{DATE}.geojsonl", base


def publish_base(builder, raw_path, base, features):
    write_raw(raw_path, features)
    assert builder.build(DATE)
    raw_path.unlink()
    return base


def test_merge_adds_only_the_rows_the_parquet_does_not_have(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0),
                                      _make_feature("create", "node", 2, 1, 1)])

    # later: a delta holding one row the parquet already has and one new row
    write_raw(raw, [_make_feature("create", "node", 2, 1, 1), _make_feature("delete", "node", 3, 2, 2)])
    assert builder.merge(DATE, base) == 1

    assert element_keys(base) == [("node", 1, 1, "create"), ("node", 2, 1, "create"),
                                  ("node", 3, 1, "delete")]
    assert not base.with_suffix(".parquet.tmp").exists()


def test_a_fresh_build_from_the_delta_alone_would_have_lost_rows(elements):
    # documents the failure merge prevents: building from late rows replaces the published file
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", n, n, n) for n in range(1, 6)])

    write_raw(raw, [_make_feature("create", "node", 99, 0, 0)])
    builder.build(DATE)

    assert element_keys(base) == [("node", 99, 1, "create")]          # the 5 published rows are gone


def test_merge_with_nothing_new_leaves_the_file_alone(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])
    before = base.read_bytes()

    write_raw(raw, [_make_feature("create", "node", 1, 0, 0)])
    assert builder.merge(DATE, base) == 0

    assert base.read_bytes() == before
    assert builder.built_mtime(DATE) == raw.stat().st_mtime           # the raw file counts as handled


def test_merge_dedupes_repeats_within_the_delta(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])

    twice = _make_feature("create", "node", 2, 1, 1)
    write_raw(raw, [twice, twice, _make_feature("create", "node", 2, 1, 1)])
    assert builder.merge(DATE, base) == 1
    assert element_keys(base) == [("node", 1, 1, "create"), ("node", 2, 1, "create")]


def test_merge_treats_version_action_and_type_as_part_of_identity(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 5, 0, 0, version=1)])

    write_raw(raw, [
        _make_feature("modify", "node", 5, 0, 0, version=2),       # same element, new version
        _make_feature("delete", "node", 5, 0, 0, version=1),       # same version, different action
        _make_feature("create", "way", 5, 0, 0, version=1),        # same id, different type
    ])
    assert builder.merge(DATE, base) == 3
    assert len(element_keys(base)) == 4


def test_merge_handles_large_ids(elements):
    builder, raw, base = elements
    big = 12_345_678_901
    publish_base(builder, raw, base, [_make_feature("create", "node", big, 0, 0)])

    write_raw(raw, [_make_feature("create", "node", big, 0, 0), _make_feature("create", "node", big + 1, 0, 0)])
    assert builder.merge(DATE, base) == 1
    assert [k[1] for k in element_keys(base)] == [big, big + 1]


def test_merge_declares_the_union_of_geometry_types(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])

    line = _make_feature("create", "way", 2, 0, 0)
    line["geometry"] = {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}
    write_raw(raw, [line])
    builder.merge(DATE, base)

    assert geometry_types(base) == ["LineString", "Point"]


def test_successive_merges_remember_earlier_geometry_types(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])

    line = _make_feature("create", "way", 2, 0, 0)
    line["geometry"] = {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}
    write_raw(raw, [line])
    builder.merge(DATE, base)
    raw.unlink()

    # a second late delta containing only points must not forget the LineString from the first
    write_raw(raw, [_make_feature("create", "node", 3, 2, 2)])
    builder.merge(DATE, base)

    assert geometry_types(base) == ["LineString", "Point"]
    assert len(element_keys(base)) == 3


def test_merge_can_use_a_base_downloaded_elsewhere(elements, tmp_path):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])
    downloaded = tmp_path / "from-r2.parquet"
    downloaded.write_bytes(base.read_bytes())
    base.unlink()                                                  # local copy was pruned

    write_raw(raw, [_make_feature("create", "node", 2, 1, 1)])
    assert builder.merge(DATE, downloaded) == 1
    assert element_keys(base) == [("node", 1, 1, "create"), ("node", 2, 1, "create")]


def test_a_failed_merge_leaves_the_published_file_untouched(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])
    before = base.read_bytes()

    raw.write_text(json.dumps(_make_feature("create", "node", 2, 1, 1)) + "\n{not json\n")
    with pytest.raises(json.JSONDecodeError):
        builder.merge(DATE, base)

    assert base.read_bytes() == before
    assert not base.with_suffix(".parquet.tmp").exists()


def test_merge_without_a_raw_file_returns_none(elements):
    builder, raw, base = elements
    publish_base(builder, raw, base, [_make_feature("create", "node", 1, 0, 0)])
    assert builder.merge(DATE, base) is None


# ---- changesets ----

def cs(id_, **extra):
    return {"id": id_, "created_at": f"{DATE}T10:00:00Z", "closed_at": "", "open": False,
            "num_changes": 1, "user": "u", "uid": 1, "comments_count": 0, "tags": {"comment": "hi"},
            "min_lat": 1.0, "max_lat": 2.0, "min_lon": 3.0, "max_lon": 4.0, **extra}


@pytest.fixture
def changesets(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "parquet"
    raw.mkdir()
    return ChangesetParquetBuilder(raw, out), raw / f"{DATE}.jsonl", out / f"date={DATE}" / "data.parquet"


def rows_by_id(path):
    t = pq.read_table(path).to_pylist()
    return {r["id"]: r for r in t}


def test_changeset_merge_replaces_by_id_and_keeps_the_rest(changesets):
    builder, raw, base = changesets
    write_raw(raw, [cs(1), cs(2), cs(3)])
    assert builder.build(DATE)
    raw.unlink()

    # a later comment on changeset 2, plus a changeset the parquet has never seen
    write_raw(raw, [cs(2, comments_count=5, tags={"comment": "hi", "reviewed": "yes"}), cs(4)])
    assert builder.merge(DATE, base) == 2

    rows = rows_by_id(base)
    assert sorted(rows) == [1, 2, 3, 4]                              # one row per changeset
    assert rows[2]["comments_count"] == 5 and dict(rows[2]["tags"])["reviewed"] == "yes"
    assert rows[1]["comments_count"] == 0 and rows[3]["comments_count"] == 0   # untouched


def test_changeset_merge_keeps_the_latest_version_within_the_delta(changesets):
    builder, raw, base = changesets
    write_raw(raw, [cs(1)])
    builder.build(DATE)
    raw.unlink()

    write_raw(raw, [cs(1, num_changes=2), cs(1, num_changes=7)])
    builder.merge(DATE, base)
    assert rows_by_id(base)[1]["num_changes"] == 7


def test_changeset_merge_is_atomic_and_declares_geometry(changesets):
    builder, raw, base = changesets
    write_raw(raw, [cs(1)])
    builder.build(DATE)
    raw.unlink()
    write_raw(raw, [cs(2, min_lon=None)])                            # no bbox -> a Point(0, 0)
    builder.merge(DATE, base)

    assert not base.with_suffix(".parquet.tmp").exists()
    assert geometry_types(base) == ["Point", "Polygon"]


def test_changeset_merge_without_a_raw_file_returns_none(changesets):
    builder, raw, base = changesets
    write_raw(raw, [cs(1)])
    builder.build(DATE)
    raw.unlink()
    assert builder.merge(DATE, base) is None
