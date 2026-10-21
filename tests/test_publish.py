import io
import json
import logging
import os
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from pipeline.build_changeset_parquet import ChangesetParquetBuilder
from pipeline.build_parquet import ParquetBuilder
from pipeline.publish import DailyPublisher
from tests.test_build_parquet import _make_feature

DATE = "2025-01-01"
LIVE = "2025-01-02"          # a cutoff of this makes DATE settled; cutoff DATE keeps DATE live
PREFIX = "osm-changes"


class FakeR2:
    """An in-memory bucket that can be made to fail."""

    def __init__(self):
        self.objects = {}
        self.fail_uploads = 0
        self.unreachable = False
        self.uploads = []

    def upload_file(self, local_path, key):
        if self.fail_uploads:
            self.fail_uploads -= 1
            raise ConnectionError("upload failed")
        self.objects[key] = Path(local_path).read_bytes()
        self.uploads.append(key)

    def download_if_exists(self, key, local_path):
        if self.unreachable:
            raise ConnectionError("R2 unreachable")
        if key not in self.objects:
            return False
        Path(local_path).write_bytes(self.objects[key])
        return True


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def keys_in(data: bytes):
    t = pq.read_table(io.BytesIO(data)).to_pydict()
    return sorted(zip(t["osm_type"], t["osm_id"]))


def write_raw(path, features):
    path.write_text("".join(json.dumps(f) + "\n" for f in features))


def node(i):
    return _make_feature("create", "node", i, float(i), float(i))


@pytest.fixture
def env(tmp_path):
    raw, parquet = tmp_path / "raw", tmp_path / "parquet"
    raw.mkdir()
    r2, clock = FakeR2(), Clock()

    def make(uploader=r2, **kw):
        return DailyPublisher("elements", raw, ".geojsonl", parquet, ParquetBuilder(raw, parquet),
                              PREFIX, uploader=uploader, work_dir=tmp_path / "work", clock=clock, **kw)
    return make, raw / f"{DATE}.geojsonl", parquet, r2, clock


KEY = f"{PREFIX}/date={DATE}/data.parquet"


def test_a_live_day_is_rebuilt_and_uploaded_as_it_grows_and_its_raw_file_kept(env):
    make, raw, parquet, r2, _ = env
    pub = make()

    write_raw(raw, [node(1)])
    pub.run(cutoff=DATE)                                  # DATE is still live
    assert keys_in(r2.objects[KEY]) == [("node", 1)]

    write_raw(raw, [node(1), node(2)])
    os.utime(raw, (raw.stat().st_atime, raw.stat().st_mtime + 5))
    pub.run(cutoff=DATE)
    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2)]
    assert raw.exists()


def test_a_settled_day_is_published_and_its_raw_file_deleted(env):
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(1), node(2)])

    pub.run(cutoff=LIVE)

    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2)]
    assert not raw.exists()


def test_late_rows_for_a_published_day_are_merged_not_substituted(env):
    """The failure this exists to prevent: a late row must not replace the day on R2."""
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(n) for n in range(1, 6)])
    pub.run(cutoff=LIVE)
    assert not raw.exists() and len(keys_in(r2.objects[KEY])) == 5

    write_raw(raw, [node(99)])                            # a row arrives after the day was finalized
    pub.run(cutoff=LIVE)

    assert keys_in(r2.objects[KEY]) == [("node", n) for n in (1, 2, 3, 4, 5, 99)]
    assert not raw.exists()                               # merged, so the delta is gone


def test_merging_uses_r2_even_when_the_local_copy_was_pruned(env):
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(1), node(2)])
    pub.run(cutoff=LIVE)
    (parquet / f"date={DATE}" / "data.parquet").unlink()   # retention removed the local file

    write_raw(raw, [node(3)])
    pub.run(cutoff=LIVE)

    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2), ("node", 3)]


def test_unreachable_r2_never_leads_to_a_substitute_and_loses_nothing(env, caplog):
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(1), node(2)])
    pub.run(cutoff=LIVE)
    good = r2.objects[KEY]

    write_raw(raw, [node(3)])
    r2.unreachable = True
    with caplog.at_level(logging.ERROR):
        pub.run(cutoff=LIVE)

    assert r2.objects[KEY] == good                        # R2 untouched
    assert raw.exists()                                   # the late row is kept for next time
    assert "Failed to publish" in caplog.text

    r2.unreachable = False
    pub.run(cutoff=LIVE)
    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2), ("node", 3)]
    assert not raw.exists()


def test_a_failed_upload_is_retried_and_the_raw_file_kept_until_it_works(env):
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(1)])
    r2.fail_uploads = 1

    pub.run(cutoff=LIVE)
    assert KEY not in r2.objects and raw.exists()         # the old code would never retry this

    pub.run(cutoff=LIVE)
    assert keys_in(r2.objects[KEY]) == [("node", 1)]
    assert not raw.exists()


def test_late_rows_are_coalesced_to_one_merge_per_interval(env):
    make, raw, parquet, r2, clock = env
    pub = make(merge_interval=3600)
    write_raw(raw, [node(1)])
    pub.run(cutoff=LIVE)

    write_raw(raw, [node(2)])
    pub.run(cutoff=LIVE)                                  # first late merge happens right away
    assert len(keys_in(r2.objects[KEY])) == 2

    write_raw(raw, [node(3)])
    clock.now += 600
    pub.run(cutoff=LIVE)                                  # too soon: held back, raw kept
    assert len(keys_in(r2.objects[KEY])) == 2 and raw.exists()

    write_raw(raw, [node(3), node(4)])
    clock.now += 3600
    pub.run(cutoff=LIVE)
    assert [k[1] for k in keys_in(r2.objects[KEY])] == [1, 2, 3, 4]
    assert not raw.exists()


def test_without_r2_late_rows_merge_into_the_local_parquet(env):
    make, raw, parquet, _, _ = env
    pub = make(uploader=None)
    write_raw(raw, [node(1), node(2)])
    pub.run(cutoff=LIVE)

    write_raw(raw, [node(3)])
    pub.run(cutoff=LIVE)

    local = parquet / f"date={DATE}" / "data.parquet"
    assert keys_in(local.read_bytes()) == [("node", 1), ("node", 2), ("node", 3)]


def test_a_day_that_was_never_published_is_built_fresh_when_caught_up_on(env):
    # catch-up: a settled day appears with no published parquet anywhere to merge into
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(1), node(2), node(3)])

    pub.run(cutoff=LIVE)

    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2), ("node", 3)]
    assert not raw.exists()


def test_after_a_restart_a_settled_raw_file_is_merged_with_what_r2_already_has(env):
    """A day that was partly published before a crash, completed from the raw file afterwards."""
    make, raw, parquet, r2, _ = env
    before_crash = make()
    write_raw(raw, [node(1), node(2)])
    before_crash.run(cutoff=DATE)                         # live: half the day published
    assert len(keys_in(r2.objects[KEY])) == 2

    write_raw(raw, [node(3)])                             # the raw file was lost; only new rows exist
    restarted = make()
    restarted.run(cutoff=LIVE)

    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2), ("node", 3)]


def test_an_empty_settled_raw_file_is_cleaned_up(env):
    make, raw, parquet, r2, _ = env
    raw.write_text("")
    make().run(cutoff=LIVE)
    assert not raw.exists() and KEY not in r2.objects


def test_without_r2_a_raw_file_older_than_the_retention_is_dropped_not_built(env):
    make, raw, parquet, r2, _ = env
    write_raw(raw, [node(1)])                             # DATE is 2025-01-01

    make(uploader=None).run(cutoff=LIVE, keep_from="2025-01-02")

    assert not raw.exists()
    assert not (parquet / f"date={DATE}").exists()        # no throwaway parquet was built


def test_with_r2_old_dates_are_still_merged_whatever_the_retention(env):
    make, raw, parquet, r2, _ = env
    pub = make()
    write_raw(raw, [node(1), node(2)])
    pub.run(cutoff=LIVE, keep_from="2025-01-02")          # published to R2 even though it is "old"
    write_raw(raw, [node(3)])
    pub.run(cutoff=LIVE, keep_from="2025-01-02")

    assert keys_in(r2.objects[KEY]) == [("node", 1), ("node", 2), ("node", 3)]


def test_one_failing_date_does_not_block_the_others(env, caplog):
    make, raw, parquet, r2, _ = env
    other = raw.parent / "2025-01-00.geojsonl"
    write_raw(raw, [node(1)])
    other.write_text("{broken\n")

    with caplog.at_level(logging.ERROR):
        make().run(cutoff=LIVE)

    assert keys_in(r2.objects[KEY]) == [("node", 1)]      # the good date still published
    assert other.exists()                                 # the bad one is kept for a retry


def test_changesets_late_update_replaces_by_id_on_r2(tmp_path):
    raw, parquet = tmp_path / "raw", tmp_path / "parquet"
    raw.mkdir()
    r2, clock = FakeR2(), Clock()
    pub = DailyPublisher("changesets", raw, ".jsonl", parquet, ChangesetParquetBuilder(raw, parquet),
                         "osm-changesets", uploader=r2, work_dir=tmp_path / "work", clock=clock)

    def cs(i, **extra):
        return {"id": i, "created_at": f"{DATE}T10:00:00Z", "closed_at": "", "open": False,
                "num_changes": 1, "user": "u", "uid": 1, "comments_count": 0, "tags": {},
                "min_lat": 1.0, "max_lat": 2.0, "min_lon": 3.0, "max_lon": 4.0, **extra}

    raw_file = raw / f"{DATE}.jsonl"
    write_raw(raw_file, [cs(1), cs(2), cs(3)])
    pub.run(cutoff=LIVE)
    key = f"osm-changesets/date={DATE}/data.parquet"
    assert len(pq.read_table(io.BytesIO(r2.objects[key]))) == 3

    write_raw(raw_file, [cs(2, comments_count=9)])        # months later, someone comments on #2
    pub.run(cutoff=LIVE)

    rows = {r["id"]: r for r in pq.read_table(io.BytesIO(r2.objects[key])).to_pylist()}
    assert sorted(rows) == [1, 2, 3]
    assert rows[2]["comments_count"] == 9 and rows[1]["comments_count"] == 0
