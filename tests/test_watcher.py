import gzip
import json
from unittest.mock import MagicMock

import pytest
import requests

from daemon.replication_planner import Planner, Position
from daemon.watcher import Watcher
from tests.test_replication_planner import BASE, FIRST, FakeSource

JAN1 = BASE // 86400 * 86400            # midnight label at the start of the synthetic timeline
FEATURES = [
    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 2]},
     "properties": {"timestamp": "2026-01-01T23:59:00Z", "osm_id": 1}},
    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 4]},
     "properties": {"timestamp": "2026-01-02T00:00:10Z", "osm_id": 2}},
]


class FakeAugmenter:
    def __init__(self, fail_after=None):
        self.fail_after = fail_after

    def process(self, osc_fileobj):
        osc_fileobj.read()  # the downloaded file must be readable gzip
        for i, feature in enumerate(FEATURES):
            if self.fail_after is not None and i >= self.fail_after:
                raise RuntimeError("API went away")
            yield feature


def make_download(fail_404_for=(), calls=None):
    def download(step, spool):
        if calls is not None:
            calls.append((step.interval, step.seq))
        if step.interval in fail_404_for:
            raise requests.HTTPError(response=MagicMock(status_code=404))
        path = f"{spool}/{step.interval}-{step.seq}.osc.gz"
        with gzip.open(path, "wb") as f:
            f.write(b"<osmChange/>")
        return path
    return download


def make_watcher(tmp_path, n_minutes=60 * 30, augmenter=None, download=None, **kwargs):
    source = FakeSource(n_minutes=n_minutes)
    return Watcher(
        tmp_path,
        planner_factory=lambda: Planner(source),
        download=download or make_download(),
        augmenter_factory=lambda api, work_dir: augmenter or FakeAugmenter(),
        **kwargs,
    )


def lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_catch_up_files_features_by_their_own_date_and_saves_position(tmp_path):
    watcher = make_watcher(tmp_path)
    watcher.save_position(Position(label=JAN1), epoch=JAN1)

    done = watcher.catch_up(max_steps=3)

    assert [s.interval for s in done] == ["day", "hour", "hour"]
    day1 = lines(tmp_path / "deletions" / "2026-01-01.geojsonl")
    day2 = lines(tmp_path / "deletions" / "2026-01-02.geojsonl")
    assert len(day1) == 3 and len(day2) == 3          # 3 steps x one feature on each side of midnight
    saved = json.loads((tmp_path / "state" / "osc_position.json").read_text())
    assert saved["label"] == done[-1].end.label and saved["epoch"] == done[-1].end.label


def test_a_failed_step_leaves_no_partial_output_and_keeps_the_position(tmp_path):
    watcher = make_watcher(tmp_path, augmenter=FakeAugmenter(fail_after=1))
    watcher.save_position(Position(label=JAN1), epoch=JAN1)

    with pytest.raises(RuntimeError):
        watcher.catch_up(max_steps=1)

    assert list((tmp_path / "deletions").glob("*.geojsonl")) == []
    assert json.loads((tmp_path / "state" / "osc_position.json").read_text())["label"] == JAN1
    assert list((tmp_path / "work").iterdir()) == []   # temp files cleaned up


def test_missing_day_diff_falls_back_to_hours(tmp_path):
    calls = []
    watcher = make_watcher(tmp_path, download=make_download(fail_404_for=("day",), calls=calls))
    watcher.save_position(Position(label=JAN1), epoch=JAN1)

    done = watcher.catch_up(max_steps=2)

    assert [s.interval for s in done] == ["hour", "hour"]
    assert calls[0][0] == "day"                               # it did try the day first


def test_caught_up_does_nothing(tmp_path):
    watcher = make_watcher(tmp_path, n_minutes=500)
    last = FIRST + 499
    watcher.save_position(Position(minute=last), epoch=FakeSource(500).ts(last))
    assert watcher.catch_up() == []
    assert list((tmp_path / "deletions").glob("*.geojsonl")) == []


def test_migrates_saved_minute_sequence_from_older_versions(tmp_path):
    watcher = make_watcher(tmp_path)
    (tmp_path / "state" / "last_seq.txt").write_text(str(FIRST + 100))
    assert watcher.load_position(watcher.planner_factory()) == Position(minute=FIRST + 100)


def test_skips_ahead_when_further_behind_than_max_days(tmp_path):
    watcher = make_watcher(tmp_path, n_minutes=60 * 24 * 10, max_days=2)
    watcher.save_position(Position(minute=FIRST + 10), epoch=FakeSource(1).ts(FIRST + 10))
    planner = watcher.planner_factory()
    position = watcher.load_position(planner)
    assert position.minute > FIRST + 60 * 24 * 7        # jumped to ~2 days before "now"
    assert planner.epoch(position) >= planner.latest["minute"][1] - 2 * 86400 - 120


def test_complete_before_waits_an_hour_past_midnight(tmp_path):
    watcher = make_watcher(tmp_path)
    assert watcher.complete_before() == "0000-00-00"      # nothing processed yet
    watcher.save_position(Position(label=JAN1), epoch=JAN1 + 86400 + 1800)   # Jan 2 00:30
    assert watcher.complete_before() == "2026-01-01"      # Jan 1 may still get edits
    watcher.save_position(Position(label=JAN1), epoch=JAN1 + 86400 + 3601)   # Jan 2 01:00:01
    assert watcher.complete_before() == "2026-01-02"      # Jan 1 is complete
