import gzip
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from daemon.changeset_watcher import ChangesetWatcher

FIXTURE = Path(__file__).parent / "fixtures" / "sample_changesets.xml"


NOW = int(datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc).timestamp())


def epoch(*args):
    return int(datetime(*args, tzinfo=timezone.utc).timestamp())


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """catch_up asks the state file when a file was generated; pretend it was NOW."""
    monkeypatch.setattr(ChangesetWatcher, "seq_epoch", lambda self, seq: NOW)


def written_ids(tmp_path):
    ids = []
    for f in sorted((tmp_path / "changesets").glob("*.jsonl")):
        ids += [json.loads(line)["id"] for line in f.read_text().splitlines()]
    return ids


def patch_fetch(monkeypatch, watcher, fn):
    monkeypatch.setattr(watcher, "fetch", fn)


def test_fetch_parses_a_file_and_maps_404_to_none(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    ok = MagicMock(status_code=200, content=gzip.compress(FIXTURE.read_bytes()))
    missing = MagicMock(status_code=404)
    session = MagicMock()
    monkeypatch.setattr(watcher, "_session", lambda: session)

    session.get.return_value = ok
    changesets = watcher.fetch(1)
    assert [c["id"] for c in changesets][:2] == [12345, 12346]

    session.get.return_value = missing
    assert watcher.fetch(2) is None


def test_sessions_retry_transient_failures_and_are_per_thread(tmp_path):
    watcher = ChangesetWatcher(tmp_path)
    adapter = watcher._session().get_adapter("https://planet.openstreetmap.org/")
    assert adapter.max_retries.total >= 3
    assert 503 in adapter.max_retries.status_forcelist
    seen = {}
    threads = [threading.Thread(target=lambda i=i: seen.update({i: watcher._session()})) for i in range(3)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len({id(s) for s in seen.values()} | {id(watcher._session())}) == 4   # nobody shares a session


def test_commits_in_sequence_order_even_when_later_files_finish_first(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path, workers=8)

    def fetch(seq):
        time.sleep((110 - seq) * 0.01)      # later sequences finish sooner
        return [{"id": seq}]
    patch_fetch(monkeypatch, watcher, fetch)

    last, done = watcher.catch_up(100, 105)
    assert (last, done) == (105, 5)
    assert written_ids(tmp_path) == [101, 102, 103, 104, 105]
    assert watcher.load_state() == 105


def test_fetches_really_overlap(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path, workers=4)
    barrier = threading.Barrier(4, timeout=5)   # only releases if 4 fetches are in flight at once

    def fetch(seq):
        barrier.wait()
        return [{"id": seq}]
    patch_fetch(monkeypatch, watcher, fetch)

    assert watcher.catch_up(0, 4) == (4, 4)


def test_stops_at_the_first_unpublished_file(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: None if seq >= 103 else [{"id": seq}])

    assert watcher.catch_up(100, 110) == (102, 2)
    assert written_ids(tmp_path) == [101, 102]      # 104+ never written even if they were ready
    assert watcher.load_state() == 102


def test_error_mid_batch_keeps_only_the_contiguous_prefix(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)

    def fetch(seq):
        if seq == 103:
            raise RuntimeError("boom")
        return [{"id": seq}]
    patch_fetch(monkeypatch, watcher, fetch)

    assert watcher.catch_up(100, 110) == (102, 2)
    assert written_ids(tmp_path) == [101, 102]
    assert watcher.load_state() == 102

    # a retry picks up exactly where it stopped: nothing skipped, nothing duplicated
    patch_fetch(monkeypatch, watcher, lambda seq: [{"id": seq}])
    assert watcher.catch_up(102, 105) == (105, 3)
    assert written_ids(tmp_path) == [101, 102, 103, 104, 105]


def test_respects_max_seqs_and_latest(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: [{"id": seq}])

    assert watcher.catch_up(0, 1000, max_seqs=7) == (7, 7)
    assert watcher.catch_up(7, 9, max_seqs=100) == (9, 2)
    assert watcher.catch_up(9, 9) == (9, 0)         # already caught up
    assert watcher.catch_up(9, 5) == (9, 0)         # latest behind us: nothing to do


def test_empty_files_advance_the_position_without_writing(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: [])

    assert watcher.catch_up(0, 3) == (3, 3)
    assert written_ids(tmp_path) == []
    assert watcher.load_state() == 3


def cs(id_, created, **extra):
    return {"id": id_, "created_at": created, **extra}


def test_files_each_changeset_under_its_creation_date(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: [
        cs(seq * 10 + 1, "2026-01-10T01:00:00Z"),
        cs(seq * 10 + 2, "2026-01-09T23:59:00Z"),     # created yesterday, updated now
    ])

    assert watcher.catch_up(0, 2) == (2, 2)

    def ids(name):
        return [json.loads(l)["id"] for l in (tmp_path / "changesets" / name).read_text().splitlines()]
    assert ids("2026-01-10.jsonl") == [11, 21]
    assert ids("2026-01-09.jsonl") == [12, 22]


def test_every_version_of_a_changeset_lands_in_the_same_file(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    # an open changeset created before midnight keeps being updated after midnight
    patch_fetch(monkeypatch, watcher, lambda seq: [cs(7, "2026-01-09T23:50:00Z", num_changes=seq)])

    watcher.catch_up(0, 3)

    assert [p.name for p in (tmp_path / "changesets").iterdir()] == ["2026-01-09.jsonl"]
    assert [json.loads(l)["num_changes"] for l in (tmp_path / "changesets" / "2026-01-09.jsonl").open()] == [1, 2, 3]


def test_updates_to_changesets_from_frozen_dates_are_dropped(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: [
        cs(1, "2026-01-10T00:30:00Z"),                 # today
        cs(2, "2026-01-09T10:00:00Z"),                 # yesterday: an open changeset may still change
        cs(3, "2026-01-08T10:00:00Z"),                 # >24h past its date's end: frozen at 12:00 on the 10th
        cs(4, "2025-06-01T10:00:00Z", comments_count=1),   # months old: a new comment on an old changeset
    ])

    watcher.catch_up(0, 1)

    assert sorted(p.name for p in (tmp_path / "changesets").iterdir()) == [
        "2026-01-09.jsonl", "2026-01-10.jsonl"]
    assert watcher.load_state() == 1                   # dropping must not stall the position


def test_freeze_boundary(tmp_path):
    f = ChangesetWatcher.is_frozen
    # 2026-01-08 starts accepting until 2026-01-10 01:00 (two days + 1h margin)
    assert not f("2026-01-08", epoch(2026, 1, 10, 0, 59))
    assert f("2026-01-08", epoch(2026, 1, 10, 1, 0))
    assert not f("2026-01-10", epoch(2026, 1, 10, 12, 0))


def test_malformed_creation_date_falls_back_to_the_batch_date(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: [{"id": 1, "created_at": ""}, {"id": 2}])
    watcher.catch_up(0, 1)
    assert written_ids(tmp_path) == [1, 2]
    assert [p.name for p in (tmp_path / "changesets").iterdir()] == ["2026-01-10.jsonl"]


def test_position_time_is_saved_and_drives_complete_before(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    assert watcher.complete_before() == "0000-00-00"           # nothing recorded yet
    patch_fetch(monkeypatch, watcher, lambda seq: [])

    watcher.catch_up(0, 3)

    assert watcher.load_epoch(3) == NOW
    assert watcher.load_epoch(2) is None                        # only valid for the exact sequence
    # NOW is 2026-01-10 12:00: 2026-01-08 froze at 01:00 today, so dates before 01-09 are final
    assert watcher.complete_before() == "2026-01-09"


def test_complete_before_is_unknown_when_the_time_was_not_recorded(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    watcher.save_state(5)                                       # old-style state: no epoch
    assert watcher.complete_before() == "0000-00-00"


def test_state_file_failures_do_not_raise(tmp_path, monkeypatch):
    watcher = ChangesetWatcher(tmp_path)
    patch_fetch(monkeypatch, watcher, lambda seq: [cs(seq, "2026-01-10T01:00:00Z")])

    def boom(self, seq):
        raise RuntimeError("state file unavailable")
    monkeypatch.setattr(ChangesetWatcher, "seq_epoch", boom)

    assert watcher.catch_up(0, 3) == (0, 0)                     # can't date the batch: wait and retry
    assert written_ids(tmp_path) == []

    watcher.save_state(0, NOW)                                  # now the start time is known
    assert watcher.catch_up(0, 3) == (3, 3)                     # files are written even though
    assert written_ids(tmp_path) == [1, 2, 3]                   # recording the end time failed
    assert watcher.complete_before() == "0000-00-00"            # ...so nothing is finalized yet


def test_seq_epoch_parses_the_state_file(tmp_path, monkeypatch):
    monkeypatch.undo()                                          # use the real seq_epoch here
    watcher = ChangesetWatcher(tmp_path)
    session = MagicMock()
    session.get.return_value = MagicMock(
        status_code=200, text="---\nlast_run: 2026-10-21 18:38:59.518083000 +00:00\nsequence: 7235369\n")
    monkeypatch.setattr(watcher, "_session", lambda: session)

    assert watcher.seq_epoch(7235369) == epoch(2026, 10, 21, 18, 38, 59)
    assert session.get.call_args[0][0].endswith("/007/235/369.state.txt")
