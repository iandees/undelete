import gzip
import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from daemon.changeset_watcher import ChangesetWatcher

FIXTURE = Path(__file__).parent / "fixtures" / "sample_changesets.xml"


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
