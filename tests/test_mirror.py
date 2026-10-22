import gzip
from unittest.mock import MagicMock

import pytest

from daemon.changeset_watcher import ChangesetWatcher
from daemon.mirror import S3, changeset_url, element_url
from daemon.osc_augmenter import download_osc
from daemon.replication_planner import HttpStateSource

PLANET = "https://planet.openstreetmap.org/replication"


def test_defaults_to_the_s3_mirror(monkeypatch):
    monkeypatch.delenv("REPLICATION_MIRROR", raising=False)
    assert element_url("hour", "state.txt") == f"{S3}/planet/replication/hour/state.txt"


@pytest.mark.parametrize("interval,rel", [
    ("minute", "state.txt"), ("minute", "007/338/442.state.txt"), ("minute", "007/338/442.osc.gz"),
    ("hour", "state.txt"), ("hour", "000/123/654.osc.gz"), ("day", "000/005/150.state.txt"),
])
def test_element_urls_on_both_mirrors(monkeypatch, interval, rel):
    monkeypatch.setenv("REPLICATION_MIRROR", "s3")
    assert element_url(interval, rel) == f"{S3}/planet/replication/{interval}/{rel}"
    monkeypatch.setenv("REPLICATION_MIRROR", "planet")
    assert element_url(interval, rel) == f"{PLANET}/{interval}/{rel}"


@pytest.mark.parametrize("rel", ["state.yaml", "007/235/370.state.txt", "007/235/370.osm.gz"])
def test_changeset_urls_on_both_mirrors(monkeypatch, rel):
    # the bucket's layout differs from planet's, which is why this is centralized
    monkeypatch.setenv("REPLICATION_MIRROR", "s3")
    assert changeset_url(rel) == f"{S3}/changesets/replication/minute/{rel}"
    monkeypatch.setenv("REPLICATION_MIRROR", "planet")
    assert changeset_url(rel) == f"{PLANET}/changesets/{rel}"


def test_unknown_mirror_is_an_error(monkeypatch):
    monkeypatch.setenv("REPLICATION_MIRROR", "cdn")
    with pytest.raises(ValueError, match="REPLICATION_MIRROR"):
        element_url("hour", "state.txt")


def test_state_source_reads_from_the_mirror(monkeypatch):
    monkeypatch.delenv("REPLICATION_MIRROR", raising=False)
    session = MagicMock()
    session.get.return_value = MagicMock(
        text="sequenceNumber=5\ntimestamp=2026-10-21T13\\:00\\:00Z\n", status_code=200)
    source = HttpStateSource(session)

    source.latest("day")
    source.timestamp("minute", 7338442)

    urls = [call.args[0] for call in session.get.call_args_list]
    assert urls == [f"{S3}/planet/replication/day/state.txt",
                    f"{S3}/planet/replication/minute/007/338/442.state.txt"]


def test_diff_downloads_come_from_the_mirror(monkeypatch, tmp_path):
    monkeypatch.delenv("REPLICATION_MIRROR", raising=False)
    session = MagicMock()
    session.get.return_value = MagicMock(raw=__import__("io").BytesIO(gzip.compress(b"<osmChange/>")))

    path = download_osc(session, 123654, "hour", str(tmp_path))

    assert session.get.call_args.args[0] == f"{S3}/planet/replication/hour/000/123/654.osc.gz"
    assert path.endswith("hour-123654.osc.gz")


def test_changeset_files_come_from_the_mirror(monkeypatch, tmp_path):
    monkeypatch.delenv("REPLICATION_MIRROR", raising=False)
    watcher = ChangesetWatcher(tmp_path)
    session = MagicMock()
    session.get.return_value = MagicMock(status_code=404)
    monkeypatch.setattr(watcher, "_session", lambda: session)

    assert watcher.fetch(7235370) is None            # a 404 from the bucket still means "not yet"
    assert session.get.call_args.args[0] == f"{S3}/changesets/replication/minute/007/235/370.osm.gz"
