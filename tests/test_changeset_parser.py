from pathlib import Path

from daemon.changeset_parser import parse_changesets

FIXTURES = Path(__file__).parent / "fixtures"


def _parse_fixture(name):
    with open(FIXTURES / name, "rb") as f:
        return parse_changesets(f)


def test_parse_closed_changeset_with_bbox():
    changesets = _parse_fixture("sample_changesets.xml")
    cs = changesets[0]
    assert cs["id"] == 12345
    assert cs["created_at"] == "2025-01-15T10:00:00Z"
    assert cs["closed_at"] == "2025-01-15T10:30:00Z"
    assert cs["open"] is False
    assert cs["num_changes"] == 42
    assert cs["user"] == "testuser"
    assert cs["uid"] == 1001
    assert cs["min_lat"] == 48.0
    assert cs["max_lat"] == 48.5
    assert cs["min_lon"] == 2.0
    assert cs["max_lon"] == 2.5
    assert cs["comments_count"] == 1
    assert cs["tags"] == {
        "comment": "Adding buildings",
        "created_by": "JOSM/1.5",
        "source": "survey",
    }


def test_parse_changeset_without_bbox():
    changesets = _parse_fixture("sample_changesets.xml")
    cs = changesets[2]
    assert cs["id"] == 12347
    assert cs["open"] is True
    assert cs["num_changes"] == 0
    assert cs["min_lat"] is None
    assert cs["max_lat"] is None
    assert cs["min_lon"] is None
    assert cs["max_lon"] is None
    assert cs["tags"] == {}


def test_parse_all_changesets():
    changesets = _parse_fixture("sample_changesets.xml")
    assert len(changesets) == 3
    ids = [cs["id"] for cs in changesets]
    assert ids == [12345, 12346, 12347]


def test_parse_changeset_tags():
    changesets = _parse_fixture("sample_changesets.xml")
    cs = changesets[1]
    assert cs["tags"]["comment"] == "Fix road alignment"
    assert cs["tags"]["created_by"] == "iD"
    assert len(cs["tags"]) == 2
