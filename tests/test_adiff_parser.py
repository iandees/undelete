from io import BytesIO
from pathlib import Path

from daemon.adiff_parser import parse_adiff

FIXTURES = Path(__file__).parent / "fixtures"


def _parse_fixture(name):
    return list(parse_adiff(str(FIXTURES / name)))


def _features_by_id(features):
    return {f["properties"]["osm_id"]: f for f in features}


# --- Tests using sample_all_actions.xml ---

def _all_actions():
    return _parse_fixture("sample_all_actions.xml")


def test_parse_create_node():
    feats = _features_by_id(_all_actions())
    f = feats[100]
    assert f["type"] == "Feature"
    assert f["geometry"] == {"type": "Point", "coordinates": [1.0, 2.0]}
    p = f["properties"]
    assert p["action"] == "create"
    assert p["osm_type"] == "node"
    assert p["osm_id"] == 100
    assert p["version"] == 1
    assert p["changeset"] == 5001
    assert p["user"] == "creator1"
    assert p["uid"] == 1001
    assert p["timestamp"] == "2025-01-10T10:00:00Z"
    assert p["tags"] == {"name": "New Cafe", "amenity": "cafe"}
    assert p["old_tags"] is None
    assert p["old_geometry"] is None


def test_parse_modify_node():
    feats = _features_by_id(_all_actions())
    f = feats[200]
    assert f["geometry"] == {"type": "Point", "coordinates": [3.001, 4.001]}
    p = f["properties"]
    assert p["action"] == "modify"
    assert p["osm_type"] == "node"
    assert p["version"] == 4
    assert p["changeset"] == 6002
    assert p["user"] == "modifier1"
    assert p["uid"] == 2002
    assert p["timestamp"] == "2025-01-11T11:00:00Z"
    assert p["tags"] == {"name": "New Name"}
    assert p["old_tags"] == {"name": "Old Name"}
    assert p["old_geometry"] == {"type": "Point", "coordinates": [3.0, 4.0]}


def test_parse_delete_node():
    feats = _features_by_id(_all_actions())
    f = feats[300]
    # Geometry comes from old
    assert f["geometry"] == {"type": "Point", "coordinates": [5.0, 6.0]}
    p = f["properties"]
    assert p["action"] == "delete"
    assert p["osm_type"] == "node"
    assert p["osm_id"] == 300
    # Version from old (last visible version)
    assert p["version"] == 5
    # Metadata from new (who deleted it)
    assert p["changeset"] == 7002
    assert p["user"] == "deleter1"
    assert p["uid"] == 3002
    assert p["timestamp"] == "2025-01-12T12:00:00Z"
    assert p["tags"] == {"name": "Deleted Place"}
    assert p["old_tags"] is None
    assert p["old_geometry"] is None


def test_parse_modify_way():
    feats = _features_by_id(_all_actions())
    f = feats[400]
    assert f["geometry"]["type"] == "LineString"
    assert f["geometry"]["coordinates"] == [
        [20.0, 10.0], [20.1, 10.1], [20.2, 10.2]
    ]
    p = f["properties"]
    assert p["action"] == "modify"
    assert p["osm_type"] == "way"
    assert p["version"] == 3
    assert p["tags"] == {"highway": "tertiary"}
    assert p["old_tags"] == {"highway": "residential"}
    assert p["old_geometry"] == {
        "type": "LineString",
        "coordinates": [[20.0, 10.0], [20.1, 10.1], [20.2, 10.2]],
    }


def test_parse_create_way_polygon():
    feats = _features_by_id(_all_actions())
    f = feats[500]
    assert f["geometry"]["type"] == "Polygon"
    coords = f["geometry"]["coordinates"][0]
    assert len(coords) == 5
    assert coords[0] == coords[-1]  # closed ring
    p = f["properties"]
    assert p["action"] == "create"
    assert p["osm_type"] == "way"
    assert p["tags"] == {"building": "yes"}
    assert p["old_tags"] is None
    assert p["old_geometry"] is None


def test_parse_empty_xml():
    source = BytesIO(b'<?xml version="1.0" encoding="UTF-8"?><osm version="0.6"></osm>')
    features = list(parse_adiff(source))
    assert features == []


def test_existing_delete_nodes_fixture():
    """Backward compat: existing fixture now returns 2 deletes + 1 modify."""
    features = _parse_fixture("sample_delete_nodes.xml")
    assert len(features) == 3
    actions = [f["properties"]["action"] for f in features]
    assert actions.count("delete") == 2
    assert actions.count("modify") == 1

    # Check a delete still works
    deletes = [f for f in features if f["properties"]["action"] == "delete"]
    f0 = deletes[0]
    assert f0["properties"]["osm_id"] == 245053500
    assert f0["geometry"]["coordinates"] == [6.4840074, 48.5848312]
    assert f0["properties"]["user"] == "patman37"
    assert f0["properties"]["changeset"] == 161348203


def test_existing_delete_ways_fixture():
    """Backward compat: existing fixture still returns 2 deletes."""
    features = _parse_fixture("sample_delete_ways.xml")
    assert len(features) == 2
    for f in features:
        assert f["properties"]["action"] == "delete"
    assert features[0]["properties"]["osm_id"] == 383040181
    assert features[0]["geometry"]["type"] == "Polygon"
    assert features[1]["properties"]["osm_id"] == 500000001
    assert features[1]["geometry"]["type"] == "LineString"
