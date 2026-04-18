import json
from io import BytesIO
from pathlib import Path

from daemon.adiff_parser import parse_adiff

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_deleted_nodes():
    features = list(parse_adiff(str(FIXTURES / "sample_delete_nodes.xml")))

    # Should only return delete actions, not modify
    assert len(features) == 2

    # First node: no tags
    f0 = features[0]
    assert f0["type"] == "Feature"
    assert f0["geometry"]["type"] == "Point"
    assert f0["geometry"]["coordinates"] == [6.4840074, 48.5848312]
    assert f0["properties"]["osm_type"] == "node"
    assert f0["properties"]["osm_id"] == 245053500
    assert f0["properties"]["version"] == 4
    # Deletion metadata comes from <new>
    assert f0["properties"]["deleted_by"] == "patman37"
    assert f0["properties"]["deleted_uid"] == 311391
    assert f0["properties"]["deleted_changeset"] == 161348203
    assert f0["properties"]["deleted_at"] == "2025-01-14T14:51:43Z"
    assert f0["properties"]["tags"] == {}

    # Second node: has tags
    f1 = features[1]
    assert f1["geometry"]["coordinates"] == [-73.9857, 40.7484]
    assert f1["properties"]["osm_id"] == 100000001
    assert f1["properties"]["tags"] == {"name": "Test Node", "amenity": "cafe"}
    assert f1["properties"]["deleted_by"] == "deleterB"
    assert f1["properties"]["deleted_changeset"] == 200


def test_parse_deleted_ways():
    features = list(parse_adiff(str(FIXTURES / "sample_delete_ways.xml")))

    assert len(features) == 2

    # First way: closed (first nd ref == last nd ref) -> Polygon
    f0 = features[0]
    assert f0["geometry"]["type"] == "Polygon"
    coords = f0["geometry"]["coordinates"][0]
    assert len(coords) == 5
    assert coords[0] == [3.1965474, 50.7989125]
    assert coords[0] == coords[-1]  # closed ring
    assert f0["properties"]["osm_type"] == "way"
    assert f0["properties"]["osm_id"] == 383040181
    assert f0["properties"]["tags"] == {"building": "shed"}

    # Second way: open (first nd ref != last nd ref) -> LineString
    f1 = features[1]
    assert f1["geometry"]["type"] == "LineString"
    assert len(f1["geometry"]["coordinates"]) == 3
    assert f1["properties"]["tags"] == {"highway": "residential", "name": "Test Street"}


def test_parse_empty_xml():
    source = BytesIO(b'<?xml version="1.0" encoding="UTF-8"?><osm version="0.6"></osm>')
    features = list(parse_adiff(source))
    assert features == []
