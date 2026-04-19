import pytest
from overpass_to_sql import overpass_to_sql, OverpassParseError

LATEST_CTE = (
    "WITH latest AS ("
    "SELECT * FROM osm_data "
    "WHERE (osm_type, osm_id, version) IN ("
    "SELECT osm_type, osm_id, MAX(version) FROM osm_data GROUP BY osm_type, osm_id"
    ") AND action != 'delete'"
    ")"
)


def _normalize(sql):
    """Collapse whitespace for comparison."""
    return " ".join(sql.split())


# --- Element type selectors ---

def test_node_no_filter():
    sql = overpass_to_sql('node;')
    normed = _normalize(sql)
    assert LATEST_CTE in _normalize(sql)
    assert "WHERE osm_type = 'node'" in normed


def test_way_no_filter():
    sql = overpass_to_sql('way;')
    assert "osm_type = 'way'" in _normalize(sql)


def test_relation_no_filter():
    sql = overpass_to_sql('relation;')
    assert "osm_type = 'relation'" in _normalize(sql)


def test_nwr_no_filter():
    sql = overpass_to_sql('nwr;')
    normed = _normalize(sql)
    assert "osm_type" not in normed.split("FROM latest")[1]


# --- Tag exists filter ---

def test_tag_exists():
    sql = overpass_to_sql('node["building"];')
    normed = _normalize(sql)
    assert "map_contains(tags, 'building')" in normed
    assert "osm_type = 'node'" in normed
