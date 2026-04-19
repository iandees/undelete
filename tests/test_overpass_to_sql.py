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


# --- Tag value filters ---

def test_tag_equals():
    sql = overpass_to_sql('node["amenity"="cafe"];')
    normed = _normalize(sql)
    assert "element_at(tags, 'amenity')[1] = 'cafe'" in normed


def test_tag_not_equals():
    sql = overpass_to_sql('node["amenity"!="cafe"];')
    normed = _normalize(sql)
    assert "(NOT map_contains(tags, 'amenity') OR element_at(tags, 'amenity')[1] != 'cafe')" in normed


def test_tag_regex():
    sql = overpass_to_sql('node["name"~"^Mc"];')
    normed = _normalize(sql)
    assert "regexp_matches(element_at(tags, 'name')[1], '^Mc')" in normed


def test_tag_regex_negated():
    sql = overpass_to_sql('node["name"!~"^Mc"];')
    normed = _normalize(sql)
    assert "NOT regexp_matches(element_at(tags, 'name')[1], '^Mc')" in normed


def test_tag_regex_case_insensitive():
    sql = overpass_to_sql('node["name"~"cafe",i];')
    normed = _normalize(sql)
    assert "regexp_matches(element_at(tags, 'name')[1], '(?i)cafe')" in normed


def test_multiple_tag_filters():
    sql = overpass_to_sql('node["amenity"="cafe"]["cuisine"="italian"];')
    normed = _normalize(sql)
    assert "element_at(tags, 'amenity')[1] = 'cafe'" in normed
    assert "element_at(tags, 'cuisine')[1] = 'italian'" in normed
    # Both conditions should be ANDed
    where_part = normed.split("WHERE")[2]  # second WHERE (after CTE)
    assert " AND " in where_part


# --- Geographic filters ---

def test_bbox_filter():
    sql = overpass_to_sql('node(51.5,-0.1,51.6,0.1);')
    normed = _normalize(sql)
    assert "ST_Within(geometry, ST_MakeEnvelope(-0.1, 51.5, 0.1, 51.6))" in normed


def test_around_filter():
    sql = overpass_to_sql('node(around:1000,51.5,-0.1);')
    normed = _normalize(sql)
    assert "ST_DWithin(geometry, ST_Point(-0.1, 51.5)" in normed


def test_tag_and_bbox():
    sql = overpass_to_sql('node["amenity"="cafe"](51.5,-0.1,51.6,0.1);')
    normed = _normalize(sql)
    assert "element_at(tags, 'amenity')[1] = 'cafe'" in normed
    assert "ST_Within(geometry, ST_MakeEnvelope(-0.1, 51.5, 0.1, 51.6))" in normed


# --- Union blocks ---

def test_union_block():
    sql = overpass_to_sql('(node["amenity"="cafe"]; way["amenity"="cafe"];);')
    normed = _normalize(sql)
    assert "UNION ALL" in normed
    assert "osm_type = 'node'" in normed
    assert "osm_type = 'way'" in normed


def test_union_three_types():
    sql = overpass_to_sql('(node["building"]; way["building"]; relation["building"];);')
    normed = _normalize(sql)
    assert normed.count("UNION ALL") == 2


# --- Output directives ---

def test_out_body():
    sql = overpass_to_sql('node["building"]; out body;')
    normed = _normalize(sql)
    assert "SELECT *" in normed


def test_out_geom():
    sql = overpass_to_sql('node["building"]; out geom;')
    normed = _normalize(sql)
    assert "SELECT *" in normed


def test_out_center():
    sql = overpass_to_sql('node["building"]; out center;')
    normed = _normalize(sql)
    assert "ST_Centroid(geometry) AS geometry" in normed
    assert "osm_type, osm_id, tags" in normed


def test_out_count():
    sql = overpass_to_sql('node["building"]; out count;')
    normed = _normalize(sql)
    assert "SELECT COUNT(*) AS count" in normed


def test_out_tags():
    sql = overpass_to_sql('node["building"]; out tags;')
    normed = _normalize(sql)
    assert "SELECT osm_type, osm_id, tags" in normed
    assert "geometry" not in normed.split("FROM latest")[1]
