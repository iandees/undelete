# Overpass QL to DuckDB SQL Translator — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python module that translates Overpass QL queries into equivalent DuckDB SQL against the `osm_data` view.

**Architecture:** Lark parser with EBNF grammar → Transformer to IR dataclasses → SQL emitter producing DuckDB SQL with a `latest` CTE for current-state semantics.

**Tech Stack:** Python 3.11+, Lark parser generator, pytest

---

### Task 1: Add lark dependency

**Files:**
- Modify: `pyproject.toml:6-12`

- [ ] **Step 1: Add lark to dependencies**

In `pyproject.toml`, add `"lark"` to the `dependencies` list:

```toml
dependencies = [
    "requests",
    "boto3",
    "python-dotenv",
    "geopandas",
    "pyarrow",
    "shapely",
    "pyyaml>=6.0.3",
    "lark",
]
```

- [ ] **Step 2: Install and verify**

Run: `uv sync`
Expected: lark is installed successfully.

Run: `uv run python -c "import lark; print(lark.__version__)"`
Expected: prints a version number.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "Add lark parser dependency"
```

---

### Task 2: Grammar and parser — element types and tag exists filter

**Files:**
- Create: `overpass_to_sql.py`
- Create: `tests/test_overpass_to_sql.py`

This task builds the skeleton: grammar, IR dataclasses, transformer, SQL emitter, and the public `overpass_to_sql()` function. Only `node`/`way`/`relation`/`nwr` selectors and the `["key"]` tag-exists filter are supported. Later tasks extend the grammar and transformer incrementally.

- [ ] **Step 1: Write failing tests for element types and tag exists**

Create `tests/test_overpass_to_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: FAIL — `overpass_to_sql` module not found.

- [ ] **Step 3: Implement grammar, IR, transformer, and SQL emitter**

Create `overpass_to_sql.py`:

```python
from dataclasses import dataclass, field
from lark import Lark, Transformer, exceptions as lark_exceptions


class OverpassParseError(Exception):
    pass


# --- Intermediate representation ---

@dataclass
class TagFilter:
    key: str
    op: str  # "exists", "=", "!=", "~", "!~"
    value: str = ""
    case_insensitive: bool = False


@dataclass
class BboxFilter:
    south: float
    west: float
    north: float
    east: float


@dataclass
class AroundFilter:
    radius: float
    lat: float
    lon: float


@dataclass
class Statement:
    osm_type: str  # "node", "way", "relation", "nwr"
    tag_filters: list[TagFilter] = field(default_factory=list)
    geo_filter: BboxFilter | AroundFilter | None = None


@dataclass
class Query:
    statements: list[Statement] = field(default_factory=list)
    output_mode: str = "body"  # "body", "geom", "center", "count", "tags"


# --- Grammar ---

GRAMMAR = r"""
    start: statement+ output?

    statement: osm_type filter* ";"

    osm_type: "node" -> node
            | "way" -> way
            | "relation" -> relation
            | "nwr" -> nwr

    filter: tag_filter

    tag_filter: "[" ESCAPED_STRING "]" -> tag_exists

    output: "out" OUTPUT_MODE ";"
    OUTPUT_MODE: "body" | "geom" | "center" | "count" | "tags"

    %import common.ESCAPED_STRING
    %import common.WS
    %ignore WS
"""

# --- Transformer ---

class OverpassTransformer(Transformer):
    def start(self, items):
        query = Query()
        for item in items:
            if isinstance(item, Statement):
                query.statements.append(item)
            elif isinstance(item, str):
                query.output_mode = item
        return query

    def statement(self, items):
        osm_type = items[0]
        stmt = Statement(osm_type=osm_type)
        for item in items[1:]:
            if isinstance(item, TagFilter):
                stmt.tag_filters.append(item)
        return stmt

    def node(self, _):
        return "node"

    def way(self, _):
        return "way"

    def relation(self, _):
        return "relation"

    def nwr(self, _):
        return "nwr"

    def filter(self, items):
        return items[0]

    def tag_filter(self, items):
        return items[0]

    def tag_exists(self, items):
        key = items[0][1:-1]  # strip quotes
        return TagFilter(key=key, op="exists")

    def output(self, items):
        return str(items[0])


# --- SQL emitter ---

LATEST_CTE = """WITH latest AS (
  SELECT * FROM osm_data
  WHERE (osm_type, osm_id, version) IN (
    SELECT osm_type, osm_id, MAX(version)
    FROM osm_data
    GROUP BY osm_type, osm_id
  )
  AND action != 'delete'
)"""


def _select_clause(output_mode: str) -> str:
    match output_mode:
        case "body" | "geom":
            return "SELECT *"
        case "center":
            return "SELECT osm_type, osm_id, tags, ST_Centroid(geometry) AS geometry"
        case "count":
            return "SELECT COUNT(*) AS count"
        case "tags":
            return "SELECT osm_type, osm_id, tags"
        case _:
            return "SELECT *"


def _tag_filter_sql(tf: TagFilter) -> str:
    match tf.op:
        case "exists":
            return f"map_contains(tags, '{tf.key}')"
        case "=":
            return f"element_at(tags, '{tf.key}')[1] = '{tf.value}'"
        case "!=":
            return f"(NOT map_contains(tags, '{tf.key}') OR element_at(tags, '{tf.key}')[1] != '{tf.value}')"
        case "~":
            pattern = f"(?i){tf.value}" if tf.case_insensitive else tf.value
            return f"regexp_matches(element_at(tags, '{tf.key}')[1], '{pattern}')"
        case "!~":
            pattern = f"(?i){tf.value}" if tf.case_insensitive else tf.value
            return f"NOT regexp_matches(element_at(tags, '{tf.key}')[1], '{pattern}')"
        case _:
            raise OverpassParseError(f"Unknown tag filter operator: {tf.op}")


def _geo_filter_sql(gf: BboxFilter | AroundFilter) -> str:
    if isinstance(gf, BboxFilter):
        return f"ST_Within(geometry, ST_MakeEnvelope({gf.west}, {gf.south}, {gf.east}, {gf.north}))"
    elif isinstance(gf, AroundFilter):
        deg = gf.radius / 111320.0
        return f"ST_DWithin(geometry, ST_Point({gf.lon}, {gf.lat}), {deg})"


def _statement_sql(stmt: Statement, select: str) -> str:
    conditions = []
    if stmt.osm_type != "nwr":
        conditions.append(f"osm_type = '{stmt.osm_type}'")
    for tf in stmt.tag_filters:
        conditions.append(_tag_filter_sql(tf))
    if stmt.geo_filter:
        conditions.append(_geo_filter_sql(stmt.geo_filter))

    where = " AND ".join(conditions)
    if where:
        return f"{select} FROM latest WHERE {where}"
    else:
        return f"{select} FROM latest"


def _emit_sql(query: Query) -> str:
    select = _select_clause(query.output_mode)
    parts = [_statement_sql(stmt, select) for stmt in query.statements]

    if len(parts) == 1:
        body = parts[0]
    else:
        body = " UNION ALL ".join(parts)

    return f"{LATEST_CTE}\n{body}"


# --- Public API ---

_parser = Lark(GRAMMAR, parser="earley")


def overpass_to_sql(query: str) -> str:
    try:
        tree = _parser.parse(query)
    except lark_exceptions.LarkError as e:
        raise OverpassParseError(f"Failed to parse Overpass QL: {e}") from e
    ir = OverpassTransformer().transform(tree)
    return _emit_sql(ir)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add overpass_to_sql.py tests/test_overpass_to_sql.py
git commit -m "Add Overpass QL parser with element types and tag exists filter"
```

---

### Task 3: Tag value filters (=, !=, ~, !~)

**Files:**
- Modify: `overpass_to_sql.py` (grammar and transformer)
- Modify: `tests/test_overpass_to_sql.py`

- [ ] **Step 1: Write failing tests for tag value filters**

Add to `tests/test_overpass_to_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_overpass_to_sql.py -v -k "tag_equals or tag_not_equals or tag_regex or multiple_tag"`
Expected: FAIL — grammar doesn't support `=`, `!=`, `~`, `!~` operators yet.

- [ ] **Step 3: Extend grammar and transformer**

In `overpass_to_sql.py`, replace the `tag_filter` grammar rule:

```python
    tag_filter: "[" ESCAPED_STRING "]" -> tag_exists
```

with:

```python
    tag_filter: "[" ESCAPED_STRING "]" -> tag_exists
             | "[" ESCAPED_STRING "=" ESCAPED_STRING "]" -> tag_eq
             | "[" ESCAPED_STRING "!=" ESCAPED_STRING "]" -> tag_neq
             | "[" ESCAPED_STRING "~" ESCAPED_STRING "]" -> tag_regex
             | "[" ESCAPED_STRING "!~" ESCAPED_STRING "]" -> tag_nregex
             | "[" ESCAPED_STRING "~" ESCAPED_STRING "," "i" "]" -> tag_regex_i
```

Add transformer methods:

```python
    def tag_eq(self, items):
        return TagFilter(key=items[0][1:-1], op="=", value=items[1][1:-1])

    def tag_neq(self, items):
        return TagFilter(key=items[0][1:-1], op="!=", value=items[1][1:-1])

    def tag_regex(self, items):
        return TagFilter(key=items[0][1:-1], op="~", value=items[1][1:-1])

    def tag_nregex(self, items):
        return TagFilter(key=items[0][1:-1], op="!~", value=items[1][1:-1])

    def tag_regex_i(self, items):
        return TagFilter(key=items[0][1:-1], op="~", value=items[1][1:-1], case_insensitive=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add overpass_to_sql.py tests/test_overpass_to_sql.py
git commit -m "Add tag value filters (=, !=, ~, !~, case-insensitive)"
```

---

### Task 4: Geographic filters (bbox and around)

**Files:**
- Modify: `overpass_to_sql.py` (grammar and transformer)
- Modify: `tests/test_overpass_to_sql.py`

- [ ] **Step 1: Write failing tests for geo filters**

Add to `tests/test_overpass_to_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_overpass_to_sql.py -v -k "bbox or around or tag_and_bbox"`
Expected: FAIL — grammar doesn't support parenthesized geo filters yet.

- [ ] **Step 3: Extend grammar and transformer**

In `overpass_to_sql.py`, add to the `filter` rule and add geo filter rules:

```python
    filter: tag_filter
          | geo_filter

    geo_filter: "(" NUMBER "," NUMBER "," NUMBER "," NUMBER ")" -> bbox
              | "(around:" NUMBER "," NUMBER "," NUMBER ")" -> around
```

Add to imports in the grammar:

```python
    %import common.NUMBER
```

Add transformer methods:

```python
    def bbox(self, items):
        south, west, north, east = [float(x) for x in items]
        return BboxFilter(south=south, west=west, north=north, east=east)

    def around(self, items):
        radius, lat, lon = [float(x) for x in items]
        return AroundFilter(radius=radius, lat=lat, lon=lon)
```

Update the `statement` transformer method to handle geo filters:

```python
    def statement(self, items):
        osm_type = items[0]
        stmt = Statement(osm_type=osm_type)
        for item in items[1:]:
            if isinstance(item, TagFilter):
                stmt.tag_filters.append(item)
            elif isinstance(item, (BboxFilter, AroundFilter)):
                stmt.geo_filter = item
        return stmt
```

Note: the `statement` method should already handle this from the initial implementation. Verify it does; if not, update it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add overpass_to_sql.py tests/test_overpass_to_sql.py
git commit -m "Add bbox and around geographic filters"
```

---

### Task 5: Union blocks

**Files:**
- Modify: `overpass_to_sql.py` (grammar and transformer)
- Modify: `tests/test_overpass_to_sql.py`

- [ ] **Step 1: Write failing tests for union blocks**

Add to `tests/test_overpass_to_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_overpass_to_sql.py -v -k "union"`
Expected: FAIL — grammar doesn't support union blocks yet.

- [ ] **Step 3: Extend grammar and transformer**

Update the `start` rule and add a `union` rule:

```python
    start: (statement | union)+ output?

    union: "(" statement+ ")" ";"
```

Add transformer method:

```python
    def union(self, items):
        # items are Statement objects; return them as a list to flatten into query.statements
        return items
```

Update the `start` transformer to handle union lists:

```python
    def start(self, items):
        query = Query()
        for item in items:
            if isinstance(item, Statement):
                query.statements.append(item)
            elif isinstance(item, list):
                query.statements.extend(item)
            elif isinstance(item, str):
                query.output_mode = item
        return query
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add overpass_to_sql.py tests/test_overpass_to_sql.py
git commit -m "Add union block support"
```

---

### Task 6: Output directives

**Files:**
- Modify: `tests/test_overpass_to_sql.py`

Output directives were implemented in Task 2's initial code. This task adds tests to verify them.

- [ ] **Step 1: Write tests for output directives**

Add to `tests/test_overpass_to_sql.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_overpass_to_sql.py -v -k "out_"`
Expected: all 5 PASS. If any fail, fix the grammar/transformer.

- [ ] **Step 3: Commit**

```bash
git add tests/test_overpass_to_sql.py
git commit -m "Add tests for output directives"
```

---

### Task 7: Error handling for unsupported syntax

**Files:**
- Modify: `tests/test_overpass_to_sql.py`

- [ ] **Step 1: Write tests for error cases**

Add to `tests/test_overpass_to_sql.py`:

```python
def test_error_empty_query():
    with pytest.raises(OverpassParseError):
        overpass_to_sql('')


def test_error_missing_semicolon():
    with pytest.raises(OverpassParseError):
        overpass_to_sql('node["building"]')


def test_error_unsupported_recurse():
    with pytest.raises(OverpassParseError):
        overpass_to_sql('node["building"]; >;')


def test_error_unsupported_out_meta():
    with pytest.raises(OverpassParseError):
        overpass_to_sql('node; out meta;')


def test_error_garbage_input():
    with pytest.raises(OverpassParseError):
        overpass_to_sql('SELECT * FROM osm_data;')
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/test_overpass_to_sql.py -v -k "error"`
Expected: all 5 PASS — invalid input should raise `OverpassParseError`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_overpass_to_sql.py
git commit -m "Add error handling tests for unsupported syntax"
```

---

### Task 8: End-to-end integration tests

**Files:**
- Modify: `tests/test_overpass_to_sql.py`

- [ ] **Step 1: Write end-to-end tests with realistic Overpass queries**

Add to `tests/test_overpass_to_sql.py`:

```python
# --- End-to-end realistic queries ---

def test_e2e_cafes_in_london():
    """node["amenity"="cafe"](51.28,-0.49,51.69,0.26); out body;"""
    sql = overpass_to_sql('node["amenity"="cafe"](51.28,-0.49,51.69,0.26); out body;')
    normed = _normalize(sql)
    assert "element_at(tags, 'amenity')[1] = 'cafe'" in normed
    assert "ST_Within(geometry, ST_MakeEnvelope(-0.49, 51.28, 0.26, 51.69))" in normed
    assert "osm_type = 'node'" in normed
    assert "action != 'delete'" in normed


def test_e2e_buildings_union():
    """Find all building features (nodes, ways, relations)."""
    sql = overpass_to_sql('(node["building"]; way["building"]; relation["building"];); out count;')
    normed = _normalize(sql)
    assert normed.count("UNION ALL") == 2
    assert "SELECT COUNT(*) AS count" in normed
    assert normed.count("map_contains(tags, 'building')") == 3


def test_e2e_restaurants_near_point():
    """Restaurants within 500m of a point."""
    sql = overpass_to_sql('nwr["amenity"="restaurant"](around:500,48.8566,2.3522);')
    normed = _normalize(sql)
    assert "element_at(tags, 'amenity')[1] = 'restaurant'" in normed
    assert "ST_DWithin(geometry, ST_Point(2.3522, 48.8566)" in normed
    assert "osm_type" not in normed.split("FROM latest")[1]


def test_e2e_default_output():
    """No explicit out directive defaults to SELECT *."""
    sql = overpass_to_sql('node["shop"];')
    normed = _normalize(sql)
    assert "SELECT *" in normed
```

- [ ] **Step 2: Run all tests**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_overpass_to_sql.py
git commit -m "Add end-to-end integration tests for Overpass QL translator"
```

---

### Task 9: Negative number support in grammar

**Files:**
- Modify: `overpass_to_sql.py` (grammar)
- Modify: `tests/test_overpass_to_sql.py`

The Lark `NUMBER` terminal only matches unsigned numbers. Bbox and around filters need negative coordinates (e.g., western hemisphere longitudes).

- [ ] **Step 1: Write failing test**

Add to `tests/test_overpass_to_sql.py`:

```python
def test_bbox_negative_coords():
    sql = overpass_to_sql('node(-33.9,-18.5,-33.8,-18.4);')
    normed = _normalize(sql)
    assert "ST_Within(geometry, ST_MakeEnvelope(-18.5, -33.9, -18.4, -33.8))" in normed
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_overpass_to_sql.py::test_bbox_negative_coords -v`
Expected: FAIL — parser chokes on the negative sign.

- [ ] **Step 3: Add signed number terminal to grammar**

In the grammar, add a `SIGNED_NUMBER` rule and update geo filter rules:

```python
    geo_filter: "(" SIGNED_NUMBER "," SIGNED_NUMBER "," SIGNED_NUMBER "," SIGNED_NUMBER ")" -> bbox
              | "(around:" SIGNED_NUMBER "," SIGNED_NUMBER "," SIGNED_NUMBER ")" -> around

    SIGNED_NUMBER: /[+-]?(\d+\.?\d*|\d*\.?\d+)([eE][+-]?\d+)?/
```

Remove the `%import common.NUMBER` line if it was only used by geo filters.

- [ ] **Step 4: Run all tests**

Run: `uv run pytest tests/test_overpass_to_sql.py -v`
Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add overpass_to_sql.py tests/test_overpass_to_sql.py
git commit -m "Support negative coordinates in bbox and around filters"
```
