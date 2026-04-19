# Overpass QL to DuckDB SQL Translator

## Overview

A Python module that translates a subset of Overpass QL into equivalent DuckDB SQL queries against the `osm_data` view. This lets users who are familiar with Overpass QL query their change data without learning DuckDB SQL.

The translator is a Python module intended for server-side use, with the goal of eventually backing an Overpass-compatible API endpoint.

## Data model bridge

The `osm_data` view stores *changes* (create/modify/delete), but Overpass QL queries *current state*. The translator bridges this by generating a `latest` CTE that picks the most recent version of each element and excludes deletions:

```sql
WITH latest AS (
  SELECT * FROM osm_data
  WHERE (osm_type, osm_id, version) IN (
    SELECT osm_type, osm_id, MAX(version)
    FROM osm_data
    GROUP BY osm_type, osm_id
  )
  AND action != 'delete'
)
SELECT ... FROM latest WHERE ...
```

This reconstructs "current state as we know it" from the change feed. The dataset is not the full OSM database (currently ~90 days of changes), but the translation is correct for the data present and will scale if full data is added later.

## Supported Overpass QL subset

### Element type selectors

- `node`, `way`, `relation` — filter by OSM element type
- `nwr` — shorthand for all three types (no type filter applied)

### Tag filters (bracket syntax)

- `["key"]` — tag exists
- `["key"="value"]` — exact match
- `["key"!="value"]` — not equal
- `["key"~"regex"]` — regex match
- `["key"!~"regex"]` — negated regex
- `["key"~"regex",i]` — case-insensitive regex

Multiple bracket groups on one statement are ANDed together.

### Geographic filters (parenthesis syntax)

- `(south,west,north,east)` — bounding box filter
- `(around:radius,lat,lon)` — within radius meters of a point

### Union blocks

- `(statement1; statement2;)` — combine results via UNION ALL

### Output directives

- `out body` — full row (default)
- `out geom` — include geometry (equivalent to body for this dataset)
- `out center` — return centroid instead of full geometry
- `out count` — return count only
- `out tags` — tags only, no geometry

Semicolons terminate statements.

### Not supported

`recurse` (`>`, `>>`), `foreach`, `if`, `convert`, `make`, `timeline`, `diff`, `out meta`/`out skel`, `[out:json]` settings, `area` pivots, named areas. Unsupported syntax produces a clear error message.

## SQL generation mapping

### Tag filters → WHERE clauses

| Overpass QL | DuckDB SQL |
|---|---|
| `["key"]` | `map_contains(tags, 'key')` |
| `["key"="value"]` | `element_at(tags, 'key')[1] = 'value'` |
| `["key"!="value"]` | `(NOT map_contains(tags, 'key') OR element_at(tags, 'key')[1] != 'value')` |
| `["key"~"regex"]` | `regexp_matches(element_at(tags, 'key')[1], 'regex')` |
| `["key"!~"regex"]` | `NOT regexp_matches(element_at(tags, 'key')[1], 'regex')` |
| `["key"~"regex",i]` | `regexp_matches(element_at(tags, 'key')[1], '(?i)regex')` |

### Element types → WHERE clauses

| Overpass QL | DuckDB SQL |
|---|---|
| `node` | `osm_type = 'node'` |
| `way` | `osm_type = 'way'` |
| `relation` | `osm_type = 'relation'` |
| `nwr` | (no type filter) |

### Geographic filters → WHERE clauses

| Overpass QL | DuckDB SQL |
|---|---|
| `(south,west,north,east)` | `ST_Within(geometry, ST_MakeEnvelope(west, south, east, north))` |
| `(around:radius,lat,lon)` | `ST_DWithin(geometry, ST_Point(lon, lat), radius / 111320.0)` |

The `around` radius is converted from meters to approximate degrees by dividing by 111320. This is imprecise at high latitudes but adequate for a convenience feature.

### Union blocks → UNION ALL

Each statement in a union block becomes a separate SELECT, combined with UNION ALL.

### Output directives → SELECT clause

| Overpass QL | DuckDB SQL |
|---|---|
| `out body` | `SELECT *` |
| `out geom` | `SELECT *` |
| `out center` | `SELECT osm_type, osm_id, tags, ST_Centroid(geometry) AS geometry` |
| `out count` | `SELECT COUNT(*) AS count` |
| `out tags` | `SELECT osm_type, osm_id, tags` |

## Module structure

Single file: `overpass_to_sql.py` at the project root.

Three internal layers:

1. **Grammar** — Lark EBNF grammar string defining the Overpass QL subset, stored as a module constant.
2. **Transformer** — Lark `Transformer` class that converts the parse tree into an intermediate representation (dataclasses capturing element type, tag filters, geo filters, output mode).
3. **SQL emitter** — takes the IR and produces a DuckDB SQL string including the `latest` CTE.

### Public API

```python
def overpass_to_sql(query: str) -> str:
    """Translate an Overpass QL query to DuckDB SQL.
    
    Raises OverpassParseError for unsupported or invalid syntax.
    """
```

### Error handling

- Unsupported syntax: Lark parse errors are caught and re-raised as `OverpassParseError` with a descriptive message (e.g., "Unsupported syntax: recurse operator '>' is not supported").
- SQL injection: not a concern because SQL is built from parsed AST nodes, not raw string concatenation. The grammar acts as a whitelist.

## Dependencies

- `lark` — parser generator library (add to project dependencies)

## Testing

Test file with cases covering:

- Each tag filter variant
- Each element type selector including `nwr`
- Bounding box and around filters
- Multiple filters on one statement (AND behavior)
- Union blocks
- Each output directive
- Error cases for unsupported syntax
- End-to-end examples matching common Overpass queries
