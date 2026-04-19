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

LATEST_CTE = (
    "WITH latest AS ("
    "SELECT * FROM osm_data "
    "WHERE (osm_type, osm_id, version) IN ("
    "SELECT osm_type, osm_id, MAX(version) FROM osm_data GROUP BY osm_type, osm_id"
    ") AND action != 'delete'"
    ")"
)


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


def _sql_str(s: str) -> str:
    return s.replace("'", "''")


def _tag_filter_sql(tf: TagFilter) -> str:
    key = _sql_str(tf.key)
    val = _sql_str(tf.value)
    match tf.op:
        case "exists":
            return f"map_contains(tags, '{key}')"
        case "=":
            return f"element_at(tags, '{key}')[1] = '{val}'"
        case "!=":
            return (
                f"(NOT map_contains(tags, '{key}') "
                f"OR element_at(tags, '{key}')[1] != '{val}')"
            )
        case "~":
            pattern = f"(?i){val}" if tf.case_insensitive else val
            return f"regexp_matches(element_at(tags, '{key}')[1], '{pattern}')"
        case "!~":
            pattern = f"(?i){val}" if tf.case_insensitive else val
            return f"NOT regexp_matches(element_at(tags, '{key}')[1], '{pattern}')"
        case _:
            raise OverpassParseError(f"Unknown tag filter operator: {tf.op}")


def _geo_filter_sql(gf: BboxFilter | AroundFilter) -> str:
    if isinstance(gf, BboxFilter):
        return (
            f"ST_Within(geometry, "
            f"ST_MakeEnvelope({gf.west}, {gf.south}, {gf.east}, {gf.north}))"
        )
    elif isinstance(gf, AroundFilter):
        deg = gf.radius / 111320.0
        return f"ST_DWithin(geometry, ST_Point({gf.lon}, {gf.lat}), {deg})"
    else:
        raise OverpassParseError(f"Unknown geo filter type: {type(gf)}")


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
