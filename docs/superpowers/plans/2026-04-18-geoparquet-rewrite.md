# GeoParquet Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite OSM undelete from a deletion-only PMTiles viewer into a general-purpose OSM change explorer backed by GeoParquet files queryable via DuckDB WASM.

**Architecture:** The daemon ingests all adiff actions (create/modify/delete) into daily GeoJSON-lines files, then periodically converts them to GeoParquet with WKB geometry columns. Files are uploaded to R2 in Hive-partitioned layout (`osm-changes/date=YYYY-MM-DD/data.parquet`). The frontend uses DuckDB WASM to query remote Parquet files and renders results on a MapLibre map with a data table below.

**Tech Stack:** Python (geopandas, pyarrow, shapely), DuckDB WASM, MapLibre GL JS, vanilla JS

---

## File Structure

### Modified Files

| File | Responsibility |
|------|---------------|
| `daemon/adiff_parser.py` | Parse all action types (create/modify/delete), extract old+new state, build WKB geometries including multipolygons for relations |
| `daemon/geojson_writer.py` | Unchanged interface, but records now carry old+new fields for all action types |
| `daemon/watcher.py` | Minor update: log message says "changes" not "deletions" |
| `pipeline/merge_upload.py` | Update `upload_file` to handle Hive-partitioned paths |
| `pipeline/prune.py` | Accept `.parquet` suffix, prune Hive-partitioned directories |
| `main.py` | Replace tile build logic with parquet build + upload, update manifest to metadata.json |
| `pyproject.toml` | Add geopandas, pyarrow, shapely dependencies |
| `Dockerfile` | Remove tippecanoe build stage, add geo packages |
| `serve.py` | Add routing for `/web/` and `/data/` paths |

### New Files

| File | Responsibility |
|------|---------------|
| `pipeline/build_parquet.py` | Convert daily GeoJSON-lines to GeoParquet with WKB geometry columns |
| `web/index.html` | Complete rewrite: DuckDB WASM query interface + MapLibre map + results table |
| `tests/test_build_parquet.py` | Tests for Parquet conversion |
| `tests/fixtures/sample_all_actions.xml` | Fixture with create/modify/delete actions including relations |

### Removed Files

| File | Reason |
|------|--------|
| `pipeline/build_tiles.py` | Replaced by `pipeline/build_parquet.py` |
| `tests/test_build_tiles.py` | No longer needed |

---

### Task 1: Update adiff_parser to capture all action types

**Files:**
- Modify: `daemon/adiff_parser.py`
- Modify: `tests/fixtures/sample_delete_nodes.xml` (rename to keep, but add new fixture)
- Create: `tests/fixtures/sample_all_actions.xml`
- Modify: `tests/test_adiff_parser.py`

- [ ] **Step 1: Create fixture with all action types**

Create `tests/fixtures/sample_all_actions.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <action type="create">
    <new>
      <node id="100" version="1" timestamp="2025-01-14T10:00:00Z"
            uid="100" user="creator" changeset="1000"
            lon="1.0" lat="2.0">
        <tag k="name" v="New Cafe"/>
        <tag k="amenity" v="cafe"/>
      </node>
    </new>
  </action>
  <action type="modify">
    <old>
      <node id="200" version="1" lon="3.0" lat="4.0"
            user="origUser" uid="200" timestamp="2024-06-01T10:00:00Z" changeset="2000">
        <tag k="name" v="Old Name"/>
        <tag k="shop" v="bakery"/>
      </node>
    </old>
    <new>
      <node id="200" version="2" timestamp="2025-01-14T11:00:00Z"
            uid="201" user="editor" changeset="2001"
            lon="3.001" lat="4.001">
        <tag k="name" v="New Name"/>
        <tag k="shop" v="bakery"/>
      </node>
    </new>
  </action>
  <action type="delete">
    <old>
      <node id="300" version="5" lon="5.0" lat="6.0"
            user="mapperA" uid="300" timestamp="2024-12-01T08:00:00Z" changeset="3000">
        <tag k="name" v="Deleted Place"/>
      </node>
    </old>
    <new>
      <node id="300" version="6" timestamp="2025-01-14T12:00:00Z"
            uid="301" user="deleter" changeset="3001"
            lon="5.0" lat="6.0" visible="false"/>
    </new>
  </action>
  <action type="modify">
    <old>
      <way id="400" version="1" user="wayMaker" uid="400"
           timestamp="2024-01-01T00:00:00Z" changeset="4000">
        <bounds minlat="50.0" minlon="3.0" maxlat="50.1" maxlon="3.1"/>
        <nd ref="1001" lon="3.0" lat="50.0"/>
        <nd ref="1002" lon="3.1" lat="50.0"/>
        <nd ref="1003" lon="3.1" lat="50.1"/>
        <tag k="highway" v="residential"/>
        <tag k="name" v="Old Street"/>
      </way>
    </old>
    <new>
      <way id="400" version="2" timestamp="2025-01-14T13:00:00Z"
           uid="401" user="wayEditor" changeset="4001">
        <bounds minlat="50.0" minlon="3.0" maxlat="50.2" maxlon="3.1"/>
        <nd ref="1001" lon="3.0" lat="50.0"/>
        <nd ref="1002" lon="3.1" lat="50.0"/>
        <nd ref="1004" lon="3.1" lat="50.2"/>
        <tag k="highway" v="tertiary"/>
        <tag k="name" v="New Street"/>
      </way>
    </new>
  </action>
  <action type="create">
    <new>
      <way id="500" version="1" timestamp="2025-01-14T14:00:00Z"
           uid="500" user="builder" changeset="5000">
        <bounds minlat="51.0" minlon="3.0" maxlat="51.1" maxlon="3.1"/>
        <nd ref="2001" lon="3.0" lat="51.0"/>
        <nd ref="2002" lon="3.1" lat="51.0"/>
        <nd ref="2003" lon="3.1" lat="51.1"/>
        <nd ref="2004" lon="3.0" lat="51.1"/>
        <nd ref="2001" lon="3.0" lat="51.0"/>
        <tag k="building" v="yes"/>
      </way>
    </new>
  </action>
</osm>
```

- [ ] **Step 2: Write failing tests for all action types**

Rewrite `tests/test_adiff_parser.py`:

```python
import json
from io import BytesIO
from pathlib import Path

from daemon.adiff_parser import parse_adiff

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_create_node():
    features = parse_adiff(str(FIXTURES / "sample_all_actions.xml"))
    creates = [f for f in features if f["properties"]["action"] == "create"]
    nodes = [f for f in creates if f["properties"]["osm_type"] == "node"]
    assert len(nodes) == 1

    f = nodes[0]
    assert f["geometry"]["type"] == "Point"
    assert f["geometry"]["coordinates"] == [1.0, 2.0]
    assert f["properties"]["osm_id"] == 100
    assert f["properties"]["version"] == 1
    assert f["properties"]["user"] == "creator"
    assert f["properties"]["uid"] == 100
    assert f["properties"]["changeset"] == 1000
    assert f["properties"]["timestamp"] == "2025-01-14T10:00:00Z"
    assert f["properties"]["tags"] == {"name": "New Cafe", "amenity": "cafe"}
    assert f["properties"]["old_tags"] is None
    assert f["properties"]["old_geometry"] is None


def test_parse_modify_node():
    features = parse_adiff(str(FIXTURES / "sample_all_actions.xml"))
    modifies = [f for f in features if f["properties"]["action"] == "modify"]
    nodes = [f for f in modifies if f["properties"]["osm_type"] == "node"]
    assert len(nodes) == 1

    f = nodes[0]
    assert f["geometry"]["type"] == "Point"
    assert f["geometry"]["coordinates"] == [3.001, 4.001]
    assert f["properties"]["osm_id"] == 200
    assert f["properties"]["version"] == 2
    assert f["properties"]["user"] == "editor"
    assert f["properties"]["tags"] == {"name": "New Name", "shop": "bakery"}
    assert f["properties"]["old_tags"] == {"name": "Old Name", "shop": "bakery"}
    assert f["properties"]["old_geometry"]["type"] == "Point"
    assert f["properties"]["old_geometry"]["coordinates"] == [3.0, 4.0]


def test_parse_delete_node():
    features = parse_adiff(str(FIXTURES / "sample_all_actions.xml"))
    deletes = [f for f in features if f["properties"]["action"] == "delete"]
    nodes = [f for f in deletes if f["properties"]["osm_type"] == "node"]
    assert len(nodes) == 1

    f = nodes[0]
    assert f["geometry"]["type"] == "Point"
    assert f["geometry"]["coordinates"] == [5.0, 6.0]
    assert f["properties"]["osm_id"] == 300
    assert f["properties"]["version"] == 5
    assert f["properties"]["user"] == "deleter"
    assert f["properties"]["uid"] == 301
    assert f["properties"]["changeset"] == 3001
    assert f["properties"]["tags"] == {"name": "Deleted Place"}
    assert f["properties"]["old_tags"] is None
    assert f["properties"]["old_geometry"] is None


def test_parse_modify_way():
    features = parse_adiff(str(FIXTURES / "sample_all_actions.xml"))
    modifies = [f for f in features if f["properties"]["action"] == "modify"]
    ways = [f for f in modifies if f["properties"]["osm_type"] == "way"]
    assert len(ways) == 1

    f = ways[0]
    assert f["geometry"]["type"] == "LineString"
    assert f["geometry"]["coordinates"] == [[3.0, 50.0], [3.1, 50.0], [3.1, 50.2]]
    assert f["properties"]["tags"] == {"highway": "tertiary", "name": "New Street"}
    assert f["properties"]["old_tags"] == {"highway": "residential", "name": "Old Street"}
    assert f["properties"]["old_geometry"]["type"] == "LineString"
    assert f["properties"]["old_geometry"]["coordinates"] == [[3.0, 50.0], [3.1, 50.0], [3.1, 50.1]]


def test_parse_create_way_polygon():
    features = parse_adiff(str(FIXTURES / "sample_all_actions.xml"))
    creates = [f for f in features if f["properties"]["action"] == "create"]
    ways = [f for f in creates if f["properties"]["osm_type"] == "way"]
    assert len(ways) == 1

    f = ways[0]
    assert f["geometry"]["type"] == "Polygon"
    assert f["properties"]["tags"] == {"building": "yes"}
    assert f["properties"]["old_tags"] is None


def test_parse_empty_xml():
    source = BytesIO(b'<?xml version="1.0" encoding="UTF-8"?><osm version="0.6"></osm>')
    features = parse_adiff(source)
    assert features == []


def test_existing_delete_nodes_fixture():
    """Verify backward compat: existing delete fixtures still parse correctly."""
    features = parse_adiff(str(FIXTURES / "sample_delete_nodes.xml"))
    deletes = [f for f in features if f["properties"]["action"] == "delete"]
    assert len(deletes) == 2

    f0 = deletes[0]
    assert f0["properties"]["osm_id"] == 245053500
    assert f0["geometry"]["coordinates"] == [6.4840074, 48.5848312]

    # The modify action in sample_delete_nodes.xml should now also be captured
    modifies = [f for f in features if f["properties"]["action"] == "modify"]
    assert len(modifies) == 1
    assert modifies[0]["properties"]["osm_id"] == 999999


def test_existing_delete_ways_fixture():
    """Verify backward compat: existing delete way fixtures still parse."""
    features = parse_adiff(str(FIXTURES / "sample_delete_ways.xml"))
    assert len(features) == 2
    assert features[0]["properties"]["action"] == "delete"
    assert features[0]["geometry"]["type"] == "Polygon"
    assert features[1]["geometry"]["type"] == "LineString"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_adiff_parser.py -v`
Expected: FAIL — features don't have `action`, `old_tags`, `old_geometry` properties

- [ ] **Step 4: Rewrite adiff_parser.py to capture all actions**

Replace `daemon/adiff_parser.py` with:

```python
"""Parse augmented diff XML and extract all OSM change actions as GeoJSON features."""

import json
import xml.sax
import xml.sax.handler


def parse_adiff(source):
    """Parse augmented diff XML and yield GeoJSON features for all actions.

    Captures create, modify, and delete actions.
    Uses SAX parsing for constant memory usage regardless of file size.
    source can be a file path string, file-like object, or bytes.
    """
    handler = _AdiffHandler()
    if isinstance(source, bytes):
        import io
        source = io.BytesIO(source)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)
    parser.parse(source)
    return handler.features


class _AdiffHandler(xml.sax.handler.ContentHandler):
    """SAX handler that extracts all OSM change actions from augmented diffs."""

    def __init__(self):
        self.features = []
        self._action_type = None
        self._in_old = False
        self._in_new = False
        # Old element state
        self._old_elem_type = None
        self._old_attrs = {}
        self._old_tags = {}
        self._old_nds = []
        self._old_bounds = None
        self._old_members = []
        # New element state
        self._new_elem_type = None
        self._new_attrs = {}
        self._new_tags = {}
        self._new_nds = []
        self._new_bounds = None
        self._new_members = []

    def _reset_action(self):
        self._action_type = None
        self._in_old = False
        self._in_new = False
        self._old_elem_type = None
        self._old_attrs = {}
        self._old_tags = {}
        self._old_nds = []
        self._old_bounds = None
        self._old_members = []
        self._new_elem_type = None
        self._new_attrs = {}
        self._new_tags = {}
        self._new_nds = []
        self._new_bounds = None
        self._new_members = []

    def startElement(self, name, attrs):
        if name == "action":
            self._reset_action()
            self._action_type = attrs.get("type")
        elif name == "old":
            self._in_old = True
            self._in_new = False
        elif name == "new":
            self._in_new = True
            self._in_old = False
        elif self._action_type:
            self._handle_child_element(name, attrs)

    def _handle_child_element(self, name, attrs):
        if name in ("node", "way", "relation"):
            if self._in_old:
                self._old_elem_type = name
                self._old_attrs = dict(attrs)
            elif self._in_new:
                self._new_elem_type = name
                self._new_attrs = dict(attrs)
        elif name == "tag":
            k, v = attrs.get("k"), attrs.get("v")
            if self._in_old:
                self._old_tags[k] = v
            elif self._in_new:
                self._new_tags[k] = v
        elif name == "nd":
            if self._in_old:
                self._old_nds.append(dict(attrs))
            elif self._in_new:
                self._new_nds.append(dict(attrs))
        elif name == "bounds":
            if self._in_old:
                self._old_bounds = dict(attrs)
            elif self._in_new:
                self._new_bounds = dict(attrs)
        elif name == "member":
            if self._in_old:
                self._old_members.append(dict(attrs))
            elif self._in_new:
                self._new_members.append(dict(attrs))

    def endElement(self, name):
        if name == "old":
            self._in_old = False
        elif name == "new":
            self._in_new = False
        elif name == "action":
            if self._action_type:
                self._emit_feature()
            self._action_type = None

    def _emit_feature(self):
        if self._action_type == "create":
            self._emit_create()
        elif self._action_type == "modify":
            self._emit_modify()
        elif self._action_type == "delete":
            self._emit_delete()

    def _emit_create(self):
        elem_type = self._new_elem_type
        if not elem_type:
            return
        geometry = self._build_geometry(elem_type, self._new_attrs, self._new_nds, self._new_bounds, self._new_tags)
        if geometry is None:
            return
        self.features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "action": "create",
                "osm_type": elem_type,
                "osm_id": int(self._new_attrs.get("id", 0)),
                "version": int(self._new_attrs.get("version", 0)),
                "changeset": int(self._new_attrs.get("changeset", 0)),
                "user": self._new_attrs.get("user", ""),
                "uid": int(self._new_attrs.get("uid", 0)),
                "timestamp": self._new_attrs.get("timestamp", ""),
                "tags": self._new_tags if self._new_tags else {},
                "old_tags": None,
                "old_geometry": None,
            },
        })

    def _emit_modify(self):
        elem_type = self._new_elem_type or self._old_elem_type
        if not elem_type:
            return
        new_geometry = self._build_geometry(elem_type, self._new_attrs, self._new_nds, self._new_bounds, self._new_tags)
        old_geometry = self._build_geometry(
            self._old_elem_type or elem_type, self._old_attrs, self._old_nds, self._old_bounds, self._old_tags
        )
        if new_geometry is None:
            return
        self.features.append({
            "type": "Feature",
            "geometry": new_geometry,
            "properties": {
                "action": "modify",
                "osm_type": elem_type,
                "osm_id": int(self._new_attrs.get("id", 0)),
                "version": int(self._new_attrs.get("version", 0)),
                "changeset": int(self._new_attrs.get("changeset", 0)),
                "user": self._new_attrs.get("user", ""),
                "uid": int(self._new_attrs.get("uid", 0)),
                "timestamp": self._new_attrs.get("timestamp", ""),
                "tags": self._new_tags if self._new_tags else {},
                "old_tags": self._old_tags if self._old_tags else {},
                "old_geometry": old_geometry,
            },
        })

    def _emit_delete(self):
        elem_type = self._old_elem_type
        if not elem_type:
            return
        geometry = self._build_geometry(elem_type, self._old_attrs, self._old_nds, self._old_bounds, self._old_tags)
        if geometry is None:
            return
        # For deletes: metadata (user/changeset/timestamp) comes from <new>
        self.features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "action": "delete",
                "osm_type": elem_type,
                "osm_id": int(self._old_attrs.get("id", 0)),
                "version": int(self._old_attrs.get("version", 0)),
                "changeset": int(self._new_attrs.get("changeset", 0)),
                "user": self._new_attrs.get("user", ""),
                "uid": int(self._new_attrs.get("uid", 0)),
                "timestamp": self._new_attrs.get("timestamp", ""),
                "tags": self._old_tags if self._old_tags else {},
                "old_tags": None,
                "old_geometry": None,
            },
        })

    def _build_geometry(self, elem_type, attrs, nds, bounds, tags):
        if elem_type == "node":
            lon = attrs.get("lon")
            lat = attrs.get("lat")
            if lon is None or lat is None:
                return None
            return {"type": "Point", "coordinates": [float(lon), float(lat)]}

        elif elem_type == "way":
            if not nds:
                return None
            coords = [[float(nd.get("lon")), float(nd.get("lat"))] for nd in nds]
            if len(nds) >= 4 and nds[0].get("ref") == nds[-1].get("ref"):
                return {"type": "Polygon", "coordinates": [coords]}
            else:
                return {"type": "LineString", "coordinates": coords}

        elif elem_type == "relation":
            if bounds:
                min_lon = float(bounds.get("minlon"))
                max_lon = float(bounds.get("maxlon"))
                min_lat = float(bounds.get("minlat"))
                max_lat = float(bounds.get("maxlat"))
                center_lon = (min_lon + max_lon) / 2
                center_lat = (min_lat + max_lat) / 2
                return {"type": "Point", "coordinates": [center_lon, center_lat]}
            return None

        return None
```

Note: Multipolygon assembly for relations is deferred to Task 2 — this gets the core action types working first with the existing bounds-center approach for relations.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_adiff_parser.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add daemon/adiff_parser.py tests/test_adiff_parser.py tests/fixtures/sample_all_actions.xml
git commit -m "Capture all adiff action types (create/modify/delete) with old+new state"
```

---

### Task 2: Add multipolygon assembly for relations

**Files:**
- Modify: `daemon/adiff_parser.py`
- Create: `tests/fixtures/sample_relation_multipolygon.xml`
- Modify: `tests/test_adiff_parser.py`

- [ ] **Step 1: Create fixture with a multipolygon relation**

Create `tests/fixtures/sample_relation_multipolygon.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <action type="modify">
    <old>
      <relation id="600" version="1" user="relMapper" uid="600"
                timestamp="2024-01-01T00:00:00Z" changeset="6000">
        <bounds minlat="50.0" minlon="3.0" maxlat="50.2" maxlon="3.2"/>
        <member type="way" ref="601" role="outer">
          <nd lon="3.0" lat="50.0"/>
          <nd lon="3.2" lat="50.0"/>
          <nd lon="3.2" lat="50.2"/>
          <nd lon="3.0" lat="50.2"/>
          <nd lon="3.0" lat="50.0"/>
        </member>
        <member type="way" ref="602" role="inner">
          <nd lon="3.05" lat="50.05"/>
          <nd lon="3.15" lat="50.05"/>
          <nd lon="3.15" lat="50.15"/>
          <nd lon="3.05" lat="50.15"/>
          <nd lon="3.05" lat="50.05"/>
        </member>
        <tag k="type" v="multipolygon"/>
        <tag k="building" v="yes"/>
      </relation>
    </old>
    <new>
      <relation id="600" version="2" timestamp="2025-01-14T15:00:00Z"
                uid="601" user="relEditor" changeset="6001">
        <bounds minlat="50.0" minlon="3.0" maxlat="50.3" maxlon="3.2"/>
        <member type="way" ref="601" role="outer">
          <nd lon="3.0" lat="50.0"/>
          <nd lon="3.2" lat="50.0"/>
          <nd lon="3.2" lat="50.3"/>
          <nd lon="3.0" lat="50.3"/>
          <nd lon="3.0" lat="50.0"/>
        </member>
        <tag k="type" v="multipolygon"/>
        <tag k="building" v="yes"/>
        <tag k="name" v="Big Building"/>
      </relation>
    </new>
  </action>
  <action type="delete">
    <old>
      <relation id="700" version="3" user="relMapper2" uid="700"
                timestamp="2024-06-01T00:00:00Z" changeset="7000">
        <bounds minlat="51.0" minlon="4.0" maxlat="51.1" maxlon="4.1"/>
        <member type="way" ref="701" role="outer">
          <nd lon="4.0" lat="51.0"/>
          <nd lon="4.1" lat="51.0"/>
          <nd lon="4.1" lat="51.1"/>
          <nd lon="4.0" lat="51.1"/>
          <nd lon="4.0" lat="51.0"/>
        </member>
        <tag k="type" v="boundary"/>
        <tag k="boundary" v="administrative"/>
      </relation>
    </old>
    <new>
      <relation id="700" version="4" timestamp="2025-01-14T16:00:00Z"
                uid="701" user="relDeleter" changeset="7001" visible="false"/>
    </new>
  </action>
  <action type="create">
    <new>
      <relation id="800" version="1" timestamp="2025-01-14T17:00:00Z"
                uid="800" user="nonMPCreator" changeset="8000">
        <bounds minlat="52.0" minlon="5.0" maxlat="52.1" maxlon="5.1"/>
        <member type="way" ref="801" role=""/>
        <tag k="type" v="route"/>
        <tag k="route" v="bus"/>
      </relation>
    </new>
  </action>
</osm>
```

- [ ] **Step 2: Write failing tests for multipolygon relations**

Add to `tests/test_adiff_parser.py`:

```python
def test_parse_modify_multipolygon_relation():
    features = parse_adiff(str(FIXTURES / "sample_relation_multipolygon.xml"))
    modifies = [f for f in features if f["properties"]["action"] == "modify"]
    assert len(modifies) == 1

    f = modifies[0]
    assert f["properties"]["osm_type"] == "relation"
    assert f["properties"]["osm_id"] == 600
    # New geometry: multipolygon with one outer ring, no inner (inner was removed)
    assert f["geometry"]["type"] == "MultiPolygon"
    assert len(f["geometry"]["coordinates"]) == 1  # one polygon
    outer = f["geometry"]["coordinates"][0][0]  # first polygon, outer ring
    assert outer[0] == [3.0, 50.0]
    assert outer[-1] == [3.0, 50.0]  # closed

    # Old geometry: multipolygon with outer + inner ring
    assert f["properties"]["old_geometry"]["type"] == "MultiPolygon"
    old_polys = f["properties"]["old_geometry"]["coordinates"]
    assert len(old_polys) == 1  # one polygon with a hole
    assert len(old_polys[0]) == 2  # outer + inner ring


def test_parse_delete_boundary_relation():
    features = parse_adiff(str(FIXTURES / "sample_relation_multipolygon.xml"))
    deletes = [f for f in features if f["properties"]["action"] == "delete"]
    assert len(deletes) == 1

    f = deletes[0]
    assert f["properties"]["osm_id"] == 700
    assert f["geometry"]["type"] == "MultiPolygon"
    assert f["properties"]["tags"]["boundary"] == "administrative"


def test_parse_non_multipolygon_relation_uses_bounds_center():
    features = parse_adiff(str(FIXTURES / "sample_relation_multipolygon.xml"))
    creates = [f for f in features if f["properties"]["action"] == "create"]
    assert len(creates) == 1

    f = creates[0]
    assert f["properties"]["osm_id"] == 800
    assert f["geometry"]["type"] == "Point"
    # Center of bounds: (5.0+5.1)/2, (52.0+52.1)/2
    assert f["geometry"]["coordinates"] == [5.05, 52.05]
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_adiff_parser.py::test_parse_modify_multipolygon_relation tests/test_adiff_parser.py::test_parse_delete_boundary_relation tests/test_adiff_parser.py::test_parse_non_multipolygon_relation_uses_bounds_center -v`
Expected: FAIL — relations currently produce Point geometry, not MultiPolygon

- [ ] **Step 4: Update parser to handle member way nds and build multipolygons**

The SAX handler needs to track `<nd>` elements that appear inside `<member>` elements. The adiff XML nests them like:

```xml
<member type="way" ref="601" role="outer">
  <nd lon="3.0" lat="50.0"/>
  ...
</member>
```

Update `_AdiffHandler` in `daemon/adiff_parser.py`:

Add tracking for current member context. Replace the `_handle_child_element` method and update `_build_geometry`:

```python
# Add to __init__ and _reset_action:
self._current_member = None  # track when we're inside a <member>

# In _handle_child_element, update the member and nd handling:
def _handle_child_element(self, name, attrs):
    if name in ("node", "way", "relation"):
        if self._in_old:
            self._old_elem_type = name
            self._old_attrs = dict(attrs)
        elif self._in_new:
            self._new_elem_type = name
            self._new_attrs = dict(attrs)
    elif name == "tag":
        k, v = attrs.get("k"), attrs.get("v")
        if self._in_old:
            self._old_tags[k] = v
        elif self._in_new:
            self._new_tags[k] = v
    elif name == "member":
        member = dict(attrs)
        member["nds"] = []
        self._current_member = member
        if self._in_old:
            self._old_members.append(member)
        elif self._in_new:
            self._new_members.append(member)
    elif name == "nd":
        if self._current_member is not None:
            self._current_member["nds"].append(dict(attrs))
        elif self._in_old:
            self._old_nds.append(dict(attrs))
        elif self._in_new:
            self._new_nds.append(dict(attrs))
    elif name == "bounds":
        if self._in_old:
            self._old_bounds = dict(attrs)
        elif self._in_new:
            self._new_bounds = dict(attrs)

# Add endElement handling for member:
def endElement(self, name):
    if name == "old":
        self._in_old = False
    elif name == "new":
        self._in_new = False
    elif name == "member":
        self._current_member = None
    elif name == "action":
        if self._action_type:
            self._emit_feature()
        self._action_type = None
```

Update `_build_geometry` for relation handling:

```python
elif elem_type == "relation":
    rel_type = tags.get("type", "")
    if rel_type in ("multipolygon", "boundary"):
        return self._build_multipolygon(members)
    if bounds:
        min_lon = float(bounds.get("minlon"))
        max_lon = float(bounds.get("maxlon"))
        min_lat = float(bounds.get("minlat"))
        max_lat = float(bounds.get("maxlat"))
        center_lon = (min_lon + max_lon) / 2
        center_lat = (min_lat + max_lat) / 2
        return {"type": "Point", "coordinates": [center_lon, center_lat]}
    return None
```

Add `_build_multipolygon` method and update `_build_geometry` signature to accept `members` and `tags`:

```python
def _build_geometry(self, elem_type, attrs, nds, bounds, tags, members=None):
    # ... existing node/way logic unchanged ...
    
    elif elem_type == "relation":
        rel_type = tags.get("type", "")
        if rel_type in ("multipolygon", "boundary") and members:
            mp = self._build_multipolygon(members)
            if mp:
                return mp
        # Fallback to bounds center
        if bounds:
            min_lon = float(bounds.get("minlon"))
            max_lon = float(bounds.get("maxlon"))
            min_lat = float(bounds.get("minlat"))
            max_lat = float(bounds.get("maxlat"))
            return {"type": "Point", "coordinates": [(min_lon + max_lon) / 2, (min_lat + max_lat) / 2]}
        return None

def _build_multipolygon(self, members):
    """Build a MultiPolygon GeoJSON geometry from relation members."""
    outers = []
    inners = []
    for member in members:
        if member.get("type") != "way":
            continue
        nds = member.get("nds", [])
        if not nds:
            continue
        coords = [[float(nd["lon"]), float(nd["lat"])] for nd in nds]
        role = member.get("role", "outer")
        if role == "inner":
            inners.append(coords)
        else:
            outers.append(coords)
    
    if not outers:
        return None
    
    # Simple approach: assign inners to the first outer that contains them
    # For most OSM multipolygons this works correctly
    polygons = []
    for outer in outers:
        polygon = [outer]
        polygons.append(polygon)
    
    # Assign inner rings to polygons (simple: all to first polygon)
    if inners and polygons:
        for inner in inners:
            polygons[0].append(inner)
    
    return {"type": "MultiPolygon", "coordinates": polygons}
```

Update all callers of `_build_geometry` in `_emit_create`, `_emit_modify`, `_emit_delete` to pass `members` and `tags`:

```python
# In _emit_create:
geometry = self._build_geometry(elem_type, self._new_attrs, self._new_nds, self._new_bounds, self._new_tags, self._new_members)

# In _emit_modify:
new_geometry = self._build_geometry(elem_type, self._new_attrs, self._new_nds, self._new_bounds, self._new_tags, self._new_members)
old_geometry = self._build_geometry(
    self._old_elem_type or elem_type, self._old_attrs, self._old_nds, self._old_bounds, self._old_tags, self._old_members
)

# In _emit_delete:
geometry = self._build_geometry(elem_type, self._old_attrs, self._old_nds, self._old_bounds, self._old_tags, self._old_members)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_adiff_parser.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add daemon/adiff_parser.py tests/test_adiff_parser.py tests/fixtures/sample_relation_multipolygon.xml
git commit -m "Add multipolygon assembly for boundary/multipolygon relations"
```

---

### Task 3: Update geojson_writer and watcher for all action types

**Files:**
- Modify: `daemon/watcher.py`
- Modify: `tests/test_watcher.py`
- Modify: `tests/test_geojson_writer.py`

- [ ] **Step 1: Write updated test for watcher**

Update `tests/test_watcher.py` — the existing SAMPLE_ADIFF has a delete action. The test should verify the count reflects all actions. Update the sample to include a modify too, and update assertions:

```python
import json
from io import BytesIO
from pathlib import Path
from unittest.mock import patch, MagicMock

from daemon.watcher import Watcher


SAMPLE_ADIFF = b"""<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <action type="delete">
    <old>
      <node id="12345" version="2" lon="1.0" lat="2.0">
        <tag k="name" v="Deleted Node"/>
      </node>
    </old>
    <new>
      <node id="12345" version="3" timestamp="2025-01-14T14:51:43Z"
            uid="100" user="deleter" changeset="999"
            lat="2.0" lon="1.0" visible="false"/>
    </new>
  </action>
  <action type="modify">
    <old>
      <node id="67890" version="1" lon="3.0" lat="4.0">
        <tag k="name" v="Old Name"/>
      </node>
    </old>
    <new>
      <node id="67890" version="2" timestamp="2025-01-14T15:00:00Z"
            uid="200" user="editor" changeset="1000"
            lon="3.1" lat="4.1">
        <tag k="name" v="New Name"/>
      </node>
    </new>
  </action>
</osm>"""


def test_get_latest_sequence():
    mock_response = MagicMock()
    mock_response.text = (
        "#Sat Jan 14 14:51:37 UTC 2025\n"
        "sequenceNumber=6429815\n"
        "timestamp=2025-01-14T14\\:50\\:58Z\n"
    )
    mock_response.raise_for_status = MagicMock()
    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=Path("/tmp/test"))
        seq = watcher.get_latest_sequence()
        assert seq == 6429815


def test_save_and_load_state(tmp_path):
    watcher = Watcher(data_dir=tmp_path)
    watcher.save_state(6429815)
    assert watcher.load_state() == 6429815


def test_fetch_and_process(tmp_path):
    mock_response = MagicMock()
    mock_response.raw = BytesIO(SAMPLE_ADIFF)
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        count = watcher.fetch_and_process(6429815)
        assert count == 2  # 1 delete + 1 modify

    geojsonl_files = list((tmp_path / "deletions").glob("*.geojsonl"))
    assert len(geojsonl_files) == 1
    lines = geojsonl_files[0].read_text().strip().split("\n")
    assert len(lines) == 2
    features = [json.loads(line) for line in lines]
    actions = {f["properties"]["action"] for f in features}
    assert actions == {"delete", "modify"}


def test_fetch_404_returns_none(tmp_path):
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        result = watcher.fetch_and_process(9999999999)
        assert result is None
```

- [ ] **Step 2: Update watcher.py log message**

In `daemon/watcher.py`, line 10, the import is already correct. The only change needed is updating the docstring and making sure the count message is generic. Change line 47's docstring from "extract deletions" to "extract changes":

```python
def fetch_and_process(self, seq: int) -> int | None:
    """Fetch one adiff by sequence number, extract all changes.

    Returns the number of changes found, or None if the adiff
    is not yet available (404).
    """
```

- [ ] **Step 3: Update geojson_writer test to use new property schema**

Update `tests/test_geojson_writer.py`:

```python
import json
from pathlib import Path

from daemon.geojson_writer import GeoJSONWriter


def _make_feature(osm_id=1, osm_type="node", lon=0.0, lat=0.0, action="create"):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "action": action,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "version": 1,
            "changeset": 1,
            "user": "testuser",
            "uid": 1,
            "timestamp": "2025-01-14T14:51:43Z",
            "tags": {},
            "old_tags": None,
            "old_geometry": None,
        },
    }


def test_append_creates_daily_file(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    feature = _make_feature()
    writer.append(feature, date_str="2025-01-14")

    daily_file = tmp_path / "2025-01-14.geojsonl"
    assert daily_file.exists()

    lines = daily_file.read_text().strip().split("\n")
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["properties"]["osm_id"] == 1
    assert parsed["properties"]["action"] == "create"


def test_append_multiple_features(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(osm_id=1), date_str="2025-01-14")
    writer.append(_make_feature(osm_id=2), date_str="2025-01-14")

    daily_file = tmp_path / "2025-01-14.geojsonl"
    lines = daily_file.read_text().strip().split("\n")
    assert len(lines) == 2


def test_list_daily_files(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(), date_str="2025-01-13")
    writer.append(_make_feature(), date_str="2025-01-14")

    files = writer.list_daily_files()
    assert len(files) == 2
    assert "2025-01-13" in files
    assert "2025-01-14" in files
```

- [ ] **Step 4: Run all tests**

Run: `uv run pytest tests/test_watcher.py tests/test_geojson_writer.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add daemon/watcher.py tests/test_watcher.py tests/test_geojson_writer.py
git commit -m "Update watcher and writer tests for all-action schema"
```

---

### Task 4: Create build_parquet module

**Files:**
- Create: `pipeline/build_parquet.py`
- Create: `tests/test_build_parquet.py`

- [ ] **Step 1: Add dependencies to pyproject.toml**

Update `pyproject.toml`:

```toml
[project]
name = "osm-undelete"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "requests",
    "boto3",
    "python-dotenv",
    "geopandas",
    "pyarrow",
    "shapely",
]

[dependency-groups]
dev = [
    "pytest",
]
```

- [ ] **Step 2: Run `uv sync`**

Run: `uv sync`
Expected: Successfully installs geopandas, pyarrow, shapely

- [ ] **Step 3: Write failing test for ParquetBuilder**

Create `tests/test_build_parquet.py`:

```python
import json
from pathlib import Path

import geopandas as gpd
import pyarrow.parquet as pq

from pipeline.build_parquet import ParquetBuilder


def _write_geojsonl(path: Path, features: list[dict]):
    with open(path, "w") as f:
        for feat in features:
            f.write(json.dumps(feat) + "\n")


def _make_feature(osm_id=1, action="create", osm_type="node", lon=1.0, lat=2.0):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "action": action,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "version": 1,
            "changeset": 100,
            "user": "testuser",
            "uid": 42,
            "timestamp": "2025-01-14T10:00:00Z",
            "tags": {"name": "Test"},
            "old_tags": None,
            "old_geometry": None,
        },
    }


def test_build_parquet_from_geojsonl(tmp_path):
    geojsonl_dir = tmp_path / "deletions"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"

    features = [
        _make_feature(osm_id=1, action="create", lon=1.0, lat=2.0),
        _make_feature(osm_id=2, action="modify", lon=3.0, lat=4.0),
        _make_feature(osm_id=3, action="delete", lon=5.0, lat=6.0),
    ]
    _write_geojsonl(geojsonl_dir / "2025-01-14.geojsonl", features)

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    built = builder.build("2025-01-14")

    assert built is True
    parquet_file = parquet_dir / "date=2025-01-14" / "data.parquet"
    assert parquet_file.exists()

    # Read back and verify
    gdf = gpd.read_parquet(parquet_file)
    assert len(gdf) == 3
    assert set(gdf["action"]) == {"create", "modify", "delete"}
    assert set(gdf["osm_id"]) == {1, 2, 3}
    assert gdf.geometry.name == "geometry"
    # Verify it's valid GeoParquet with WKB geometry
    assert all(gdf.geometry.geom_type == "Point")


def test_build_skips_if_unchanged(tmp_path):
    geojsonl_dir = tmp_path / "deletions"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"

    _write_geojsonl(
        geojsonl_dir / "2025-01-14.geojsonl",
        [_make_feature(osm_id=1)],
    )

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    assert builder.build("2025-01-14") is True
    assert builder.build("2025-01-14") is False  # no change


def test_build_nonexistent_date(tmp_path):
    geojsonl_dir = tmp_path / "deletions"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    assert builder.build("2025-01-14") is False


def test_old_geometry_preserved(tmp_path):
    geojsonl_dir = tmp_path / "deletions"
    geojsonl_dir.mkdir()
    parquet_dir = tmp_path / "parquet"

    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [3.0, 4.0]},
        "properties": {
            "action": "modify",
            "osm_type": "node",
            "osm_id": 10,
            "version": 2,
            "changeset": 200,
            "user": "editor",
            "uid": 50,
            "timestamp": "2025-01-14T11:00:00Z",
            "tags": {"name": "New"},
            "old_tags": {"name": "Old"},
            "old_geometry": {"type": "Point", "coordinates": [1.0, 2.0]},
        },
    }
    _write_geojsonl(geojsonl_dir / "2025-01-14.geojsonl", [feature])

    builder = ParquetBuilder(geojsonl_dir, parquet_dir)
    builder.build("2025-01-14")

    gdf = gpd.read_parquet(parquet_dir / "date=2025-01-14" / "data.parquet")
    assert len(gdf) == 1
    row = gdf.iloc[0]
    assert row["old_tags"] == '{"name": "Old"}'
    assert row["tags"] == '{"name": "New"}'
    # old_geometry stored as WKB in a separate column
    assert row["old_geometry"] is not None
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/test_build_parquet.py -v`
Expected: FAIL — `pipeline.build_parquet` module doesn't exist

- [ ] **Step 5: Implement build_parquet.py**

Create `pipeline/build_parquet.py`:

```python
"""Convert daily GeoJSON-lines files to GeoParquet."""

import json
import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely import wkb
from shapely.geometry import shape

logger = logging.getLogger(__name__)


class ParquetBuilder:
    def __init__(self, geojsonl_dir: Path, parquet_dir: Path):
        self.geojsonl_dir = Path(geojsonl_dir)
        self.parquet_dir = Path(parquet_dir)
        self._last_mtime: dict[str, float] = {}

    def build(self, date_str: str) -> bool:
        """Convert a daily GeoJSON-lines file to GeoParquet.

        Returns True if a new Parquet file was written, False if skipped.
        """
        geojsonl_file = self.geojsonl_dir / f"{date_str}.geojsonl"
        if not geojsonl_file.exists():
            return False

        mtime = geojsonl_file.stat().st_mtime
        if self._last_mtime.get(date_str) == mtime:
            return False

        output_dir = self.parquet_dir / f"date={date_str}"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "data.parquet"

        records = []
        geometries = []
        old_geometries = []

        with open(geojsonl_file) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                feature = json.loads(line)
                props = feature["properties"]
                geom = shape(feature["geometry"])
                geometries.append(geom)

                old_geom = props.get("old_geometry")
                if old_geom:
                    old_geometries.append(wkb.dumps(shape(old_geom)))
                else:
                    old_geometries.append(None)

                tags = props.get("tags")
                old_tags = props.get("old_tags")

                records.append({
                    "action": props["action"],
                    "osm_type": props["osm_type"],
                    "osm_id": props["osm_id"],
                    "version": props["version"],
                    "changeset": props["changeset"],
                    "user": props["user"],
                    "uid": props["uid"],
                    "timestamp": props["timestamp"],
                    "tags": json.dumps(tags) if tags else "{}",
                    "old_tags": json.dumps(old_tags) if old_tags else None,
                    "old_geometry": None,  # placeholder, set below
                })

        if not records:
            return False

        gdf = gpd.GeoDataFrame(records, geometry=geometries, crs="EPSG:4326")
        gdf["old_geometry"] = old_geometries

        gdf.to_parquet(output_file)
        self._last_mtime[date_str] = mtime
        logger.info("Built %s (%d features)", output_file, len(gdf))
        return True
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_build_parquet.py -v`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add pipeline/build_parquet.py tests/test_build_parquet.py pyproject.toml
git commit -m "Add GeoParquet builder to convert daily GeoJSONL to Parquet"
```

---

### Task 5: Update prune.py for Hive-partitioned Parquet

**Files:**
- Modify: `pipeline/prune.py`
- Modify: `tests/test_prune.py`

- [ ] **Step 1: Read current prune test**

Current `tests/test_prune.py`:

```python
# Read the file to confirm exact contents before modifying
```

- [ ] **Step 2: Write updated test**

Replace `tests/test_prune.py`:

```python
from datetime import date
from pathlib import Path

from pipeline.prune import prune_old_files


def test_prune_removes_old_geojsonl(tmp_path):
    old_file = tmp_path / "2025-01-01.geojsonl"
    new_file = tmp_path / "2025-01-20.geojsonl"
    old_file.touch()
    new_file.touch()

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 21))
    assert old_file in pruned
    assert not old_file.exists()
    assert new_file.exists()


def test_prune_removes_old_parquet_dirs(tmp_path):
    old_dir = tmp_path / "date=2025-01-01"
    old_dir.mkdir()
    (old_dir / "data.parquet").touch()

    new_dir = tmp_path / "date=2025-01-20"
    new_dir.mkdir()
    (new_dir / "data.parquet").touch()

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 21))
    pruned_paths = [p for p in pruned]
    assert old_dir in pruned_paths
    assert not old_dir.exists()
    assert new_dir.exists()


def test_prune_keeps_non_date_files(tmp_path):
    metadata = tmp_path / "metadata.json"
    metadata.touch()

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 21))
    assert pruned == []
    assert metadata.exists()
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/test_prune.py -v`
Expected: FAIL — prune doesn't handle `date=YYYY-MM-DD` directories

- [ ] **Step 4: Update prune.py**

Replace `pipeline/prune.py`:

```python
"""Prune old daily GeoJSON and Parquet files past retention."""

import logging
import shutil
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def prune_old_files(directory: Path, retention_days: int, today: date | None = None) -> list[Path]:
    """Delete files/dirs older than retention_days. Returns list of deleted paths."""
    if today is None:
        today = date.today()

    cutoff = today - timedelta(days=retention_days)
    pruned = []

    for f in sorted(directory.iterdir()):
        # Handle .geojsonl files (date is the stem)
        if f.is_file() and f.suffix in (".geojsonl", ".pmtiles"):
            try:
                file_date = date.fromisoformat(f.stem)
            except ValueError:
                continue
            if file_date < cutoff:
                logger.info("Pruning old file: %s", f.name)
                f.unlink()
                pruned.append(f)

        # Handle Hive-partitioned dirs like date=2025-01-14
        elif f.is_dir() and f.name.startswith("date="):
            try:
                dir_date = date.fromisoformat(f.name.split("=", 1)[1])
            except ValueError:
                continue
            if dir_date < cutoff:
                logger.info("Pruning old partition: %s", f.name)
                shutil.rmtree(f)
                pruned.append(f)

    return pruned
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_prune.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add pipeline/prune.py tests/test_prune.py
git commit -m "Update pruning to handle Hive-partitioned Parquet directories"
```

---

### Task 6: Update main.py orchestration

**Files:**
- Modify: `main.py`
- Remove: `pipeline/build_tiles.py`
- Remove: `tests/test_build_tiles.py`

- [ ] **Step 1: Rewrite main.py**

Replace `main.py`:

```python
"""OSM Changes — main entry point.

Runs the watcher daemon with periodic Parquet builds and R2 uploads.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from daemon.watcher import Watcher
from pipeline.build_parquet import ParquetBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

POLL_INTERVAL = 60  # seconds between checks for new sequences


def write_and_upload_metadata(data_dir: Path, uploader: R2Uploader | None, r2_public_url: str):
    """Write metadata.json with available date range and upload it."""
    parquet_dir = data_dir / "parquet"
    date_dirs = sorted(
        d.name.split("=", 1)[1]
        for d in parquet_dir.iterdir()
        if d.is_dir() and d.name.startswith("date=")
    )
    if not date_dirs:
        return

    metadata = {
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base_url": r2_public_url,
        "min_date": date_dirs[0],
        "max_date": date_dirs[-1],
        "dates": date_dirs,
    }
    metadata_path = parquet_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata))
    if uploader:
        uploader.upload_file(metadata_path, "osm-changes/metadata.json")
    logger.info("Wrote metadata: %s to %s (%d dates)", date_dirs[0], date_dirs[-1], len(date_dirs))


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))

    watcher = Watcher(data_dir)
    parquet_builder = ParquetBuilder(data_dir / "deletions", data_dir / "parquet")

    # R2 uploader (optional)
    uploader = None
    r2_endpoint = os.environ.get("R2_ENDPOINT_URL")
    r2_public_url = os.environ.get("R2_PUBLIC_URL", "")
    if r2_endpoint:
        uploader = R2Uploader(
            endpoint_url=r2_endpoint,
            access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            bucket_name=os.environ["R2_BUCKET_NAME"],
        )
        logger.info("R2 upload enabled: %s/%s", r2_endpoint, os.environ["R2_BUCKET_NAME"])
    else:
        logger.info("R2 upload disabled (R2_ENDPOINT_URL not set)")

    parquet_build_interval = int(os.environ.get("PARQUET_BUILD_INTERVAL", "300"))
    retention_days = int(os.environ.get("RETENTION_DAYS", "90"))

    # Determine starting sequence
    last_seq = watcher.load_state()
    if last_seq is None:
        last_seq = watcher.get_latest_sequence()
        logger.info("No saved state, starting from latest sequence: %d", last_seq)
    else:
        logger.info("Resuming from saved sequence: %d", last_seq)

    last_parquet_build = 0.0

    logger.info("Starting watcher daemon (poll=%ds, parquet_build=%ds)",
                POLL_INTERVAL, parquet_build_interval)

    next_poll = 0.0

    while True:
        now = time.time()
        sleep_for = next_poll - now
        if sleep_for > 0:
            logger.debug("Sleeping %.1fs until next poll", sleep_for)
            time.sleep(sleep_for)
        now = time.time()
        next_poll = now + POLL_INTERVAL
        logger.debug("Loop tick: last_seq=%d", last_seq)

        # Poll for new adiffs
        try:
            logger.debug("Fetching latest sequence number")
            latest_seq = watcher.get_latest_sequence()
            logger.debug("Latest sequence: %d", latest_seq)
        except Exception:
            logger.exception("Failed to get latest sequence")
            latest_seq = last_seq

        while last_seq < latest_seq:
            next_seq = last_seq + 1
            try:
                logger.debug("Fetching and processing seq %d (latest=%d)", next_seq, latest_seq)
                count = watcher.fetch_and_process(next_seq)
                if count is None:
                    logger.debug("Seq %d not yet available (404), will retry", next_seq)
                    break
                logger.debug("Processed seq %d: %d changes", next_seq, count)
                if count > 0:
                    logger.info("Seq %d: %d changes", next_seq, count)
                last_seq = next_seq
                watcher.save_state(last_seq)
            except Exception:
                logger.exception("Failed to process seq %d", next_seq)
                break

        # Periodic: build Parquet and upload
        if (now - last_parquet_build) >= parquet_build_interval:
            last_parquet_build = now
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                # Build today's parquet
                built = parquet_builder.build(today_str)
                if built:
                    logger.info("Built parquet for %s", today_str)
                    if uploader:
                        parquet_file = data_dir / "parquet" / f"date={today_str}" / "data.parquet"
                        uploader.upload_file(parquet_file, f"osm-changes/date={today_str}/data.parquet")

                # Build any older days that haven't been converted yet
                for geojsonl in sorted((data_dir / "deletions").glob("*.geojsonl")):
                    date_str = geojsonl.stem
                    if date_str == today_str:
                        continue
                    if parquet_builder.build(date_str):
                        logger.info("Built parquet for %s", date_str)
                        if uploader:
                            pf = data_dir / "parquet" / f"date={date_str}" / "data.parquet"
                            uploader.upload_file(pf, f"osm-changes/date={date_str}/data.parquet")
                        # Clean up geojsonl for past days that have been converted
                        geojsonl.unlink()
                        logger.info("Cleaned up %s", geojsonl.name)

                # Write and upload metadata
                write_and_upload_metadata(data_dir, uploader, r2_public_url)

                # Prune old data
                prune_old_files(data_dir / "parquet", retention_days)
                prune_old_files(data_dir / "deletions", retention_days)
            except Exception:
                logger.exception("Failed to build/upload parquet")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
```

- [ ] **Step 2: Delete old tile-related files**

```bash
rm pipeline/build_tiles.py tests/test_build_tiles.py
```

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests PASS (test_build_tiles.py is gone, test_merge_upload.py still works as R2Uploader interface is unchanged)

- [ ] **Step 4: Commit**

```bash
git add main.py pyproject.toml
git rm pipeline/build_tiles.py tests/test_build_tiles.py
git commit -m "Replace tile pipeline with Parquet build and upload"
```

---

### Task 7: Update Dockerfile

**Files:**
- Modify: `Dockerfile`

- [ ] **Step 1: Rewrite Dockerfile**

Replace `Dockerfile`:

```dockerfile
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install Python dependencies
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy application code
COPY daemon/ daemon/
COPY pipeline/ pipeline/
COPY main.py .

ENV DATA_DIR=/data

VOLUME /data

CMD ["uv", "run", "python", "main.py"]
```

- [ ] **Step 2: Commit**

```bash
git add Dockerfile
git commit -m "Simplify Dockerfile: remove tippecanoe, just Python deps"
```

---

### Task 8: Build the web frontend

**Files:**
- Rewrite: `web/index.html`

- [ ] **Step 1: Write the complete frontend**

Replace `web/index.html` with a new DuckDB WASM + MapLibre interface:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OSM Changes Explorer</title>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.css">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      display: flex;
      flex-direction: column;
      height: 100vh;
      background: #f5f5f5;
    }

    /* Query bar */
    #query-bar {
      background: #1e1e2e;
      padding: 8px 12px;
      display: flex;
      gap: 8px;
      align-items: stretch;
      border-bottom: 1px solid #313244;
    }
    #query-editor {
      flex: 1;
      background: #181825;
      color: #cdd6f4;
      border: 1px solid #313244;
      border-radius: 4px;
      padding: 8px;
      font-family: "SF Mono", "Fira Code", "Cascadia Code", monospace;
      font-size: 13px;
      resize: vertical;
      min-height: 40px;
      max-height: 200px;
    }
    #query-editor:focus { outline: 1px solid #89b4fa; border-color: #89b4fa; }

    #query-controls {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    #run-btn {
      background: #a6e3a1;
      color: #1e1e2e;
      border: none;
      border-radius: 4px;
      padding: 6px 16px;
      font-weight: 600;
      font-size: 13px;
      cursor: pointer;
    }
    #run-btn:hover { background: #94e2d5; }
    #run-btn:disabled { background: #585b70; color: #a6adc8; cursor: wait; }

    #examples-btn {
      background: #313244;
      color: #cdd6f4;
      border: 1px solid #45475a;
      border-radius: 4px;
      padding: 4px 12px;
      font-size: 12px;
      cursor: pointer;
    }
    #examples-btn:hover { background: #45475a; }

    #bbox-btn {
      background: #313244;
      color: #cdd6f4;
      border: 1px solid #45475a;
      border-radius: 4px;
      padding: 4px 12px;
      font-size: 12px;
      cursor: pointer;
    }
    #bbox-btn:hover { background: #45475a; }

    /* Examples dropdown */
    #examples-menu {
      display: none;
      position: absolute;
      top: 100%;
      right: 0;
      background: #1e1e2e;
      border: 1px solid #313244;
      border-radius: 6px;
      padding: 4px 0;
      z-index: 100;
      min-width: 300px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }
    #examples-menu.open { display: block; }
    .example-item {
      padding: 8px 14px;
      color: #cdd6f4;
      font-size: 12px;
      cursor: pointer;
    }
    .example-item:hover { background: #313244; }
    .example-item .label { font-weight: 600; }
    .example-item .desc { color: #a6adc8; font-size: 11px; margin-top: 2px; }

    /* Status bar */
    #status-bar {
      background: #1e1e2e;
      color: #a6adc8;
      font-size: 12px;
      padding: 4px 12px;
      border-bottom: 1px solid #313244;
      display: flex;
      justify-content: space-between;
    }
    #status-bar .error { color: #f38ba8; }

    /* Map */
    #map-container {
      flex: 1;
      position: relative;
      min-height: 200px;
    }
    #map { width: 100%; height: 100%; }

    /* Resize handle */
    #resize-handle {
      height: 6px;
      background: #313244;
      cursor: row-resize;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    #resize-handle::after {
      content: "";
      width: 40px;
      height: 2px;
      background: #585b70;
      border-radius: 1px;
    }

    /* Results table */
    #results-container {
      height: 250px;
      overflow: auto;
      background: #1e1e2e;
    }
    #results-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    #results-table thead {
      position: sticky;
      top: 0;
      z-index: 1;
    }
    #results-table th {
      background: #313244;
      color: #cdd6f4;
      padding: 6px 10px;
      text-align: left;
      font-weight: 600;
      white-space: nowrap;
      border-bottom: 1px solid #45475a;
    }
    #results-table td {
      padding: 4px 10px;
      color: #cdd6f4;
      border-bottom: 1px solid #1e1e2e;
      white-space: nowrap;
      max-width: 300px;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    #results-table tr { background: #181825; }
    #results-table tr:hover { background: #313244; }
    #results-table tr.selected { background: #45475a; }
    #results-table td.action-create { color: #a6e3a1; }
    #results-table td.action-modify { color: #f9e2af; }
    #results-table td.action-delete { color: #f38ba8; }

    .maplibregl-popup-content {
      max-width: 350px;
      font-size: 13px;
    }
    .popup-title { font-weight: bold; margin-bottom: 4px; }
    .popup-meta { color: #666; font-size: 12px; margin-bottom: 6px; }
    .popup-meta a { color: #0078d4; }
    .tag-table { width: 100%; border-collapse: collapse; margin-bottom: 6px; }
    .tag-table td { padding: 2px 6px; border-bottom: 1px solid #eee; font-size: 12px; }
    .tag-table td:first-child { font-weight: 600; color: #444; white-space: nowrap; }

    /* Loading overlay */
    #loading {
      display: none;
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(0,0,0,0.5);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      color: white;
      font-size: 16px;
    }
    #loading.visible { display: flex; }
  </style>
</head>
<body>

<div id="query-bar">
  <textarea id="query-editor" rows="2" spellcheck="false">-- Loading DuckDB WASM...</textarea>
  <div id="query-controls">
    <button id="run-btn" disabled onclick="runQuery()">&#9654; Run</button>
    <div style="position:relative">
      <button id="examples-btn" onclick="toggleExamples()">Examples &#9662;</button>
      <div id="examples-menu"></div>
    </div>
    <button id="bbox-btn" onclick="insertBboxFilter()">&#127758; Map Bounds</button>
  </div>
</div>

<div id="status-bar">
  <span id="status-text">Initializing DuckDB WASM...</span>
  <span id="result-count"></span>
</div>

<div id="map-container">
  <div id="map"></div>
</div>

<div id="resize-handle"></div>

<div id="results-container">
  <table id="results-table">
    <thead><tr></tr></thead>
    <tbody></tbody>
  </table>
</div>

<div id="loading"><div>Running query...</div></div>

<script src="https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.js"></script>
<script type="module">
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm";

// ── Globals ──
let db = null;
let conn = null;
let metadata = null;
let currentResults = null;
let selectedRowIdx = null;
let currentPopup = null;

const map = new maplibregl.Map({
  container: "map",
  style: {
    version: 8,
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        maxzoom: 19,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      },
    },
    layers: [{ id: "osm", type: "raster", source: "osm" }],
  },
  center: [0, 30],
  zoom: 2,
});
map.addControl(new maplibregl.NavigationControl(), "top-right");

// ── Init DuckDB ──
async function initDuckDB() {
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" })
  );
  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  conn = await db.connect();
  await conn.query("INSTALL spatial; LOAD spatial;");
  await conn.query("INSTALL httpfs; LOAD httpfs;");

  setStatus("DuckDB ready. Loading metadata...");
  await loadMetadata();
}

async function loadMetadata() {
  try {
    const base = window.location.origin;
    const resp = await fetch(base + "/data/parquet/metadata.json");
    if (!resp.ok) {
      setStatus("No metadata.json found — enter a query manually");
      setDefaultQuery();
      return;
    }
    metadata = await resp.json();
    setStatus(`Ready — data from ${metadata.min_date} to ${metadata.max_date} (updated ${metadata.last_updated})`);
    buildExamples();
    setDefaultQuery();
  } catch (e) {
    setStatus("Failed to load metadata: " + e.message, true);
    setDefaultQuery();
  }
}

function getParquetUrl(datePattern) {
  if (metadata && metadata.base_url) {
    return `${metadata.base_url}/osm-changes/date=${datePattern}/data.parquet`;
  }
  // Local dev: use serve.py path
  return `${window.location.origin}/data/parquet/date=${datePattern}/data.parquet`;
}

function setDefaultQuery() {
  const today = new Date().toISOString().slice(0, 10);
  const url = getParquetUrl(today);
  const editor = document.getElementById("query-editor");
  editor.value = `SELECT action, osm_type, osm_id, version, user, timestamp, tags, geometry\nFROM read_parquet('${url}')\nLIMIT 100`;
  document.getElementById("run-btn").disabled = false;
}

// ── Examples ──
function buildExamples() {
  const menu = document.getElementById("examples-menu");
  const today = new Date().toISOString().slice(0, 10);
  const url = getParquetUrl(today);
  const allUrl = getParquetUrl("*");

  const examples = [
    {
      label: "Today's changes (sample)",
      desc: "First 100 changes from today",
      sql: `SELECT action, osm_type, osm_id, version, user, timestamp, tags, geometry\nFROM read_parquet('${url}')\nLIMIT 100`,
    },
    {
      label: "Today's deletes",
      desc: "All deletions from today",
      sql: `SELECT osm_type, osm_id, user, timestamp, tags, geometry\nFROM read_parquet('${url}')\nWHERE action = 'delete'\nLIMIT 500`,
    },
    {
      label: "Changes by user",
      desc: "Top users by change count today",
      sql: `SELECT user, action, count(*) as cnt\nFROM read_parquet('${url}')\nGROUP BY user, action\nORDER BY cnt DESC\nLIMIT 50`,
    },
    {
      label: "Deleted buildings (all time)",
      desc: "Buildings deleted across all dates",
      sql: `SELECT osm_type, osm_id, user, timestamp, tags, geometry\nFROM read_parquet('${allUrl}', hive_partitioning=true)\nWHERE action = 'delete' AND tags LIKE '%building%'\nLIMIT 500`,
    },
  ];

  menu.innerHTML = "";
  for (const ex of examples) {
    const div = document.createElement("div");
    div.className = "example-item";
    div.innerHTML = `<div class="label">${ex.label}</div><div class="desc">${ex.desc}</div>`;
    div.onclick = () => {
      document.getElementById("query-editor").value = ex.sql;
      menu.classList.remove("open");
    };
    menu.appendChild(div);
  }
}
window.toggleExamples = function() {
  document.getElementById("examples-menu").classList.toggle("open");
};

// Close menu on outside click
document.addEventListener("click", (e) => {
  if (!e.target.closest("#examples-btn") && !e.target.closest("#examples-menu")) {
    document.getElementById("examples-menu").classList.remove("open");
  }
});

// ── Bbox filter ──
window.insertBboxFilter = function() {
  const bounds = map.getBounds();
  const clause = `ST_Within(geometry, ST_MakeEnvelope(${bounds.getWest().toFixed(6)}, ${bounds.getSouth().toFixed(6)}, ${bounds.getEast().toFixed(6)}, ${bounds.getNorth().toFixed(6)}))`;
  const editor = document.getElementById("query-editor");
  const sql = editor.value;
  // If there's already a WHERE, add AND; otherwise add WHERE
  if (/WHERE/i.test(sql)) {
    // Insert before LIMIT/ORDER/GROUP if present, else at end
    const insertPoint = sql.search(/\b(LIMIT|ORDER|GROUP)\b/i);
    if (insertPoint > -1) {
      editor.value = sql.slice(0, insertPoint) + `AND ${clause}\n` + sql.slice(insertPoint);
    } else {
      editor.value = sql + `\nAND ${clause}`;
    }
  } else {
    const insertPoint = sql.search(/\b(LIMIT|ORDER|GROUP)\b/i);
    if (insertPoint > -1) {
      editor.value = sql.slice(0, insertPoint) + `WHERE ${clause}\n` + sql.slice(insertPoint);
    } else {
      editor.value = sql + `\nWHERE ${clause}`;
    }
  }
};

// ── Run query ──
window.runQuery = async function() {
  const sql = document.getElementById("query-editor").value.trim();
  if (!sql) return;

  document.getElementById("run-btn").disabled = true;
  document.getElementById("loading").classList.add("visible");
  setStatus("Running query...");

  try {
    const result = await conn.query(sql);
    const rows = result.toArray().map(r => {
      const obj = {};
      for (const field of result.schema.fields) {
        obj[field.name] = r[field.name];
      }
      return obj;
    });

    currentResults = rows;
    selectedRowIdx = null;
    renderTable(result.schema.fields.map(f => f.name), rows);
    renderMapResults(rows);
    setStatus(`Query complete`);
    document.getElementById("result-count").textContent = `${rows.length} rows`;
  } catch (e) {
    setStatus("Query error: " + e.message, true);
    document.getElementById("result-count").textContent = "";
  } finally {
    document.getElementById("run-btn").disabled = false;
    document.getElementById("loading").classList.remove("visible");
  }
};

// Ctrl+Enter to run
document.getElementById("query-editor").addEventListener("keydown", (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
    e.preventDefault();
    runQuery();
  }
});

// ── Table rendering ──
function renderTable(columns, rows) {
  const thead = document.querySelector("#results-table thead tr");
  const tbody = document.querySelector("#results-table tbody");

  // Filter out geometry columns from display
  const displayCols = columns.filter(c => c !== "geometry" && c !== "old_geometry");

  thead.innerHTML = displayCols.map(c => `<th>${escapeHtml(c)}</th>`).join("");
  tbody.innerHTML = "";

  for (let i = 0; i < rows.length; i++) {
    const tr = document.createElement("tr");
    tr.dataset.idx = i;
    for (const col of displayCols) {
      const td = document.createElement("td");
      let val = rows[i][col];
      if (col === "action") {
        td.className = `action-${val}`;
      }
      if (val === null || val === undefined) {
        td.textContent = "";
        td.style.color = "#585b70";
      } else if (typeof val === "object") {
        td.textContent = JSON.stringify(val);
      } else {
        td.textContent = String(val);
      }
      tr.appendChild(td);
    }
    tr.onclick = () => selectRow(i);
    tbody.appendChild(tr);
  }
}

function selectRow(idx) {
  // Deselect previous
  const prev = document.querySelector("#results-table tr.selected");
  if (prev) prev.classList.remove("selected");

  selectedRowIdx = idx;
  const tr = document.querySelector(`#results-table tr[data-idx="${idx}"]`);
  if (tr) tr.classList.add("selected");

  const row = currentResults[idx];
  if (row.geometry) {
    zoomToFeature(row);
  }
}

// ── Map rendering ──
function renderMapResults(rows) {
  // Remove previous layer/source
  if (map.getLayer("results-fill")) map.removeLayer("results-fill");
  if (map.getLayer("results-line")) map.removeLayer("results-line");
  if (map.getLayer("results-point")) map.removeLayer("results-point");
  if (map.getSource("results")) map.removeSource("results");
  if (currentPopup) { currentPopup.remove(); currentPopup = null; }

  const features = [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    if (!row.geometry) continue;

    let geojsonGeom;
    try {
      // geometry comes back as Uint8Array (WKB) from DuckDB
      geojsonGeom = wkbToGeoJSON(row.geometry);
    } catch {
      continue;
    }

    const props = { _idx: i };
    for (const [k, v] of Object.entries(row)) {
      if (k === "geometry" || k === "old_geometry") continue;
      props[k] = typeof v === "object" && v !== null ? JSON.stringify(v) : v;
    }
    features.push({ type: "Feature", geometry: geojsonGeom, properties: props });
  }

  if (features.length === 0) return;

  const geojson = { type: "FeatureCollection", features };
  map.addSource("results", { type: "geojson", data: geojson });

  map.addLayer({
    id: "results-fill",
    type: "fill",
    source: "results",
    filter: ["==", "$type", "Polygon"],
    paint: { "fill-color": "#89b4fa", "fill-opacity": 0.3, "fill-outline-color": "#89b4fa" },
  });
  map.addLayer({
    id: "results-line",
    type: "line",
    source: "results",
    filter: ["==", "$type", "LineString"],
    paint: { "line-color": "#89b4fa", "line-width": 2 },
  });
  map.addLayer({
    id: "results-point",
    type: "circle",
    source: "results",
    filter: ["==", "$type", "Point"],
    paint: {
      "circle-radius": 5,
      "circle-color": ["match", ["get", "action"],
        "create", "#a6e3a1",
        "modify", "#f9e2af",
        "delete", "#f38ba8",
        "#89b4fa"
      ],
      "circle-stroke-width": 1,
      "circle-stroke-color": "#fff",
    },
  });

  // Fit map to results
  const bounds = new maplibregl.LngLatBounds();
  for (const f of features) {
    const coords = extractCoords(f.geometry);
    for (const c of coords) bounds.extend(c);
  }
  if (!bounds.isEmpty()) {
    map.fitBounds(bounds, { padding: 50, maxZoom: 16 });
  }
}

// Map click handler
map.on("click", (e) => {
  const layers = ["results-point", "results-line", "results-fill"].filter(l => map.getLayer(l));
  const features = map.queryRenderedFeatures(e.point, { layers });
  if (features.length === 0) return;

  const f = features[0];
  const props = f.properties;

  let html = `<div class="popup-title">${props.osm_type || ""}/${props.osm_id || ""}</div>`;
  if (props.action) {
    html += `<div class="popup-meta">Action: <strong>${props.action}</strong>`;
    if (props.user) html += ` by <a href="https://www.openstreetmap.org/user/${encodeURIComponent(props.user)}" target="_blank">${props.user}</a>`;
    if (props.timestamp) html += ` at ${props.timestamp}`;
    html += `</div>`;
  }

  // Tags
  if (props.tags && props.tags !== "{}") {
    try {
      const tags = typeof props.tags === "string" ? JSON.parse(props.tags) : props.tags;
      const entries = Object.entries(tags);
      if (entries.length > 0) {
        html += `<table class="tag-table">`;
        for (const [k, v] of entries) {
          html += `<tr><td>${escapeHtml(k)}</td><td>${escapeHtml(String(v))}</td></tr>`;
        }
        html += `</table>`;
      }
    } catch {}
  }

  if (props.osm_type && props.osm_id) {
    html += `<div style="margin-top:6px"><a href="https://www.openstreetmap.org/${props.osm_type}/${props.osm_id}/history" target="_blank" style="color:#0078d4;font-size:12px">View history on OSM</a></div>`;
  }

  if (currentPopup) currentPopup.remove();
  currentPopup = new maplibregl.Popup().setLngLat(e.lngLat).setHTML(html).addTo(map);

  // Highlight corresponding table row
  if (props._idx !== undefined) {
    selectRow(typeof props._idx === "string" ? parseInt(props._idx) : props._idx);
    const tr = document.querySelector(`#results-table tr[data-idx="${props._idx}"]`);
    if (tr) tr.scrollIntoView({ block: "nearest" });
  }
});

// Cursor change
for (const layer of ["results-point", "results-line", "results-fill"]) {
  map.on("mouseenter", layer, () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", layer, () => { map.getCanvas().style.cursor = ""; });
}

function zoomToFeature(row) {
  if (!row.geometry) return;
  try {
    const geom = wkbToGeoJSON(row.geometry);
    const coords = extractCoords(geom);
    if (coords.length === 1) {
      map.flyTo({ center: coords[0], zoom: 16 });
    } else {
      const bounds = new maplibregl.LngLatBounds();
      for (const c of coords) bounds.extend(c);
      map.fitBounds(bounds, { padding: 50, maxZoom: 16 });
    }
  } catch {}
}

// ── WKB to GeoJSON ──
// Minimal WKB parser for Point, LineString, Polygon, MultiPolygon
function wkbToGeoJSON(wkb) {
  const buf = wkb instanceof ArrayBuffer ? wkb : wkb.buffer.slice(wkb.byteOffset, wkb.byteOffset + wkb.byteLength);
  const view = new DataView(buf);
  let offset = 0;

  function readGeometry() {
    const byteOrder = view.getUint8(offset); offset += 1;
    const le = byteOrder === 1;
    const wkbType = view.getUint32(offset, le); offset += 4;

    switch (wkbType) {
      case 1: return readPoint(le);
      case 2: return readLineString(le);
      case 3: return readPolygon(le);
      case 4: return readMultiPoint(le);
      case 5: return readMultiLineString(le);
      case 6: return readMultiPolygon(le);
      default: throw new Error("Unsupported WKB type: " + wkbType);
    }
  }

  function readPoint(le) {
    const x = view.getFloat64(offset, le); offset += 8;
    const y = view.getFloat64(offset, le); offset += 8;
    return { type: "Point", coordinates: [x, y] };
  }

  function readLineString(le) {
    const numPoints = view.getUint32(offset, le); offset += 4;
    const coords = [];
    for (let i = 0; i < numPoints; i++) {
      const x = view.getFloat64(offset, le); offset += 8;
      const y = view.getFloat64(offset, le); offset += 8;
      coords.push([x, y]);
    }
    return { type: "LineString", coordinates: coords };
  }

  function readPolygon(le) {
    const numRings = view.getUint32(offset, le); offset += 4;
    const rings = [];
    for (let r = 0; r < numRings; r++) {
      const numPoints = view.getUint32(offset, le); offset += 4;
      const ring = [];
      for (let i = 0; i < numPoints; i++) {
        const x = view.getFloat64(offset, le); offset += 8;
        const y = view.getFloat64(offset, le); offset += 8;
        ring.push([x, y]);
      }
      rings.push(ring);
    }
    return { type: "Polygon", coordinates: rings };
  }

  function readMultiPoint(le) {
    const num = view.getUint32(offset, le); offset += 4;
    const coords = [];
    for (let i = 0; i < num; i++) {
      const g = readGeometry();
      coords.push(g.coordinates);
    }
    return { type: "MultiPoint", coordinates: coords };
  }

  function readMultiLineString(le) {
    const num = view.getUint32(offset, le); offset += 4;
    const coords = [];
    for (let i = 0; i < num; i++) {
      const g = readGeometry();
      coords.push(g.coordinates);
    }
    return { type: "MultiLineString", coordinates: coords };
  }

  function readMultiPolygon(le) {
    const num = view.getUint32(offset, le); offset += 4;
    const coords = [];
    for (let i = 0; i < num; i++) {
      const g = readGeometry();
      coords.push(g.coordinates);
    }
    return { type: "MultiPolygon", coordinates: coords };
  }

  return readGeometry();
}

function extractCoords(geom) {
  switch (geom.type) {
    case "Point": return [geom.coordinates];
    case "LineString": return geom.coordinates;
    case "Polygon": return geom.coordinates[0];
    case "MultiPolygon": return geom.coordinates.flatMap(p => p[0]);
    default: return [];
  }
}

// ── Resize handle ──
const resizeHandle = document.getElementById("resize-handle");
const resultsContainer = document.getElementById("results-container");
let isResizing = false;

resizeHandle.addEventListener("mousedown", (e) => {
  isResizing = true;
  e.preventDefault();
});

document.addEventListener("mousemove", (e) => {
  if (!isResizing) return;
  const containerBottom = document.body.getBoundingClientRect().bottom;
  const newHeight = containerBottom - e.clientY;
  resultsContainer.style.height = Math.max(50, Math.min(newHeight, window.innerHeight - 200)) + "px";
  map.resize();
});

document.addEventListener("mouseup", () => {
  if (isResizing) {
    isResizing = false;
    map.resize();
  }
});

// ── Helpers ──
function setStatus(msg, isError = false) {
  const el = document.getElementById("status-text");
  el.textContent = msg;
  el.className = isError ? "error" : "";
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

// ── Init ──
map.on("load", () => {
  initDuckDB().catch(e => setStatus("DuckDB init failed: " + e.message, true));
});
</script>
</body>
</html>
```

- [ ] **Step 2: Test locally**

Run: `uv run python serve.py`

Open `http://localhost:8080/web/` in a browser. Verify:
- DuckDB WASM initializes (status bar shows "DuckDB ready")
- Query editor is visible with a default query
- Examples dropdown works
- Map renders
- If no data exists yet, run `uv run python main.py` briefly to generate some data, then test a query

- [ ] **Step 3: Commit**

```bash
git add web/index.html
git commit -m "Rewrite frontend with DuckDB WASM query interface and map"
```

---

### Task 9: Update serve.py routing

**Files:**
- Modify: `serve.py`

- [ ] **Step 1: Update serve.py**

The current serve.py serves from the project root. The new frontend expects `/data/parquet/` paths and `/web/`. The current setup already serves these as directory paths from the root, so no changes are needed beyond adding proper Content-Type for `.parquet` files:

Update `serve.py` — add parquet MIME type:

```python
"""Local dev server that serves both the web UI and data files with CORS and byte range support."""

import http.server
import mimetypes
import os
import sys
from pathlib import Path

# Register parquet MIME type
mimetypes.add_type("application/octet-stream", ".parquet")


class CORSRangeHandler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Range")
        self.send_header("Access-Control-Expose-Headers", "Content-Range, Content-Length")
        self.send_header("Accept-Ranges", "bytes")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        range_header = self.headers.get("Range")
        if not range_header:
            return super().do_GET()

        path = self.translate_path(self.path)
        try:
            file_size = os.path.getsize(path)
        except OSError:
            self.send_error(404)
            return

        # Parse "bytes=start-end"
        range_spec = range_header.replace("bytes=", "")
        parts = range_spec.split("-")
        start = int(parts[0]) if parts[0] else 0
        end = int(parts[1]) if parts[1] else file_size - 1
        end = min(end, file_size - 1)
        length = end - start + 1

        self.send_response(206)
        self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
        self.send_header("Content-Length", str(length))
        self.send_header("Content-Type", self.guess_type(path))
        self.end_headers()

        with open(path, "rb") as f:
            f.seek(start)
            self.wfile.write(f.read(length))


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    os.chdir(Path(__file__).parent)
    server = http.server.HTTPServer(("", port), CORSRangeHandler)
    print(f"Serving on http://localhost:{port}")
    print(f"  UI:    http://localhost:{port}/web/")
    print(f"  Data:  http://localhost:{port}/data/parquet/")
    server.serve_forever()
```

- [ ] **Step 2: Commit**

```bash
git add serve.py
git commit -m "Add parquet MIME type to dev server"
```

---

### Task 10: Update docker-compose.yml and environment

**Files:**
- Modify: `docker-compose.yml` (if it exists)

- [ ] **Step 1: Check if docker-compose.yml exists and update env vars**

The `.env` file needs new variables:
- `R2_PUBLIC_URL` — the public URL prefix for the R2 bucket (used in frontend queries)
- `PARQUET_BUILD_INTERVAL` — replaces `TILE_BUILD_INTERVAL` (default 300)
- `RETENTION_DAYS` — replaces `TILE_RETENTION_DAYS` (default 90)

Old env vars to remove: `TILE_BUILD_INTERVAL`, `TODAY_BUILD_INTERVAL`, `TILE_RETENTION_DAYS`

- [ ] **Step 2: Update docker-compose.yml if present**

Ensure the volume mount and env vars are correct. If docker-compose.yml references `TILE_BUILD_INTERVAL` or `TILE_RETENTION_DAYS`, update to `PARQUET_BUILD_INTERVAL` and `RETENTION_DAYS`.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "Update docker-compose for Parquet pipeline env vars"
```

---

### Task 11: End-to-end integration test

**Files:**
- No new files — manual testing

- [ ] **Step 1: Run the daemon briefly to collect data**

```bash
uv run python main.py
```

Wait ~2 minutes for a few sequences to be processed and a parquet build to trigger. Check:
- `data/deletions/YYYY-MM-DD.geojsonl` exists and has lines
- `data/parquet/date=YYYY-MM-DD/data.parquet` exists
- `data/parquet/metadata.json` exists

- [ ] **Step 2: Test the frontend**

```bash
uv run python serve.py
```

Open `http://localhost:8080/web/` and verify:
- DuckDB WASM initializes
- Default query populates with today's date
- Run the query — results appear in table
- Map shows features
- Clicking a feature shows popup with OSM link
- Clicking a row in table zooms map
- "Examples" dropdown works
- "Map Bounds" button inserts spatial filter

- [ ] **Step 3: Run full test suite one final time**

Run: `uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 4: Final commit with any fixes**

If any fixes were needed during integration testing, commit them.
