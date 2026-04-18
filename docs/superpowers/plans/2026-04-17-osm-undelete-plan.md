# OSM Undelete Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a system that watches OSM augmented diffs, extracts deleted objects, generates PMTiles, and presents them on an interactive map for recovery.

**Architecture:** A Python daemon polls augmented diffs from adiffs.osmcha.org, extracts deletions into daily line-delimited GeoJSON files, periodically runs tippecanoe/tile-join to produce PMTiles, uploads to Cloudflare R2. A static HTML page with MapLibre GL JS displays the data and allows users to download deleted objects for re-adding via JOSM.

**Tech Stack:** Python 3, requests, lxml, boto3, tippecanoe, tile-join, MapLibre GL JS, PMTiles JS

---

### Task 1: Project Scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `.gitignore`(update existing)
- Create: `daemon/__init__.py`
- Create: `pipeline/__init__.py`

- [ ] **Step 1: Create requirements.txt**

```
requests
lxml
boto3
python-dotenv
```

- [ ] **Step 2: Create .env.example**

```
R2_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=osm-undelete
TILE_RETENTION_DAYS=90
TILE_BUILD_INTERVAL=600
TODAY_UPLOAD_INTERVAL=60
DATA_DIR=./data
```

- [ ] **Step 3: Update .gitignore**

```
.superpowers/
data/
.env
__pycache__/
*.pyc
```

- [ ] **Step 4: Create package directories**

```bash
mkdir -p daemon pipeline data/deletions data/tiles data/state
touch daemon/__init__.py pipeline/__init__.py
```

- [ ] **Step 5: Set up virtualenv and install dependencies**

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

- [ ] **Step 6: Commit**

```bash
git add requirements.txt .env.example .gitignore daemon/__init__.py pipeline/__init__.py
git commit -m "Add project scaffolding"
```

---

### Task 2: Adiff Parser

**Files:**
- Create: `daemon/adiff_parser.py`
- Create: `tests/test_adiff_parser.py`
- Create: `tests/__init__.py`
- Create: `tests/fixtures/sample_delete_nodes.xml`
- Create: `tests/fixtures/sample_delete_ways.xml`

The augmented diff XML format (from adiffs.osmcha.org) looks like this:

**Deleted node (no tags):**
```xml
<action type="delete">
  <old>
    <node id="245053500" version="4" lon="6.4840074" lat="48.5848312"/>
  </old>
  <new>
    <node id="245053500" version="5" timestamp="2025-01-14T14:51:43Z"
          uid="311391" user="patman37" changeset="161348203"
          lat="48.5848312" lon="6.4840074" visible="false"/>
  </new>
</action>
```

**Deleted way (with tags and resolved geometry):**
```xml
<action type="delete">
  <old>
    <way id="452142440" version="2" user="joost schouppe import" uid="4212780"
         timestamp="2019-05-02T19:37:23Z" changeset="69818094">
      <bounds minlat="50.8059832" minlon="3.1143633" maxlat="50.8060128" maxlon="3.1144426"/>
      <nd ref="4489453005" lon="3.1143633" lat="50.8060083"/>
      <nd ref="4489453004" lon="3.1144426" lat="50.8060128"/>
      <nd ref="4489453003" lon="3.114411" lat="50.805989"/>
      <nd ref="4489453002" lon="3.1143678" lat="50.8059832"/>
      <nd ref="4489453001" lon="3.1143669" lat="50.8059867"/>
      <nd ref="4489453005" lon="3.1143633" lat="50.8060083"/>
      <tag k="building" v="shed"/>
      <tag k="source:geometry:date" v="2008-11-21"/>
    </way>
  </old>
  <new>
    <way id="452142440" version="3" timestamp="2025-01-14T14:51:11Z"
         uid="170722" user="JosV" changeset="161348179" visible="false"/>
  </new>
</action>
```

Key observations:
- Delete actions have `<old>` (with full data) and `<new>` (with `visible="false"` and no geometry/tags)
- The `<old>` element has the geometry and tags at time of deletion
- Ways have `<nd>` children with `lon`/`lat` attributes (resolved geometry)
- The `<new>` element has the changeset/user/timestamp of the deletion itself
- Nodes without tags have no `<tag>` children
- Ways whose first and last `<nd>` share the same `ref` are closed (polygons)

- [ ] **Step 1: Create test fixtures**

Create `tests/fixtures/sample_delete_nodes.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <action type="delete">
    <old>
      <node id="245053500" version="4" lon="6.4840074" lat="48.5848312"/>
    </old>
    <new>
      <node id="245053500" version="5" timestamp="2025-01-14T14:51:43Z"
            uid="311391" user="patman37" changeset="161348203"
            lat="48.5848312" lon="6.4840074" visible="false"/>
    </new>
  </action>
  <action type="delete">
    <old>
      <node id="100000001" version="3" lon="-73.9857" lat="40.7484"
            user="mapperA" uid="999" timestamp="2024-06-01T10:00:00Z" changeset="100">
        <tag k="name" v="Test Node"/>
        <tag k="amenity" v="cafe"/>
      </node>
    </old>
    <new>
      <node id="100000001" version="4" timestamp="2025-01-14T15:00:00Z"
            uid="888" user="deleterB" changeset="200"
            lat="40.7484" lon="-73.9857" visible="false"/>
    </new>
  </action>
  <action type="modify">
    <old>
      <node id="999999" version="1" lon="0.0" lat="0.0"/>
    </old>
    <new>
      <node id="999999" version="2" timestamp="2025-01-14T15:00:00Z"
            uid="111" user="editor" changeset="300" lon="1.0" lat="1.0"/>
    </new>
  </action>
</osm>
```

Create `tests/fixtures/sample_delete_ways.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <action type="delete">
    <old>
      <way id="383040181" version="1" user="Jakka" uid="2403313"
           timestamp="2015-11-27T09:53:27Z" changeset="35607893">
        <bounds minlat="50.7988902" minlon="3.1965474" maxlat="50.7989166" maxlon="3.1965993"/>
        <nd ref="3862422559" lon="3.1965474" lat="50.7989125"/>
        <nd ref="3862422560" lon="3.1965944" lat="50.7989166"/>
        <nd ref="3862422558" lon="3.1965993" lat="50.7988943"/>
        <nd ref="3862422557" lon="3.1965523" lat="50.7988902"/>
        <nd ref="3862422559" lon="3.1965474" lat="50.7989125"/>
        <tag k="building" v="shed"/>
      </way>
    </old>
    <new>
      <way id="383040181" version="2" timestamp="2025-01-14T14:51:11Z"
           uid="170722" user="JosV" changeset="161348179" visible="false"/>
    </new>
  </action>
  <action type="delete">
    <old>
      <way id="500000001" version="2" user="mapperC" uid="555"
           timestamp="2023-03-15T08:00:00Z" changeset="400">
        <bounds minlat="51.0" minlon="3.0" maxlat="51.1" maxlon="3.1"/>
        <nd ref="1001" lon="3.0" lat="51.0"/>
        <nd ref="1002" lon="3.1" lat="51.0"/>
        <nd ref="1003" lon="3.1" lat="51.1"/>
        <tag k="highway" v="residential"/>
        <tag k="name" v="Test Street"/>
      </way>
    </old>
    <new>
      <way id="500000001" version="3" timestamp="2025-01-14T14:52:00Z"
           uid="666" user="deleterD" changeset="500" visible="false"/>
    </new>
  </action>
</osm>
```

- [ ] **Step 2: Write the failing tests**

Create `tests/__init__.py` (empty) and `tests/test_adiff_parser.py`:

```python
import json
from pathlib import Path

from daemon.adiff_parser import parse_adiff

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_deleted_nodes():
    xml_bytes = (FIXTURES / "sample_delete_nodes.xml").read_bytes()
    features = list(parse_adiff(xml_bytes))

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
    xml_bytes = (FIXTURES / "sample_delete_ways.xml").read_bytes()
    features = list(parse_adiff(xml_bytes))

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
    xml_bytes = b'<?xml version="1.0" encoding="UTF-8"?><osm version="0.6"></osm>'
    features = list(parse_adiff(xml_bytes))
    assert features == []
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_adiff_parser.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'daemon.adiff_parser'`

- [ ] **Step 4: Implement adiff_parser.py**

Create `daemon/adiff_parser.py`:

```python
"""Parse augmented diff XML and extract deleted OSM objects as GeoJSON features."""

from lxml import etree


def parse_adiff(xml_bytes: bytes):
    """Parse augmented diff XML bytes and yield GeoJSON features for deleted objects.

    Each yielded feature represents a deleted node, way, or relation with its
    geometry and metadata at time of deletion.
    """
    root = etree.fromstring(xml_bytes)

    for action in root.iterchildren("action"):
        if action.get("type") != "delete":
            continue

        old_elem = action.find("old")
        new_elem = action.find("new")
        if old_elem is None or new_elem is None:
            continue

        old_obj = old_elem[0]
        new_obj = new_elem[0]
        obj_type = old_obj.tag  # "node", "way", or "relation"

        tags = {tag.get("k"): tag.get("v") for tag in old_obj.iterchildren("tag")}

        geometry = _extract_geometry(obj_type, old_obj)
        if geometry is None:
            continue

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "osm_type": obj_type,
                "osm_id": int(old_obj.get("id")),
                "version": int(old_obj.get("version")),
                "tags": tags,
                "deleted_by": new_obj.get("user", ""),
                "deleted_uid": int(new_obj.get("uid", 0)),
                "deleted_changeset": int(new_obj.get("changeset", 0)),
                "deleted_at": new_obj.get("timestamp", ""),
            },
        }
        yield feature


def _extract_geometry(obj_type, elem):
    """Extract GeoJSON geometry from an OSM element."""
    if obj_type == "node":
        lon = elem.get("lon")
        lat = elem.get("lat")
        if lon is None or lat is None:
            return None
        return {"type": "Point", "coordinates": [float(lon), float(lat)]}

    elif obj_type == "way":
        nds = elem.findall("nd")
        if not nds:
            return None
        coords = [[float(nd.get("lon")), float(nd.get("lat"))] for nd in nds]
        # Check if closed way (polygon)
        if len(nds) >= 4 and nds[0].get("ref") == nds[-1].get("ref"):
            return {"type": "Polygon", "coordinates": [coords]}
        else:
            return {"type": "LineString", "coordinates": coords}

    elif obj_type == "relation":
        # Relations can have complex geometry. For now, compute a centroid
        # from any member nodes or way nodes we can find.
        # Full relation geometry support is a future enhancement.
        bounds = elem.find("bounds")
        if bounds is not None:
            min_lon = float(bounds.get("minlon"))
            max_lon = float(bounds.get("maxlon"))
            min_lat = float(bounds.get("minlat"))
            max_lat = float(bounds.get("maxlat"))
            center_lon = (min_lon + max_lon) / 2
            center_lat = (min_lat + max_lat) / 2
            return {"type": "Point", "coordinates": [center_lon, center_lat]}

        members = elem.findall("member")
        for member in members:
            if member.get("type") == "node" and member.get("lon") and member.get("lat"):
                return {
                    "type": "Point",
                    "coordinates": [float(member.get("lon")), float(member.get("lat"))],
                }
        return None

    return None
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_adiff_parser.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add daemon/adiff_parser.py tests/
git commit -m "Add adiff parser with tests"
```

---

### Task 3: GeoJSON Writer

**Files:**
- Create: `daemon/geojson_writer.py`
- Create: `tests/test_geojson_writer.py`

The writer appends GeoJSON features to daily line-delimited GeoJSON files and maintains a `today.geojson` file.

- [ ] **Step 1: Write failing tests**

Create `tests/test_geojson_writer.py`:

```python
import json
from pathlib import Path

from daemon.geojson_writer import GeoJSONWriter


def _make_feature(osm_id=1, osm_type="node", lon=0.0, lat=0.0):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {
            "osm_type": osm_type,
            "osm_id": osm_id,
            "version": 1,
            "tags": {},
            "deleted_by": "user",
            "deleted_uid": 1,
            "deleted_changeset": 1,
            "deleted_at": "2025-01-14T14:51:43Z",
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


def test_append_multiple_features(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(osm_id=1), date_str="2025-01-14")
    writer.append(_make_feature(osm_id=2), date_str="2025-01-14")

    daily_file = tmp_path / "2025-01-14.geojsonl"
    lines = daily_file.read_text().strip().split("\n")
    assert len(lines) == 2


def test_get_today_geojson(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(osm_id=1), date_str="2025-01-14")
    writer.append(_make_feature(osm_id=2), date_str="2025-01-14")

    fc = writer.get_feature_collection("2025-01-14")
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 2


def test_list_daily_files(tmp_path):
    writer = GeoJSONWriter(tmp_path)
    writer.append(_make_feature(), date_str="2025-01-13")
    writer.append(_make_feature(), date_str="2025-01-14")

    files = writer.list_daily_files()
    assert len(files) == 2
    assert "2025-01-13" in files
    assert "2025-01-14" in files
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_geojson_writer.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement geojson_writer.py**

Create `daemon/geojson_writer.py`:

```python
"""Write deleted OSM features to daily line-delimited GeoJSON files."""

import json
from pathlib import Path


class GeoJSONWriter:
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def append(self, feature: dict, date_str: str):
        """Append a GeoJSON feature to the daily file for the given date."""
        daily_file = self.output_dir / f"{date_str}.geojsonl"
        with open(daily_file, "a") as f:
            f.write(json.dumps(feature, separators=(",", ":")) + "\n")

    def get_feature_collection(self, date_str: str) -> dict:
        """Read a daily file and return a GeoJSON FeatureCollection."""
        daily_file = self.output_dir / f"{date_str}.geojsonl"
        features = []
        if daily_file.exists():
            for line in daily_file.read_text().strip().split("\n"):
                if line:
                    features.append(json.loads(line))
        return {"type": "FeatureCollection", "features": features}

    def list_daily_files(self) -> dict[str, Path]:
        """Return a dict mapping date strings to their file paths."""
        result = {}
        for f in sorted(self.output_dir.glob("*.geojsonl")):
            date_str = f.stem
            result[date_str] = f
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_geojson_writer.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add daemon/geojson_writer.py tests/test_geojson_writer.py
git commit -m "Add GeoJSON writer with tests"
```

---

### Task 4: Watcher Daemon

**Files:**
- Create: `daemon/watcher.py`
- Create: `tests/test_watcher.py`

The watcher polls adiffs.osmcha.org, parses deletions, and writes them to daily GeoJSON files. It tracks the last-seen sequence number to resume after restarts.

- [ ] **Step 1: Write failing tests**

Create `tests/test_watcher.py`:

```python
import json
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
    mock_response.content = SAMPLE_ADIFF
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        count = watcher.fetch_and_process(6429815)
        assert count == 1

    # Check that a daily file was written
    geojsonl_files = list((tmp_path / "deletions").glob("*.geojsonl"))
    assert len(geojsonl_files) == 1
    line = geojsonl_files[0].read_text().strip()
    feature = json.loads(line)
    assert feature["properties"]["osm_id"] == 12345
    assert feature["properties"]["tags"]["name"] == "Deleted Node"


def test_fetch_404_returns_zero(tmp_path):
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("daemon.watcher.requests.get", return_value=mock_response):
        watcher = Watcher(data_dir=tmp_path)
        count = watcher.fetch_and_process(9999999999)
        assert count == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_watcher.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement watcher.py**

Create `daemon/watcher.py`:

```python
"""Watch adiffs.osmcha.org for new augmented diffs and extract deletions."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from daemon.adiff_parser import parse_adiff
from daemon.geojson_writer import GeoJSONWriter

logger = logging.getLogger(__name__)

ADIFF_URL = "https://adiffs.osmcha.org/replication/minute/{seq}.adiff"
STATE_URL = "https://planet.openstreetmap.org/replication/minute/state.txt"


class Watcher:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.state_dir = self.data_dir / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.writer = GeoJSONWriter(self.data_dir / "deletions")

    def get_latest_sequence(self) -> int:
        """Get the latest available sequence number from OSM replication."""
        resp = requests.get(STATE_URL)
        resp.raise_for_status()
        for line in resp.text.strip().split("\n"):
            if line.startswith("sequenceNumber="):
                return int(line.split("=")[1])
        raise ValueError("Could not find sequenceNumber in state.txt")

    def load_state(self) -> int | None:
        """Load the last processed sequence number from disk."""
        state_file = self.state_dir / "last_seq.txt"
        if state_file.exists():
            return int(state_file.read_text().strip())
        return None

    def save_state(self, seq: int):
        """Save the last processed sequence number to disk."""
        state_file = self.state_dir / "last_seq.txt"
        state_file.write_text(str(seq))

    def fetch_and_process(self, seq: int) -> int:
        """Fetch one adiff by sequence number, extract deletions, return count."""
        url = ADIFF_URL.format(seq=seq)
        resp = requests.get(url)
        if resp.status_code == 404:
            return 0
        resp.raise_for_status()

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        count = 0
        for feature in parse_adiff(resp.content):
            self.writer.append(feature, date_str=today)
            count += 1

        return count
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_watcher.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add daemon/watcher.py tests/test_watcher.py
git commit -m "Add watcher with tests"
```

---

### Task 5: Tile Build Pipeline

**Files:**
- Create: `pipeline/build_tiles.py`
- Create: `tests/test_build_tiles.py`

Runs tippecanoe on daily GeoJSON files to produce PMTiles, then tile-join to merge them.

- [ ] **Step 1: Write failing tests**

Create `tests/test_build_tiles.py`:

```python
import json
import subprocess
from pathlib import Path
from unittest.mock import patch, call

from pipeline.build_tiles import TileBuilder


def _write_geojsonl(path: Path, features: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for feat in features:
            f.write(json.dumps(feat) + "\n")


def _make_feature(osm_id=1):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
        "properties": {"osm_type": "node", "osm_id": osm_id},
    }


def test_build_daily_tiles(tmp_path):
    deletions_dir = tmp_path / "deletions"
    tiles_dir = tmp_path / "tiles"
    _write_geojsonl(deletions_dir / "2025-01-14.geojsonl", [_make_feature(1)])

    builder = TileBuilder(deletions_dir, tiles_dir)

    with patch("pipeline.build_tiles.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        built = builder.build_daily_tiles()

    assert len(built) == 1
    assert "2025-01-14" in built
    # Verify tippecanoe was called
    assert mock_run.called
    cmd = mock_run.call_args[0][0]
    assert "tippecanoe" in cmd[0]


def test_skip_already_built(tmp_path):
    deletions_dir = tmp_path / "deletions"
    tiles_dir = tmp_path / "tiles"
    tiles_dir.mkdir(parents=True)
    _write_geojsonl(deletions_dir / "2025-01-14.geojsonl", [_make_feature(1)])
    # Create an existing pmtiles file that is newer than the geojsonl
    pmtiles_file = tiles_dir / "2025-01-14.pmtiles"
    pmtiles_file.write_bytes(b"fake")
    # Make the pmtiles newer
    import os
    import time
    future_time = time.time() + 10
    os.utime(pmtiles_file, (future_time, future_time))

    builder = TileBuilder(deletions_dir, tiles_dir)

    with patch("pipeline.build_tiles.subprocess.run") as mock_run:
        built = builder.build_daily_tiles()

    assert len(built) == 0
    assert not mock_run.called


def test_merge_tiles(tmp_path):
    tiles_dir = tmp_path / "tiles"
    tiles_dir.mkdir(parents=True)
    (tiles_dir / "2025-01-13.pmtiles").write_bytes(b"fake1")
    (tiles_dir / "2025-01-14.pmtiles").write_bytes(b"fake2")

    builder = TileBuilder(tmp_path / "deletions", tiles_dir)

    with patch("pipeline.build_tiles.subprocess.run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        builder.merge_tiles()

    assert mock_run.called
    cmd = mock_run.call_args[0][0]
    assert "tile-join" in cmd[0]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_build_tiles.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement build_tiles.py**

Create `pipeline/build_tiles.py`:

```python
"""Build PMTiles from daily GeoJSON files using tippecanoe and tile-join."""

import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


class TileBuilder:
    def __init__(self, deletions_dir: Path, tiles_dir: Path):
        self.deletions_dir = Path(deletions_dir)
        self.tiles_dir = Path(tiles_dir)
        self.tiles_dir.mkdir(parents=True, exist_ok=True)

    def build_daily_tiles(self) -> list[str]:
        """Build PMTiles for any daily GeoJSON files that are new or updated.

        Returns list of date strings that were built.
        """
        built = []
        for geojsonl_file in sorted(self.deletions_dir.glob("*.geojsonl")):
            date_str = geojsonl_file.stem
            pmtiles_file = self.tiles_dir / f"{date_str}.pmtiles"

            # Skip if pmtiles is newer than geojsonl
            if pmtiles_file.exists():
                if pmtiles_file.stat().st_mtime > geojsonl_file.stat().st_mtime:
                    continue

            self._run_tippecanoe(geojsonl_file, pmtiles_file)
            built.append(date_str)

        return built

    def merge_tiles(self):
        """Merge all daily PMTiles into a single merged.pmtiles file."""
        daily_files = sorted(self.tiles_dir.glob("????-??-??.pmtiles"))
        if not daily_files:
            logger.info("No daily PMTiles files to merge")
            return

        merged_file = self.tiles_dir / "merged.pmtiles"
        cmd = [
            "tile-join",
            "--force",
            "--no-tile-size-limit",
            "-o", str(merged_file),
        ] + [str(f) for f in daily_files]

        logger.info("Merging %d daily files into %s", len(daily_files), merged_file)
        subprocess.run(cmd, check=True)

    def _run_tippecanoe(self, input_file: Path, output_file: Path):
        """Run tippecanoe on a single GeoJSON file."""
        cmd = [
            "tippecanoe",
            "--force",
            "--no-tile-size-limit",
            "-o", str(output_file),
            "-l", "deletions",
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            str(input_file),
        ]
        logger.info("Building tiles: %s -> %s", input_file.name, output_file.name)
        subprocess.run(cmd, check=True)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_build_tiles.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/build_tiles.py tests/test_build_tiles.py
git commit -m "Add tile build pipeline with tests"
```

---

### Task 6: R2 Upload

**Files:**
- Create: `pipeline/merge_upload.py`
- Create: `tests/test_merge_upload.py`

Uploads merged.pmtiles and today.geojson to Cloudflare R2 via boto3.

- [ ] **Step 1: Write failing tests**

Create `tests/test_merge_upload.py`:

```python
import json
from pathlib import Path
from unittest.mock import patch, MagicMock

from pipeline.merge_upload import R2Uploader


def test_upload_file(tmp_path):
    test_file = tmp_path / "test.pmtiles"
    test_file.write_bytes(b"fake pmtiles data")

    mock_client = MagicMock()
    with patch("pipeline.merge_upload.boto3.client", return_value=mock_client):
        uploader = R2Uploader(
            endpoint_url="https://test.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            bucket_name="test-bucket",
        )
        uploader.upload_file(test_file, "test.pmtiles")

    mock_client.upload_file.assert_called_once_with(
        str(test_file), "test-bucket", "test.pmtiles"
    )


def test_upload_today_geojson(tmp_path):
    geojson_data = {
        "type": "FeatureCollection",
        "features": [{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}],
    }

    mock_client = MagicMock()
    with patch("pipeline.merge_upload.boto3.client", return_value=mock_client):
        uploader = R2Uploader(
            endpoint_url="https://test.r2.cloudflarestorage.com",
            access_key_id="key",
            secret_access_key="secret",
            bucket_name="test-bucket",
        )
        uploader.upload_today_geojson(geojson_data)

    mock_client.put_object.assert_called_once()
    call_kwargs = mock_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == "today.geojson"
    assert call_kwargs["ContentType"] == "application/geo+json"
    body = json.loads(call_kwargs["Body"])
    assert body["type"] == "FeatureCollection"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_merge_upload.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement merge_upload.py**

Create `pipeline/merge_upload.py`:

```python
"""Upload PMTiles and GeoJSON to Cloudflare R2."""

import json
import logging
from pathlib import Path

import boto3

logger = logging.getLogger(__name__)


class R2Uploader:
    def __init__(self, endpoint_url: str, access_key_id: str, secret_access_key: str, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    def upload_file(self, local_path: Path, remote_key: str):
        """Upload a file to R2."""
        logger.info("Uploading %s -> s3://%s/%s", local_path, self.bucket_name, remote_key)
        self.client.upload_file(str(local_path), self.bucket_name, remote_key)

    def upload_today_geojson(self, feature_collection: dict):
        """Upload today's GeoJSON FeatureCollection to R2."""
        body = json.dumps(feature_collection, separators=(",", ":"))
        self.client.put_object(
            Bucket=self.bucket_name,
            Key="today.geojson",
            Body=body,
            ContentType="application/geo+json",
        )
        logger.info("Uploaded today.geojson (%d features)", len(feature_collection["features"]))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_merge_upload.py -v
```

Expected: All 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/merge_upload.py tests/test_merge_upload.py
git commit -m "Add R2 uploader with tests"
```

---

### Task 7: Pruning Old Data

**Files:**
- Create: `pipeline/prune.py`
- Create: `tests/test_prune.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_prune.py`:

```python
from datetime import date
from pathlib import Path

from pipeline.prune import prune_old_files


def test_prune_old_files(tmp_path):
    # Create files for various dates
    for d in ["2025-01-01", "2025-01-10", "2025-01-14"]:
        (tmp_path / f"{d}.geojsonl").write_text("data")
        (tmp_path / f"{d}.pmtiles").write_bytes(b"data")

    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 14))

    # Only 2025-01-01 should be pruned (13 days old > 10 day retention)
    assert len(pruned) == 2  # both .geojsonl and .pmtiles
    assert not (tmp_path / "2025-01-01.geojsonl").exists()
    assert not (tmp_path / "2025-01-01.pmtiles").exists()
    assert (tmp_path / "2025-01-10.geojsonl").exists()
    assert (tmp_path / "2025-01-14.geojsonl").exists()


def test_prune_nothing_to_prune(tmp_path):
    (tmp_path / "2025-01-14.geojsonl").write_text("data")
    pruned = prune_old_files(tmp_path, retention_days=10, today=date(2025, 1, 14))
    assert len(pruned) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_prune.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement prune.py**

Create `pipeline/prune.py`:

```python
"""Prune old daily GeoJSON and PMTiles files past retention."""

import logging
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def prune_old_files(directory: Path, retention_days: int, today: date | None = None) -> list[Path]:
    """Delete files older than retention_days. Returns list of deleted paths."""
    if today is None:
        today = date.today()

    cutoff = today - timedelta(days=retention_days)
    pruned = []

    for f in sorted(directory.iterdir()):
        if f.suffix not in (".geojsonl", ".pmtiles"):
            continue
        try:
            file_date = date.fromisoformat(f.stem)
        except ValueError:
            continue
        if file_date < cutoff:
            logger.info("Pruning old file: %s", f.name)
            f.unlink()
            pruned.append(f)

    return pruned
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_prune.py -v
```

Expected: All 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add pipeline/prune.py tests/test_prune.py
git commit -m "Add file pruning with tests"
```

---

### Task 8: Main Entry Point

**Files:**
- Create: `main.py`

This ties everything together: daemon loop + periodic tile builds + periodic uploads.

- [ ] **Step 1: Implement main.py**

Create `main.py`:

```python
"""OSM Undelete — main entry point.

Runs the watcher daemon with periodic tile builds and R2 uploads.
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone, date
from pathlib import Path

from dotenv import load_dotenv

from daemon.watcher import Watcher
from daemon.geojson_writer import GeoJSONWriter
from pipeline.build_tiles import TileBuilder
from pipeline.merge_upload import R2Uploader
from pipeline.prune import prune_old_files

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = 5  # seconds between adiff polls


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "./data"))

    watcher = Watcher(data_dir)
    tile_builder = TileBuilder(data_dir / "deletions", data_dir / "tiles")

    # R2 uploader (optional — skip if not configured)
    uploader = None
    r2_endpoint = os.environ.get("R2_ENDPOINT_URL")
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

    tile_build_interval = int(os.environ.get("TILE_BUILD_INTERVAL", "600"))
    today_upload_interval = int(os.environ.get("TODAY_UPLOAD_INTERVAL", "60"))
    retention_days = int(os.environ.get("TILE_RETENTION_DAYS", "90"))

    # Determine starting sequence
    last_seq = watcher.load_state()
    if last_seq is None:
        last_seq = watcher.get_latest_sequence()
        logger.info("No saved state, starting from latest sequence: %d", last_seq)
    else:
        logger.info("Resuming from saved sequence: %d", last_seq)

    last_tile_build = 0.0
    last_today_upload = 0.0

    logger.info("Starting watcher daemon (poll=%ds, tile_build=%ds, today_upload=%ds)",
                POLL_INTERVAL, tile_build_interval, today_upload_interval)

    while True:
        now = time.time()

        # Poll for new adiffs
        try:
            latest_seq = watcher.get_latest_sequence()
        except Exception:
            logger.exception("Failed to get latest sequence, retrying in %ds", POLL_INTERVAL)
            time.sleep(POLL_INTERVAL)
            continue

        if last_seq < latest_seq:
            next_seq = last_seq + 1
            try:
                count = watcher.fetch_and_process(next_seq)
                if count > 0:
                    logger.info("Seq %d: %d deletions", next_seq, count)
                last_seq = next_seq
                watcher.save_state(last_seq)
            except Exception:
                logger.exception("Failed to process seq %d", next_seq)
                time.sleep(POLL_INTERVAL)
                continue
        else:
            time.sleep(POLL_INTERVAL)

        # Periodic: upload today's GeoJSON
        if uploader and (now - last_today_upload) >= today_upload_interval:
            try:
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                writer = GeoJSONWriter(data_dir / "deletions")
                fc = writer.get_feature_collection(today_str)
                if fc["features"]:
                    uploader.upload_today_geojson(fc)
                last_today_upload = now
            except Exception:
                logger.exception("Failed to upload today.geojson")

        # Periodic: build tiles and upload
        if (now - last_tile_build) >= tile_build_interval:
            try:
                built = tile_builder.build_daily_tiles()
                if built:
                    logger.info("Built tiles for: %s", ", ".join(built))
                    tile_builder.merge_tiles()
                    if uploader:
                        merged = data_dir / "tiles" / "merged.pmtiles"
                        if merged.exists():
                            uploader.upload_file(merged, "merged.pmtiles")

                # Prune old files
                prune_old_files(data_dir / "deletions", retention_days)
                prune_old_files(data_dir / "tiles", retention_days)

                last_tile_build = now
            except Exception:
                logger.exception("Failed to build/upload tiles")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Shutting down")
        sys.exit(0)
```

- [ ] **Step 2: Verify the daemon starts and stops cleanly**

```bash
# Quick smoke test — should start polling and exit on Ctrl+C
timeout 5 python main.py || true
```

Expected: Logs showing "Starting watcher daemon" and either sequence processing or a clean timeout.

- [ ] **Step 3: Commit**

```bash
git add main.py
git commit -m "Add main entry point for watcher daemon"
```

---

### Task 9: Static Web Map

**Files:**
- Create: `web/index.html`

A single HTML page that loads PMTiles from R2 and today's GeoJSON, displays deleted objects on a MapLibre map, and shows popups with object details and recovery options.

- [ ] **Step 1: Create web/index.html**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OSM Undelete</title>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.css">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    #map { position: absolute; top: 0; left: 0; width: 100%; height: 100%; }

    .config-bar {
      position: absolute;
      top: 10px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 10;
      background: white;
      padding: 8px 16px;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.2);
      font-size: 14px;
      display: flex;
      gap: 8px;
      align-items: center;
    }
    .config-bar input {
      font-size: 13px;
      padding: 4px 8px;
      border: 1px solid #ccc;
      border-radius: 4px;
      width: 400px;
    }
    .config-bar button {
      padding: 4px 12px;
      border: 1px solid #0078d4;
      border-radius: 4px;
      background: #0078d4;
      color: white;
      cursor: pointer;
      font-size: 13px;
    }

    .maplibregl-popup-content {
      max-width: 400px;
      max-height: 400px;
      overflow-y: auto;
      font-size: 13px;
    }
    .popup-title {
      font-weight: bold;
      font-size: 15px;
      margin-bottom: 6px;
    }
    .popup-meta {
      color: #666;
      margin-bottom: 8px;
      font-size: 12px;
    }
    .popup-meta a { color: #0078d4; }
    .tag-table {
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 8px;
    }
    .tag-table td {
      padding: 2px 6px;
      border-bottom: 1px solid #eee;
      font-size: 12px;
    }
    .tag-table td:first-child {
      font-weight: 600;
      color: #444;
      white-space: nowrap;
    }
    .popup-actions {
      display: flex;
      gap: 6px;
      margin-top: 8px;
    }
    .popup-actions a, .popup-actions button {
      padding: 4px 10px;
      border-radius: 4px;
      font-size: 12px;
      text-decoration: none;
      cursor: pointer;
      border: 1px solid #ccc;
      background: #f5f5f5;
      color: #333;
    }
    .popup-actions a:hover, .popup-actions button:hover {
      background: #e0e0e0;
    }
  </style>
</head>
<body>

<div class="config-bar">
  <label>PMTiles URL:</label>
  <input type="text" id="pmtiles-url" placeholder="https://your-r2-bucket.example.com/merged.pmtiles">
  <button onclick="loadTiles()">Load</button>
</div>

<div id="map"></div>

<script src="https://unpkg.com/maplibre-gl@4/dist/maplibre-gl.js"></script>
<script src="https://unpkg.com/pmtiles@3/dist/pmtiles.js"></script>
<script>

// Register PMTiles protocol
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

const map = new maplibregl.Map({
  container: "map",
  style: {
    version: 8,
    sources: {
      osm: {
        type: "raster",
        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
        tileSize: 256,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      },
    },
    layers: [{ id: "osm", type: "raster", source: "osm" }],
  },
  center: [0, 30],
  zoom: 2,
});

map.addControl(new maplibregl.NavigationControl(), "top-right");

// Layer colors
const DELETE_COLOR = "#e74c3c";
const TODAY_COLOR = "#e67e22";

map.on("load", () => {
  // Try loading from URL hash
  const params = new URLSearchParams(window.location.hash.slice(1));
  const savedUrl = params.get("pmtiles");
  if (savedUrl) {
    document.getElementById("pmtiles-url").value = savedUrl;
    addPMTilesSource(savedUrl);
  }
});

function loadTiles() {
  const url = document.getElementById("pmtiles-url").value.trim();
  if (!url) return;

  // Save to URL hash
  window.location.hash = `pmtiles=${encodeURIComponent(url)}`;

  // Remove existing layers/sources
  for (const layerId of ["deletions-points", "deletions-lines", "deletions-polygons"]) {
    if (map.getLayer(layerId)) map.removeLayer(layerId);
  }
  if (map.getSource("deletions")) map.removeSource("deletions");

  addPMTilesSource(url);

  // Also try loading today.geojson from the same base URL
  const baseUrl = url.replace(/\/[^/]+$/, "");
  loadTodayGeoJSON(baseUrl + "/today.geojson");
}

function addPMTilesSource(url) {
  map.addSource("deletions", {
    type: "vector",
    url: `pmtiles://${url}`,
  });

  addDeletionLayers("deletions", DELETE_COLOR);
}

function addDeletionLayers(sourceId, color) {
  const sourceLayer = "deletions";

  map.addLayer({
    id: sourceId + "-polygons",
    type: "fill",
    source: sourceId,
    "source-layer": sourceLayer,
    filter: ["==", "$type", "Polygon"],
    paint: {
      "fill-color": color,
      "fill-opacity": 0.3,
      "fill-outline-color": color,
    },
  });

  map.addLayer({
    id: sourceId + "-lines",
    type: "line",
    source: sourceId,
    "source-layer": sourceLayer,
    filter: ["==", "$type", "LineString"],
    paint: {
      "line-color": color,
      "line-width": 2,
    },
  });

  map.addLayer({
    id: sourceId + "-points",
    type: "circle",
    source: sourceId,
    "source-layer": sourceLayer,
    filter: ["==", "$type", "Point"],
    paint: {
      "circle-radius": 5,
      "circle-color": color,
      "circle-stroke-width": 1,
      "circle-stroke-color": "#fff",
    },
  });
}

function loadTodayGeoJSON(url) {
  // Remove existing today layers
  for (const layerId of ["today-points", "today-lines", "today-polygons"]) {
    if (map.getLayer(layerId)) map.removeLayer(layerId);
  }
  if (map.getSource("today")) map.removeSource("today");

  fetch(url)
    .then((r) => {
      if (!r.ok) throw new Error("No today.geojson");
      return r.json();
    })
    .then((geojson) => {
      map.addSource("today", { type: "geojson", data: geojson });

      map.addLayer({
        id: "today-polygons",
        type: "fill",
        source: "today",
        filter: ["==", "$type", "Polygon"],
        paint: { "fill-color": TODAY_COLOR, "fill-opacity": 0.3, "fill-outline-color": TODAY_COLOR },
      });
      map.addLayer({
        id: "today-lines",
        type: "line",
        source: "today",
        filter: ["==", "$type", "LineString"],
        paint: { "line-color": TODAY_COLOR, "line-width": 2 },
      });
      map.addLayer({
        id: "today-points",
        type: "circle",
        source: "today",
        filter: ["==", "$type", "Point"],
        paint: { "circle-radius": 5, "circle-color": TODAY_COLOR, "circle-stroke-width": 1, "circle-stroke-color": "#fff" },
      });
    })
    .catch(() => {
      // today.geojson not available — that's fine
    });

  // Refresh today's data periodically
  setInterval(() => {
    fetch(url)
      .then((r) => r.ok ? r.json() : null)
      .then((geojson) => {
        if (geojson && map.getSource("today")) {
          map.getSource("today").setData(geojson);
        }
      })
      .catch(() => {});
  }, 60000);
}

// Click handler for popups
const clickableLayers = [
  "deletions-points", "deletions-lines", "deletions-polygons",
  "today-points", "today-lines", "today-polygons",
];

map.on("click", (e) => {
  const features = map.queryRenderedFeatures(e.point, { layers: clickableLayers.filter((l) => map.getLayer(l)) });
  if (features.length === 0) return;

  const f = features[0];
  const props = f.properties;
  const osmType = props.osm_type;
  const osmId = props.osm_id;

  // Parse tags — may be a JSON string
  let tags = props.tags;
  if (typeof tags === "string") {
    try { tags = JSON.parse(tags); } catch { tags = {}; }
  }
  tags = tags || {};

  // Build popup
  let html = `<div class="popup-title">${osmType}/${osmId}</div>`;
  html += `<div class="popup-meta">`;
  html += `Deleted <strong>${props.deleted_at || "unknown"}</strong>`;
  html += ` by <a href="https://www.openstreetmap.org/user/${encodeURIComponent(props.deleted_by)}" target="_blank">${props.deleted_by || "unknown"}</a>`;
  html += ` in <a href="https://www.openstreetmap.org/changeset/${props.deleted_changeset}" target="_blank">changeset ${props.deleted_changeset}</a>`;
  html += `</div>`;

  // Tags table
  const tagEntries = Object.entries(tags);
  if (tagEntries.length > 0) {
    html += `<table class="tag-table">`;
    for (const [k, v] of tagEntries) {
      html += `<tr><td>${escapeHtml(k)}</td><td>${escapeHtml(v)}</td></tr>`;
    }
    html += `</table>`;
  } else {
    html += `<p style="color:#999;font-size:12px;">No tags</p>`;
  }

  // Actions
  const center = getFeatureCenter(f);
  const josmUrl = `http://localhost:8111/load_and_zoom?left=${center[0] - 0.001}&right=${center[0] + 0.001}&bottom=${center[1] - 0.001}&top=${center[1] + 0.001}`;

  html += `<div class="popup-actions">`;
  html += `<a href="https://www.openstreetmap.org/${osmType}/${osmId}/history" target="_blank">History</a>`;
  html += `<a href="${josmUrl}" target="_blank">Open in JOSM</a>`;
  html += `<button onclick="downloadOsm('${osmType}', ${osmId}, ${JSON.stringify(JSON.stringify(tags))}, ${JSON.stringify(JSON.stringify(f.geometry))})">Download .osm</button>`;
  html += `</div>`;

  new maplibregl.Popup()
    .setLngLat(e.lngLat)
    .setHTML(html)
    .addTo(map);
});

// Change cursor on hover
for (const layer of clickableLayers) {
  map.on("mouseenter", layer, () => { if (map.getLayer(layer)) map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", layer, () => { if (map.getLayer(layer)) map.getCanvas().style.cursor = ""; });
}

function getFeatureCenter(feature) {
  const geom = feature.geometry;
  if (geom.type === "Point") return geom.coordinates;
  // For lines and polygons, compute centroid of bbox
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  const coords = geom.type === "Polygon" ? geom.coordinates[0] : geom.coordinates;
  for (const c of coords) {
    if (c[0] < minX) minX = c[0];
    if (c[0] > maxX) maxX = c[0];
    if (c[1] < minY) minY = c[1];
    if (c[1] > maxY) maxY = c[1];
  }
  return [(minX + maxX) / 2, (minY + maxY) / 2];
}

function escapeHtml(str) {
  const div = document.createElement("div");
  div.textContent = str;
  return div.innerHTML;
}

function downloadOsm(osmType, osmId, tagsJson, geomJson) {
  const tags = JSON.parse(tagsJson);
  const geom = JSON.parse(geomJson);

  let xml = '<?xml version="1.0" encoding="UTF-8"?>\n';
  xml += '<osm version="0.6" generator="osm-undelete">\n';

  if (osmType === "node") {
    xml += `  <node id="-1" lon="${geom.coordinates[0]}" lat="${geom.coordinates[1]}" version="1">\n`;
    for (const [k, v] of Object.entries(tags)) {
      xml += `    <tag k="${escapeXml(k)}" v="${escapeXml(v)}"/>\n`;
    }
    xml += `  </node>\n`;
  } else if (osmType === "way") {
    const coords = geom.type === "Polygon" ? geom.coordinates[0] : geom.coordinates;
    let nodeId = -1;
    for (const c of coords) {
      xml += `  <node id="${nodeId}" lon="${c[0]}" lat="${c[1]}" version="1"/>\n`;
      nodeId--;
    }
    xml += `  <way id="-1" version="1">\n`;
    for (let i = -1; i > nodeId; i--) {
      xml += `    <nd ref="${i}"/>\n`;
    }
    for (const [k, v] of Object.entries(tags)) {
      xml += `    <tag k="${escapeXml(k)}" v="${escapeXml(v)}"/>\n`;
    }
    xml += `  </way>\n`;
  }

  xml += '</osm>';

  const blob = new Blob([xml], { type: "application/xml" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `${osmType}-${osmId}.osm`;
  a.click();
  URL.revokeObjectURL(url);
}

function escapeXml(str) {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

</script>
</body>
</html>
```

- [ ] **Step 2: Test the web page locally**

```bash
cd web && python3 -m http.server 8080
```

Open http://localhost:8080 in a browser. Verify:
- Map loads with OSM tiles
- Config bar is visible at the top
- No JavaScript errors in the console

- [ ] **Step 3: Commit**

```bash
git add web/index.html
git commit -m "Add static web map with MapLibre and PMTiles"
```

---

### Task 10: End-to-End Integration Test

**Files:** None new — tests existing code together.

- [ ] **Step 1: Run the full test suite**

```bash
python -m pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 2: Manual smoke test with real data**

```bash
# Fetch a single real adiff and process it locally
python -c "
from daemon.watcher import Watcher
from pathlib import Path
w = Watcher(Path('./data'))
seq = w.get_latest_sequence()
print(f'Latest sequence: {seq}')
count = w.fetch_and_process(seq)
print(f'Deletions found: {count}')
"
```

Verify that `data/deletions/` contains a `.geojsonl` file with valid GeoJSON features.

- [ ] **Step 3: Test tile generation (requires tippecanoe installed)**

```bash
# Only if tippecanoe is installed
which tippecanoe && python -c "
from pipeline.build_tiles import TileBuilder
from pathlib import Path
tb = TileBuilder(Path('./data/deletions'), Path('./data/tiles'))
built = tb.build_daily_tiles()
print(f'Built tiles for: {built}')
if built:
    tb.merge_tiles()
    print('Merged successfully')
"
```

- [ ] **Step 4: Commit any test data cleanup**

```bash
# Clean up test data if any was generated
rm -rf data/deletions/* data/tiles/* data/state/*
```
