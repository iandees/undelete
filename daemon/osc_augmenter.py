"""Build augmented-diff-style features from planet osmChange files + the OSM API.

PROTOTYPE. Planet's replication .osc files (minute, hour or day) only contain the
new state of each element: no old tags, no way geometry. This module fills the
gaps with batch requests to the OSM API (``/nodes?nodes=1v2,3,...``) and emits
features in the same shape as ``daemon.adiff_parser.parse_adiff``.

Memory is bounded regardless of diff size: the .osc is streamed into a throwaway
SQLite file, API fetch lists are computed with SQL (so a node referenced by many
ways is fetched once), and features are generated one at a time.

Geometry is resolved "as of" each edit. The table holds every node/way version in
the diff, the previous versions we fetched, and the current API state of anything
the diff doesn't touch. For an edit at time T by changeset C:
  * new geometry uses the latest member version with timestamp <= T
  * old geometry does the same but ignores versions written by C itself
Members the diff doesn't touch fall back to their current API state, so a long lag
can misplace a node that moved after the diff window.

Relations: only multipolygon/boundary relations with a bounded number of way
members get a geometry; the rest are skipped and counted.

Usage:
    uv run python -m daemon.osc_augmenter --interval hour --count 1 [--out f.geojsonl]
"""

import argparse
import gzip
import json
import logging
import os
import resource
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from xml.etree import ElementTree as ET

import requests

from daemon.adiff_parser import _build_geometry

logger = logging.getLogger(__name__)

PLANET_URL = "https://planet.openstreetmap.org/replication"
API_URL = "https://api.openstreetmap.org/api/0.6"
USER_AGENT = os.environ.get(
    "OSM_USER_AGENT", "osm-undelete/0.1 (+https://github.com/iandees/undelete)"
)

MAX_IDS_PER_REQUEST = 700
MAX_QUERY_CHARS = 6000  # keep URLs well under common 8KB limits
MAX_RELATION_WAYS = 200  # skip geometry for relations with more way members
MULTIPOLYGON_TYPES = ("multipolygon", "boundary")
KINDS = {"node": "nodes", "way": "ways", "relation": "relations"}
WANT_PAGE = 5000
# Pause between API requests so a long catch-up can't hammer the API back to back.
DEFAULT_MIN_INTERVAL = float(os.environ.get("OSM_API_MIN_INTERVAL", "0.5"))


class NotFound(Exception):
    pass


@dataclass
class Element:
    type: str
    id: int
    version: int
    visible: bool
    ts: str = ""
    changeset: int = 0
    uid: int = 0
    user: str = ""
    lon: float | None = None
    lat: float | None = None
    tags: dict = field(default_factory=dict)
    nds: list = field(default_factory=list)
    members: list = field(default_factory=list)

    @property
    def xy(self):
        if self.lon is None or self.lat is None:
            return None
        return (self.lon, self.lat)


def _parse_element(el, visible=None):
    a = el.attrib
    if visible is None:
        visible = a.get("visible", "true") != "false"
    return Element(
        type=el.tag,
        id=int(a["id"]),
        version=int(a["version"]),
        visible=visible,
        ts=a.get("timestamp", ""),
        changeset=int(a.get("changeset", 0)),
        uid=int(a.get("uid", 0)),
        user=a.get("user", ""),
        lon=float(a["lon"]) if "lon" in a else None,
        lat=float(a["lat"]) if "lat" in a else None,
        tags={t.get("k"): t.get("v") for t in el.findall("tag")},
        nds=[int(n.get("ref")) for n in el.findall("nd")],
        members=[
            {"type": m.get("type"), "ref": int(m.get("ref")), "role": m.get("role", "")}
            for m in el.findall("member")
        ],
    )


def iter_osc(fileobj):
    """Stream (action, Element) from an osmChange file without holding it in memory."""
    root = None
    action = None
    for event, el in ET.iterparse(fileobj, events=("start", "end")):
        if event == "start":
            if root is None:
                root = el
            elif el.tag in ("create", "modify", "delete"):
                action = el.tag
            continue
        if el.tag in KINDS:
            yield action, _parse_element(el, visible=action != "delete")
        elif el.tag in ("create", "modify", "delete"):
            root.clear()  # drop finished <action> subtrees


def seq_path(seq: int) -> str:
    s = f"{seq:09d}"
    return f"{s[0:3]}/{s[3:6]}/{s[6:9]}"


class ApiClient:
    """Thin OSM API client for batch multi-fetch, with request accounting."""

    def __init__(self, session=None, min_interval=DEFAULT_MIN_INTERVAL):
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = USER_AGENT
        self.min_interval = min_interval
        self._last_request = 0.0
        self._pace = threading.Lock()  # one pacing clock even if used from several threads
        self.requests = Counter()  # label -> request count
        self.bytes = 0
        self.seconds = 0.0

    def _get(self, url, label):
        for attempt in range(5):
            with self._pace:
                wait = self._last_request + self.min_interval - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                start = self._last_request = time.monotonic()
            resp = self.session.get(url, timeout=60)
            self.seconds += time.monotonic() - start
            self.requests[label] += 1
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = float(resp.headers.get("Retry-After", 2 ** attempt))
                logger.warning("HTTP %d from %s, retrying in %.0fs", resp.status_code, label, delay)
                time.sleep(delay)
                continue
            if resp.status_code in (404, 410):
                raise NotFound(url)
            resp.raise_for_status()
            self.bytes += len(resp.content)
            return resp.content
        resp.raise_for_status()

    def _fetch_tokens(self, kind, tokens, label):
        """Fetch one batch; on 404 (some id missing/redacted) bisect to isolate it."""
        url = f"{API_URL}/{KINDS[kind]}?{KINDS[kind]}={','.join(tokens)}"
        try:
            content = self._get(url, label)
        except NotFound:
            if len(tokens) == 1:
                logger.warning("Missing %s %s", kind, tokens[0])
                return []
            mid = len(tokens) // 2
            return (self._fetch_tokens(kind, tokens[:mid], label)
                    + self._fetch_tokens(kind, tokens[mid:], label))
        return [_parse_element(el) for el in ET.fromstring(content) if el.tag == kind]

    def fetch(self, kind, tokens, label):
        """Fetch tokens ('123' for latest, '123v4' for a version) in URL-safe chunks."""
        out = []
        chunk, chars = [], 0
        for tok in tokens:
            if chunk and (len(chunk) >= MAX_IDS_PER_REQUEST or chars + len(tok) + 1 > MAX_QUERY_CHARS):
                out += self._fetch_tokens(kind, chunk, label)
                chunk, chars = [], 0
            chunk.append(tok)
            chars += len(tok) + 1
        if chunk:
            out += self._fetch_tokens(kind, chunk, label)
        return out


SCHEMA = """
CREATE TABLE elements (
    type TEXT NOT NULL, id INTEGER NOT NULL, version INTEGER NOT NULL,
    src TEXT NOT NULL,          -- 'osc' (in the diff), 'prev' (fetched old version), 'current'
    action TEXT,                -- create/modify/delete for src='osc'
    visible INTEGER NOT NULL, ts TEXT, changeset INTEGER, uid INTEGER, user TEXT,
    lon REAL, lat REAL, tags TEXT, nds TEXT, members TEXT,
    UNIQUE (type, id, version)
);
CREATE TABLE want (type TEXT NOT NULL, id INTEGER NOT NULL, version INTEGER NOT NULL,
                   UNIQUE (type, id, version));
"""
COLUMNS = "type,id,version,src,action,visible,ts,changeset,uid,user,lon,lat,tags,nds,members"


def _row(e: Element, src, action=None):
    return (e.type, e.id, e.version, src, action, int(e.visible), e.ts, e.changeset, e.uid,
            e.user, e.lon, e.lat, json.dumps(e.tags), json.dumps(e.nds), json.dumps(e.members))


def _element(row) -> Element:
    (type_, id_, version, _src, _action, visible, ts, changeset, uid, user,
     lon, lat, tags, nds, members) = row
    return Element(type_, id_, version, bool(visible), ts, changeset, uid, user, lon, lat,
                   json.loads(tags), json.loads(nds), json.loads(members))


class Augmenter:
    """Turns one osmChange file into augmented-diff-style GeoJSON features."""

    def __init__(self, api: ApiClient | None = None, work_dir: str | None = None):
        self.api = api or ApiClient()
        self.work_dir = work_dir
        self.stats = Counter()

    def process(self, osc_fileobj):
        """Generator of features for one osmChange file. self.stats fills as it runs."""
        tmp = tempfile.mkdtemp(prefix="osc-augment-", dir=self.work_dir)
        try:
            self.db = sqlite3.connect(os.path.join(tmp, "work.db"))
            self.db.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;"
                                  "PRAGMA cache_size=-20000; PRAGMA temp_store=FILE;")
            self.db.executescript(SCHEMA)
            for name, step in (("load", lambda: self._load(osc_fileobj)),
                               ("previous", self._fetch_previous_versions),
                               ("relation_ways", self._fetch_relation_ways),
                               ("nodes", self._fetch_nodes)):
                start = time.monotonic()
                api_before = self.api.seconds
                step()
                self.stats[f"seconds.{name}"] = round(time.monotonic() - start)
                self.stats[f"seconds.{name}.api_wait"] = round(self.api.seconds - api_before)
            yield from self._features()
            self.stats["work_db_mb"] = os.path.getsize(os.path.join(tmp, "work.db")) // 2 ** 20
            self.db.close()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    # ---- loading ----

    def _insert(self, rows):
        self.db.executemany(
            f"INSERT OR REPLACE INTO elements ({COLUMNS}) VALUES ({','.join('?' * 15)})", rows)

    def _load(self, fileobj):
        batch = []
        for action, e in iter_osc(fileobj):
            batch.append(_row(e, "osc", action))
            self.stats[f"osc.{action}.{e.type}"] += 1
            if len(batch) >= 5000:
                self._insert(batch)
                batch = []
        self._insert(batch)
        self.db.commit()

    # ---- API prefetch (fetch lists are computed in SQL, so each thing is fetched once) ----

    def _drain_want(self, label, versioned, src):
        """Fetch everything in `want` from the API into `elements`, then empty it."""
        for kind in KINDS:
            last = 0
            while True:
                rows = self.db.execute(
                    "SELECT rowid,id,version FROM want WHERE type=? AND rowid>? ORDER BY rowid LIMIT ?",
                    (kind, last, WANT_PAGE)).fetchall()
                if not rows:
                    break
                last = rows[-1][0]
                tokens = [f"{i}v{v}" if versioned else str(i) for _, i, v in rows]
                self._insert([_row(e, src) for e in self.api.fetch(kind, tokens, f"{label}.{kind}")])
        self.db.execute("DELETE FROM want")
        self.db.commit()

    def _fetch_previous_versions(self):
        """The version before every modified/deleted element (and before each element's
        first version in the diff), unless the diff already has it."""
        self.db.execute("""
            INSERT OR IGNORE INTO want SELECT e.type, e.id, e.version - 1 FROM elements e
            WHERE e.src='osc' AND e.version > 1 AND NOT EXISTS (
                SELECT 1 FROM elements p WHERE p.type=e.type AND p.id=e.id AND p.version=e.version-1)
        """)
        self._drain_want("previous", versioned=True, src="prev")

    def _drain_current_and_last_visible(self, label):
        """Fetch current state for everything in `want`, plus the last visible version
        of anything that has since been deleted."""
        self._drain_want(label, versioned=False, src="current")
        self.db.execute("""
            INSERT OR IGNORE INTO want SELECT type, id, version - 1 FROM elements c
            WHERE c.src='current' AND c.visible=0 AND c.version > 1
              AND NOT EXISTS (SELECT 1 FROM elements p WHERE p.type=c.type AND p.id=c.id
                              AND p.version=c.version-1)
        """)
        self._drain_want(f"{label}.last_visible", versioned=True, src="prev")

    def _drop_known_from_want(self):
        self.db.execute("""DELETE FROM want WHERE EXISTS (
            SELECT 1 FROM elements x WHERE x.type=want.type AND x.id=want.id)""")

    def _fetch_relation_ways(self):
        """Way members of multipolygon relations (new and previous versions)."""
        rows = self.db.execute(
            "SELECT tags, members FROM elements WHERE type='relation' AND visible=1").fetchall()
        want = set()
        for tags, members in rows:
            rel = Element("relation", 0, 0, True, tags=json.loads(tags), members=json.loads(members))
            if self._wants_relation_geometry(rel):
                want.update(m["ref"] for m in rel.members if m["type"] == "way")
        self._queue_want("way", want)
        self._drop_known_from_want()
        self._drain_current_and_last_visible("relation_ways")

    def _queue_want(self, kind, ids):
        self.db.executemany("INSERT OR IGNORE INTO want VALUES (?,?,0)", [(kind, i) for i in ids])

    def _fetch_nodes(self):
        """Current state of every node referenced by a way that isn't in the diff."""
        self.db.execute("""
            INSERT OR IGNORE INTO want SELECT 'node', j.value, 0
            FROM elements e, json_each(e.nds) j WHERE e.type='way'""")
        self._drop_known_from_want()
        self._drain_current_and_last_visible("nodes")

    # ---- as-of lookups ----

    def _exact(self, type_, id_, version):
        row = self.db.execute(
            f"SELECT {COLUMNS} FROM elements WHERE type=? AND id=? AND version=?",
            (type_, id_, version)).fetchone()
        return _element(row) if row else None

    def _as_of(self, type_, id_, ts, exclude_changeset=None):
        """The element as it stood at `ts`, ignoring versions by `exclude_changeset`."""
        row = self.db.execute(
            f"SELECT {COLUMNS} FROM elements WHERE type=? AND id=? AND ts<=? "
            "AND (? IS NULL OR changeset != ?) ORDER BY version DESC LIMIT 1",
            (type_, id_, ts, exclude_changeset, exclude_changeset)).fetchone()
        if row is None:
            # Untouched by the diff window: current API state is the best we have
            row = self.db.execute(
                f"SELECT {COLUMNS} FROM elements WHERE type=? AND id=? AND src='current'",
                (type_, id_)).fetchone()
            if row is not None:
                self.stats["fallback_current_state"] += 1
        if row is None:
            return None
        e = _element(row)
        return e if e.visible else None

    def _nds(self, way: Element, ts, exclude_changeset):
        out = []
        for ref in way.nds:
            node = self._as_of("node", ref, ts, exclude_changeset)
            if node is None or node.xy is None:
                self.stats["missing_node_coords"] += 1
                continue
            out.append({"ref": str(ref), "lon": node.lon, "lat": node.lat})
        return out

    @staticmethod
    def _wants_relation_geometry(rel):
        if rel.tags.get("type") not in MULTIPOLYGON_TYPES:
            return False
        return sum(1 for m in rel.members if m["type"] == "way") <= MAX_RELATION_WAYS

    def _geometry(self, e: Element, ts, exclude_changeset):
        """Geometry of `e`'s content, with members resolved as of `ts`."""
        if e.type == "node":
            return _build_geometry("node", {"lon": e.lon, "lat": e.lat}, None, None)
        if e.type == "way":
            nds = self._nds(e, ts, exclude_changeset)
            return _build_geometry("way", {}, nds, None) if nds else None
        if not self._wants_relation_geometry(e):
            self.stats["skipped_relation_geometry"] += 1
            return None
        members = []
        for m in e.members:
            if m["type"] != "way":
                continue
            way = self._as_of("way", m["ref"], ts, exclude_changeset)
            if way is None:
                self.stats["missing_member_way"] += 1
                continue
            members.append({"role": m["role"], "nds": self._nds(way, ts, exclude_changeset)})
        return _build_geometry("relation", {}, None, None, members=members, tags=e.tags)

    # ---- features ----

    def _features(self):
        cursor = self.db.execute(
            f"SELECT {COLUMNS} FROM elements WHERE src='osc' ORDER BY rowid")
        for row in cursor:
            f = self._feature(row[4], _element(row))
            if f is not None:
                self.stats[f"feature.{row[4]}.{row[0]}"] += 1
                yield f

    def _feature(self, action, e: Element):
        prev = None
        if action in ("modify", "delete") and e.version > 1:
            prev = self._exact(e.type, e.id, e.version - 1)
            if prev is not None and not prev.visible:
                prev = None

        if action == "create":
            geometry, old_geometry = self._geometry(e, e.ts, None), None
            tags, old_tags, version = e.tags, None, e.version
        elif action == "modify":
            geometry = self._geometry(e, e.ts, None)
            old_geometry = self._geometry(prev, e.ts, e.changeset) if prev is not None else None
            tags, old_tags, version = e.tags, (prev.tags if prev is not None else {}), e.version
        else:  # delete
            if prev is None:
                self.stats["skipped_delete_no_previous"] += 1
                return None
            geometry, old_geometry = self._geometry(prev, e.ts, e.changeset), None
            tags, old_tags, version = {}, prev.tags, prev.version

        if geometry is None:
            self.stats["skipped_no_geometry"] += 1
            return None
        return {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "action": action,
                "osm_type": e.type,
                "osm_id": e.id,
                "version": version,
                "changeset": e.changeset,
                "user": e.user,
                "uid": e.uid,
                "timestamp": e.ts,
                "tags": tags,
                "old_tags": old_tags if old_tags else ({} if action != "create" else None),
                "old_geometry": old_geometry,
            },
        }


def latest_planet_sequence(session, interval="minute") -> int:
    text = session.get(f"{PLANET_URL}/{interval}/state.txt", timeout=30).text
    for line in text.splitlines():
        if line.startswith("sequenceNumber="):
            return int(line.split("=")[1])
    raise ValueError("no sequenceNumber in state.txt")


def download_osc(session, seq: int, interval, directory) -> str:
    """Download an .osc.gz to `directory` and return its path."""
    resp = session.get(f"{PLANET_URL}/{interval}/{seq_path(seq)}.osc.gz", stream=True, timeout=60)
    resp.raise_for_status()
    path = os.path.join(directory, f"{interval}-{seq}.osc.gz")
    with open(path, "wb") as f:
        shutil.copyfileobj(resp.raw, f)
    return path


def open_osc(session, seq: int, interval, tmp_dir):
    """Download an .osc.gz to disk and return an open gzip stream over it."""
    return gzip.open(download_osc(session, seq, interval, tmp_dir), "rb")


def peak_rss_mb():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 2 ** 20 if sys.platform == "darwin" else rss / 2 ** 10


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--seq", type=int, help="first sequence (default: latest - count - 1)")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--interval", choices=("minute", "hour", "day"), default="minute")
    parser.add_argument("--out", help="write features as line-delimited GeoJSON")
    parser.add_argument("--min-interval", type=float, default=DEFAULT_MIN_INTERVAL,
                        help="minimum seconds between OSM API requests (default %(default)s)")
    parser.add_argument("--work-dir", help="where to put temp files (default: system temp)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    first = args.seq or latest_planet_sequence(session, args.interval) - args.count - 1
    out = open(args.out, "w") if args.out else None
    tmp = tempfile.mkdtemp(prefix="osc-download-", dir=args.work_dir)

    try:
        for seq in range(first, first + args.count):
            augmenter = Augmenter(ApiClient(min_interval=args.min_interval), work_dir=args.work_dir)
            start = time.monotonic()
            n = 0
            with open_osc(session, seq, args.interval, tmp) as osc:
                for feature in augmenter.process(osc):
                    n += 1
                    if out:
                        out.write(json.dumps(feature, separators=(",", ":")) + "\n")
            api = augmenter.api
            print(f"{args.interval} seq {seq}: {n} features in {time.monotonic() - start:.0f}s, "
                  f"{sum(api.requests.values())} API requests ({api.bytes / 1e6:.1f} MB, "
                  f"{api.seconds:.0f}s waiting), peak RSS {peak_rss_mb():.0f} MB")
            print("  requests:", dict(sorted(api.requests.items())))
            print("  stats:   ", dict(sorted(augmenter.stats.items())))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if out:
            out.close()


if __name__ == "__main__":
    main()
