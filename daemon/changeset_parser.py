"""Parse OSM changeset replication XML files."""

import logging
from xml.sax import make_parser, handler, parseString

logger = logging.getLogger(__name__)


def parse_changesets(source):
    """Parse changeset replication XML and return a list of changeset dicts.

    Each dict has: id, created_at, closed_at, open, num_changes, user, uid,
    min_lat, max_lat, min_lon, max_lon, comments_count, tags.
    """
    h = _ChangesetHandler()
    parser = make_parser()
    parser.setContentHandler(h)
    if isinstance(source, (bytes, str)):
        parseString(source if isinstance(source, bytes) else source.encode(), h)
    else:
        parser.parse(source)
    return h.changesets


class _ChangesetHandler(handler.ContentHandler):
    def __init__(self):
        self.changesets = []
        self._current = None

    def startElement(self, name, attrs):
        if name == "changeset":
            self._current = {
                "id": int(attrs["id"]),
                "created_at": attrs.get("created_at", ""),
                "closed_at": attrs.get("closed_at", ""),
                "open": attrs.get("open", "false") == "true",
                "num_changes": int(attrs.get("num_changes", 0)),
                "user": attrs.get("user", ""),
                "uid": int(attrs.get("uid", 0)),
                "comments_count": int(attrs.get("comments_count", 0)),
                "tags": {},
            }
            # Bbox — may be absent for changesets with no spatial data
            if "min_lat" in attrs:
                self._current["min_lat"] = float(attrs["min_lat"])
                self._current["max_lat"] = float(attrs["max_lat"])
                self._current["min_lon"] = float(attrs["min_lon"])
                self._current["max_lon"] = float(attrs["max_lon"])
            else:
                self._current["min_lat"] = None
                self._current["max_lat"] = None
                self._current["min_lon"] = None
                self._current["max_lon"] = None

        elif name == "tag" and self._current is not None:
            self._current["tags"][attrs["k"]] = attrs["v"]

    def endElement(self, name):
        if name == "changeset" and self._current is not None:
            self.changesets.append(self._current)
            self._current = None
