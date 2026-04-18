"""Parse augmented diff XML and extract deleted OSM objects as GeoJSON features."""

import xml.sax
import xml.sax.handler


def parse_adiff(source):
    """Parse augmented diff XML and yield GeoJSON features for deleted objects.

    Uses SAX parsing for constant memory usage regardless of file size.
    source can be a file-like object (for streaming) or bytes.
    """
    handler = _AdiffHandler()
    # For bytes input, wrap in a BytesIO
    if isinstance(source, bytes):
        import io
        source = io.BytesIO(source)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)
    parser.parse(source)
    return handler.features


class _AdiffHandler(xml.sax.handler.ContentHandler):
    """SAX handler that extracts deleted OSM objects from augmented diffs."""

    def __init__(self):
        self.features = []
        # Current action state
        self._action_type = None
        self._in_old = False
        self._in_new = False
        # Old element state
        self._old_tag = None
        self._old_attrs = {}
        self._old_tags = {}
        self._old_nds = []
        self._old_bounds = None
        # New element state
        self._new_attrs = {}

    def startElement(self, name, attrs):
        if name == "action":
            self._action_type = attrs.get("type")
            self._in_old = False
            self._in_new = False
            self._old_tag = None
            self._old_attrs = {}
            self._old_tags = {}
            self._old_nds = []
            self._old_bounds = None
            self._new_attrs = {}
        elif name == "old":
            self._in_old = True
        elif name == "new":
            self._in_new = True
            self._in_old = False
        elif self._action_type == "delete":
            if self._in_old:
                if name in ("node", "way", "relation"):
                    self._old_tag = name
                    self._old_attrs = dict(attrs)
                elif name == "tag":
                    self._old_tags[attrs.get("k")] = attrs.get("v")
                elif name == "nd":
                    self._old_nds.append(dict(attrs))
                elif name == "bounds":
                    self._old_bounds = dict(attrs)
            elif self._in_new:
                if name in ("node", "way", "relation"):
                    self._new_attrs = dict(attrs)

    def endElement(self, name):
        if name == "old":
            self._in_old = False
        elif name == "new":
            self._in_new = False
        elif name == "action":
            if self._action_type == "delete" and self._old_tag:
                self._emit_feature()
            self._action_type = None

    def _emit_feature(self):
        geometry = self._extract_geometry()
        if geometry is None:
            return

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "osm_type": self._old_tag,
                "osm_id": int(self._old_attrs.get("id", 0)),
                "version": int(self._old_attrs.get("version", 0)),
                "tags": self._old_tags,
                "deleted_by": self._new_attrs.get("user", ""),
                "deleted_uid": int(self._new_attrs.get("uid", 0)),
                "deleted_changeset": int(self._new_attrs.get("changeset", 0)),
                "deleted_at": self._new_attrs.get("timestamp", ""),
            },
        }
        self.features.append(feature)

    def _extract_geometry(self):
        if self._old_tag == "node":
            lon = self._old_attrs.get("lon")
            lat = self._old_attrs.get("lat")
            if lon is None or lat is None:
                return None
            return {"type": "Point", "coordinates": [float(lon), float(lat)]}

        elif self._old_tag == "way":
            if not self._old_nds:
                return None
            coords = [
                [float(nd.get("lon")), float(nd.get("lat"))]
                for nd in self._old_nds
            ]
            nds = self._old_nds
            if len(nds) >= 4 and nds[0].get("ref") == nds[-1].get("ref"):
                return {"type": "Polygon", "coordinates": [coords]}
            else:
                return {"type": "LineString", "coordinates": coords}

        elif self._old_tag == "relation":
            if self._old_bounds:
                min_lon = float(self._old_bounds.get("minlon"))
                max_lon = float(self._old_bounds.get("maxlon"))
                min_lat = float(self._old_bounds.get("minlat"))
                max_lat = float(self._old_bounds.get("maxlat"))
                center_lon = (min_lon + max_lon) / 2
                center_lat = (min_lat + max_lat) / 2
                return {"type": "Point", "coordinates": [center_lon, center_lat]}
            return None

        return None
