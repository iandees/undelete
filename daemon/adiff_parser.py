"""Parse augmented diff XML and extract OSM objects as GeoJSON features."""

import xml.sax
import xml.sax.handler


def parse_adiff(source):
    """Parse augmented diff XML and yield GeoJSON features for all action types.

    Captures create, modify, and delete actions.
    Uses SAX parsing for constant memory usage regardless of file size.
    source can be a file-like object (for streaming) or bytes.
    """
    handler = _AdiffHandler()
    if isinstance(source, bytes):
        import io
        source = io.BytesIO(source)
    parser = xml.sax.make_parser()
    parser.setContentHandler(handler)
    parser.parse(source)
    return handler.features


def _build_geometry(osm_type, attrs, nds, bounds):
    """Build a GeoJSON geometry from element data."""
    if osm_type == "node":
        lon = attrs.get("lon")
        lat = attrs.get("lat")
        if lon is None or lat is None:
            return None
        return {"type": "Point", "coordinates": [float(lon), float(lat)]}

    elif osm_type == "way":
        if not nds:
            return None
        coords = [[float(nd["lon"]), float(nd["lat"])] for nd in nds]
        if len(nds) >= 4 and nds[0].get("ref") == nds[-1].get("ref"):
            return {"type": "Polygon", "coordinates": [coords]}
        return {"type": "LineString", "coordinates": coords}

    elif osm_type == "relation":
        if bounds:
            min_lon = float(bounds["minlon"])
            max_lon = float(bounds["maxlon"])
            min_lat = float(bounds["minlat"])
            max_lat = float(bounds["maxlat"])
            return {
                "type": "Point",
                "coordinates": [(min_lon + max_lon) / 2, (min_lat + max_lat) / 2],
            }
        return None

    return None


class _AdiffHandler(xml.sax.handler.ContentHandler):
    """SAX handler that extracts OSM objects from augmented diffs."""

    def __init__(self):
        self.features = []
        self._action_type = None
        self._in_old = False
        self._in_new = False
        # Old element state
        self._old_osm_type = None
        self._old_attrs = {}
        self._old_tags = {}
        self._old_nds = []
        self._old_bounds = None
        # New element state
        self._new_osm_type = None
        self._new_attrs = {}
        self._new_tags = {}
        self._new_nds = []
        self._new_bounds = None

    def _reset_action(self):
        self._action_type = None
        self._in_old = False
        self._in_new = False
        self._old_osm_type = None
        self._old_attrs = {}
        self._old_tags = {}
        self._old_nds = []
        self._old_bounds = None
        self._new_osm_type = None
        self._new_attrs = {}
        self._new_tags = {}
        self._new_nds = []
        self._new_bounds = None

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
        elif self._in_old:
            self._handle_element(name, attrs, "old")
        elif self._in_new:
            self._handle_element(name, attrs, "new")

    def _handle_element(self, name, attrs, side):
        if name in ("node", "way", "relation"):
            if side == "old":
                self._old_osm_type = name
                self._old_attrs = dict(attrs)
            else:
                self._new_osm_type = name
                self._new_attrs = dict(attrs)
        elif name == "tag":
            target = self._old_tags if side == "old" else self._new_tags
            target[attrs.get("k")] = attrs.get("v")
        elif name == "nd":
            target = self._old_nds if side == "old" else self._new_nds
            target.append(dict(attrs))
        elif name == "bounds":
            if side == "old":
                self._old_bounds = dict(attrs)
            else:
                self._new_bounds = dict(attrs)

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
        action = self._action_type

        if action == "create":
            # Everything from <new>
            osm_type = self._new_osm_type
            if not osm_type:
                return
            geometry = _build_geometry(osm_type, self._new_attrs, self._new_nds, self._new_bounds)
            if geometry is None:
                return
            attrs = self._new_attrs
            tags = self._new_tags
            old_tags = None
            old_geometry = None

        elif action == "modify":
            # Primary state from <new>, old state from <old>
            osm_type = self._new_osm_type or self._old_osm_type
            if not osm_type:
                return
            geometry = _build_geometry(
                self._new_osm_type or self._old_osm_type,
                self._new_attrs, self._new_nds, self._new_bounds,
            )
            if geometry is None:
                return
            attrs = self._new_attrs
            tags = self._new_tags
            old_tags = self._old_tags if self._old_tags else {}
            old_geometry = _build_geometry(
                self._old_osm_type or self._new_osm_type,
                self._old_attrs, self._old_nds, self._old_bounds,
            )

        elif action == "delete":
            # Geometry/tags from <old>, metadata from <new>
            osm_type = self._old_osm_type
            if not osm_type:
                return
            geometry = _build_geometry(osm_type, self._old_attrs, self._old_nds, self._old_bounds)
            if geometry is None:
                return
            # Use old attrs for version (last visible), new attrs for who/when
            attrs = self._new_attrs
            tags = self._old_tags
            old_tags = None
            old_geometry = None
            # For delete, version comes from old (last visible version)
            version = int(self._old_attrs.get("version", 0))
        else:
            return

        if action != "delete":
            version = int(attrs.get("version", 0))

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "action": action,
                "osm_type": osm_type,
                "osm_id": int(attrs.get("id", self._old_attrs.get("id", 0))),
                "version": version,
                "changeset": int(attrs.get("changeset", 0)),
                "user": attrs.get("user", ""),
                "uid": int(attrs.get("uid", 0)),
                "timestamp": attrs.get("timestamp", ""),
                "tags": tags,
                "old_tags": old_tags,
                "old_geometry": old_geometry,
            },
        }
        self.features.append(feature)
