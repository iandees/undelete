"""Parse augmented diff XML and extract deleted OSM objects as GeoJSON features."""

from lxml import etree


def parse_adiff(source):
    """Parse augmented diff XML and yield GeoJSON features for deleted objects.

    source can be a file-like object (for streaming) or bytes.
    """
    context = etree.iterparse(source, events=("end",), tag="action")

    for _, action in context:
        if action.get("type") != "delete":
            action.clear()
            continue

        old_elem = action.find("old")
        new_elem = action.find("new")
        if old_elem is None or new_elem is None:
            action.clear()
            continue

        old_obj = old_elem[0]
        new_obj = new_elem[0]
        obj_type = old_obj.tag

        tags = {tag.get("k"): tag.get("v") for tag in old_obj.iterchildren("tag")}

        geometry = _extract_geometry(obj_type, old_obj)
        if geometry is None:
            action.clear()
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
        action.clear()
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
        if len(nds) >= 4 and nds[0].get("ref") == nds[-1].get("ref"):
            return {"type": "Polygon", "coordinates": [coords]}
        else:
            return {"type": "LineString", "coordinates": coords}

    elif obj_type == "relation":
        bounds = elem.find("bounds")
        if bounds is not None:
            min_lon = float(bounds.get("minlon"))
            max_lon = float(bounds.get("maxlon"))
            min_lat = float(bounds.get("minlat"))
            max_lat = float(bounds.get("maxlat"))
            center_lon = (min_lon + max_lon) / 2
            center_lat = (min_lat + max_lat) / 2
            return {"type": "Point", "coordinates": [center_lon, center_lat]}
        return None

    return None
