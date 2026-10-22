"""Where replication files are downloaded from.

planet.openstreetmap.org answers every file request with a 302 to the same files on an AWS S3
bucket. That Apache host is the weak point: several simultaneous new connections to it time
out (urllib3 "Retrying" warnings), while the bucket handles them without trouble. So by
default we fetch from the bucket directly. Set REPLICATION_MIRROR=planet to go through
planet.openstreetmap.org instead.

The two layouts differ for changesets:

    planet:  <planet>/replication/changesets/<rel>
    s3:      <bucket>/changesets/replication/minute/<rel>

and for elements the bucket just adds a `planet/` prefix:

    planet:  <planet>/replication/<interval>/<rel>
    s3:      <bucket>/planet/replication/<interval>/<rel>
"""

import os

PLANET = "https://planet.openstreetmap.org/replication"
S3 = "https://osm-planet-eu-central-1.s3.dualstack.eu-central-1.amazonaws.com"


def _use_s3() -> bool:
    mirror = os.environ.get("REPLICATION_MIRROR", "s3").strip().lower()
    if mirror not in ("s3", "planet"):
        raise ValueError(f"REPLICATION_MIRROR must be 's3' or 'planet', not {mirror!r}")
    return mirror == "s3"


def element_url(interval: str, rel: str) -> str:
    """URL of an element replication file, e.g. ('hour', 'state.txt') or ('day', '000/005/150.osc.gz')."""
    if _use_s3():
        return f"{S3}/planet/replication/{interval}/{rel}"
    return f"{PLANET}/{interval}/{rel}"


def changeset_url(rel: str) -> str:
    """URL of a changeset replication file, e.g. 'state.yaml' or '007/235/370.osm.gz'."""
    if _use_s3():
        return f"{S3}/changesets/replication/minute/{rel}"
    return f"{PLANET}/changesets/{rel}"
