"""Watch OSM changeset replication feed and write daily JSONL files."""

import gzip
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

from daemon.changeset_parser import parse_changesets

logger = logging.getLogger(__name__)

STATE_URL = "https://planet.openstreetmap.org/replication/changesets/state.yaml"
CHANGESET_URL = "https://planet.openstreetmap.org/replication/changesets/{path}.osm.gz"


def _seq_to_path(seq: int) -> str:
    """Convert sequence number to zero-padded path: 6978450 -> 006/978/450."""
    s = f"{seq:09d}"
    return f"{s[0:3]}/{s[3:6]}/{s[6:9]}"


class ChangesetWatcher:
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        self.state_dir = self.data_dir / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir = self.data_dir / "changesets"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_latest_sequence(self) -> int:
        """Get the latest available sequence number from changeset replication."""
        resp = requests.get(STATE_URL, timeout=30)
        resp.raise_for_status()
        state = yaml.safe_load(resp.text)
        return int(state["sequence"])

    def load_state(self) -> int | None:
        """Load the last processed changeset sequence number from disk."""
        state_file = self.state_dir / "last_changeset_seq.txt"
        if state_file.exists():
            return int(state_file.read_text().strip())
        return None

    def save_state(self, seq: int):
        """Save the last processed changeset sequence number to disk."""
        state_file = self.state_dir / "last_changeset_seq.txt"
        state_file.write_text(str(seq))

    def fetch_and_process(self, seq: int) -> int | None:
        """Fetch one changeset replication file by sequence number.

        Returns the number of changesets found, or None if not yet available (404).
        """
        path = _seq_to_path(seq)
        url = CHANGESET_URL.format(path=path)
        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code == 404:
            resp.close()
            return None
        resp.raise_for_status()

        raw_bytes = gzip.decompress(resp.content)
        resp.close()

        changesets = parse_changesets(raw_bytes)
        if not changesets:
            return 0

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_file = self.output_dir / f"{today}.jsonl"
        with open(daily_file, "a") as f:
            for cs in changesets:
                f.write(json.dumps(cs, separators=(",", ":")) + "\n")

        return len(changesets)
