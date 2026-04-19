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
        resp = requests.get(STATE_URL, timeout=30)
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

    def fetch_and_process(self, seq: int) -> int | None:
        """Fetch one adiff by sequence number, extract all changes.

        Returns the number of changes found, or None if the adiff
        is not yet available (404).
        """
        url = ADIFF_URL.format(seq=seq)
        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code == 404:
            resp.close()
            return None
        resp.raise_for_status()
        resp.raw.decode_content = True

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        count = 0
        for feature in parse_adiff(resp.raw):
            self.writer.append(feature, date_str=today)
            count += 1
        resp.close()

        return count
