"""Watch OSM changeset replication feed and write daily JSONL files."""

import gzip
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from daemon.changeset_parser import parse_changesets

logger = logging.getLogger(__name__)

STATE_URL = "https://planet.openstreetmap.org/replication/changesets/state.yaml"
CHANGESET_URL = "https://planet.openstreetmap.org/replication/changesets/{path}.osm.gz"


def _seq_to_path(seq: int) -> str:
    """Convert sequence number to zero-padded path: 6978450 -> 006/978/450."""
    s = f"{seq:09d}"
    return f"{s[0:3]}/{s[3:6]}/{s[6:9]}"


class ChangesetWatcher:
    def __init__(self, data_dir: Path, workers: int = 8):
        self.workers = workers
        self._local = threading.local()  # one requests.Session per worker thread
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

    def _session(self):
        if not hasattr(self._local, "session"):
            session = requests.Session()
            # Transient trouble (connection errors, timeouts, 429/5xx) is retried per file
            # with backoff, so one blip doesn't end a whole parallel batch.
            retry = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
                          respect_retry_after_header=True)
            session.mount("https://", HTTPAdapter(max_retries=retry))
            self._local.session = session
        return self._local.session

    def fetch(self, seq: int) -> list[dict] | None:
        """Download and parse one changeset replication file (no writing).

        Returns the changesets in it, or None if it is not published yet (404).
        """
        url = CHANGESET_URL.format(path=_seq_to_path(seq))
        resp = self._session().get(url, timeout=(10, 60))
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return parse_changesets(gzip.decompress(resp.content))

    def _write(self, changesets: list[dict]):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        daily_file = self.output_dir / f"{today}.jsonl"
        with open(daily_file, "a") as f:
            for cs in changesets:
                f.write(json.dumps(cs, separators=(",", ":")) + "\n")

    def fetch_and_process(self, seq: int) -> int | None:
        """Fetch one changeset replication file and append it to today's file.

        Returns the number of changesets found, or None if not yet available (404).
        """
        changesets = self.fetch(seq)
        if changesets is None:
            return None
        if changesets:
            self._write(changesets)
        return len(changesets)

    def catch_up(self, last_seq: int, latest_seq: int, max_seqs: int = 100) -> tuple[int, int]:
        """Process up to `max_seqs` sequences after `last_seq`, downloading in parallel.

        Files are fetched and parsed concurrently by `self.workers` threads but committed
        strictly in sequence order, and the saved position only ever advances over a
        contiguous run of written files. A file that is not published yet (404) or that
        fails ends the batch there, so a retry never skips or duplicates anything.

        Returns (new last_seq, number of sequences processed).
        """
        seqs = range(last_seq + 1, min(latest_seq, last_seq + max_seqs) + 1)
        if not seqs:
            return last_seq, 0
        done = 0
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self.fetch, seq) for seq in seqs]
            try:
                for seq, future in zip(seqs, futures):
                    try:
                        changesets = future.result()
                    except Exception:
                        logger.exception("Failed to process changeset seq %d", seq)
                        break
                    if changesets is None:
                        break
                    if changesets:
                        self._write(changesets)
                        logger.info("Changeset seq %d: %d changesets", seq, len(changesets))
                    last_seq = seq
                    self.save_state(seq)
                    done += 1
            finally:
                for future in futures:
                    future.cancel()  # don't start downloads we're not going to use
        return last_seq, done
