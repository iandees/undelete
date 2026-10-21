"""Watch OSM changeset replication feed and write daily JSONL files.

Each changeset is filed under the date it was *created*. That never changes, so every
version of a changeset (open ones are re-emitted each minute while being edited) lands in
one daily file and the parquet builder's dedup keeps exactly one row per changeset.

A date is only written to until it is "frozen": open changesets can be updated for about
24 hours, so a date is final two days after it starts (plus a margin). Later updates to
changesets that old (almost always a new comment) are dropped and counted: appending them
would recreate a tiny file for a day that has already been published, and rebuilding it
would replace that day's complete parquet with just those rows.
"""

import gzip
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from daemon.changeset_parser import parse_changesets

logger = logging.getLogger(__name__)

STATE_URL = "https://planet.openstreetmap.org/replication/changesets/state.yaml"
CHANGESET_URL = "https://planet.openstreetmap.org/replication/changesets/{path}.osm.gz"
CHANGESET_STATE_URL = "https://planet.openstreetmap.org/replication/changesets/{path}.state.txt"

# A date stops accepting changesets this many days after it begins (open changesets can be
# edited for ~24h) plus a margin for replication lag.
FREEZE_DAYS = float(os.environ.get("CHANGESET_FREEZE_DAYS", "2"))
FREEZE_MARGIN = 3600
# When keeping up (about one file a minute) log a summary this often instead of every file;
# a batch that catches up on several files is always logged, as one line.
LOG_INTERVAL = 600


def _seq_to_path(seq: int) -> str:
    """Convert sequence number to zero-padded path: 6978450 -> 006/978/450."""
    s = f"{seq:09d}"
    return f"{s[0:3]}/{s[3:6]}/{s[6:9]}"


class ChangesetWatcher:
    def __init__(self, data_dir: Path, workers: int = 8):
        self.workers = workers
        self.log_interval = LOG_INTERVAL
        self._pending_log = {"files": 0, "changesets": 0, "dropped": 0, "first": None, "last": None}
        self._last_log = time.monotonic()
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

    def save_state(self, seq: int, epoch: int | None = None):
        """Save the last processed changeset sequence number (and its replication time)."""
        (self.state_dir / "last_changeset_seq.txt").write_text(str(seq))
        if epoch is not None:
            (self.state_dir / "last_changeset_epoch.txt").write_text(f"{seq} {epoch}")

    def load_epoch(self, seq: int) -> int | None:
        """Replication time of `seq` if we saved it (it must be for exactly this sequence)."""
        path = self.state_dir / "last_changeset_epoch.txt"
        if path.exists():
            saved_seq, _, epoch = path.read_text().partition(" ")
            if saved_seq == str(seq) and epoch.isdigit():
                return int(epoch)
        return None

    def seq_epoch(self, seq: int) -> int:
        """When replication file `seq` was generated (from its state file), as epoch seconds."""
        resp = self._session().get(
            CHANGESET_STATE_URL.format(path=_seq_to_path(seq)), timeout=(10, 30))
        resp.raise_for_status()
        last_run = yaml.safe_load(resp.text)["last_run"]
        if isinstance(last_run, str):  # not parsed as a timestamp
            last_run = datetime.fromisoformat(last_run.replace(" +00:00", "+00:00")[:26] + "+00:00")
        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
        return int(last_run.timestamp())

    # ---- freezing ----

    @staticmethod
    def is_frozen(date_str: str, now_epoch: int) -> bool:
        """True once `date_str` no longer accepts changesets, as of replication time `now_epoch`."""
        day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        return now_epoch - FREEZE_MARGIN >= day_start + FREEZE_DAYS * 86400

    def complete_before(self) -> str:
        """ISO date: every date earlier than this is frozen, so its file will not change."""
        last_seq = self.load_state()
        epoch = self.load_epoch(last_seq) if last_seq is not None else None
        if epoch is None:
            return "0000-00-00"
        last_frozen = datetime.fromtimestamp(
            epoch - FREEZE_MARGIN - FREEZE_DAYS * 86400, timezone.utc).date()
        return (last_frozen + timedelta(days=1)).isoformat()

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

    def _write(self, changesets: list[dict], now_epoch: int | None = None) -> int:
        """Append changesets to the file for the date each was created. Returns how many
        were dropped because their date is already frozen as of `now_epoch`."""
        fallback = datetime.fromtimestamp(now_epoch or datetime.now(timezone.utc).timestamp(),
                                          timezone.utc).strftime("%Y-%m-%d")
        by_date, dropped = {}, 0
        for cs in changesets:
            date = (cs.get("created_at") or "")[:10]
            try:
                datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                date = fallback
            if now_epoch is not None and self.is_frozen(date, now_epoch):
                dropped += 1
                continue
            by_date.setdefault(date, []).append(cs)
        for date, items in by_date.items():
            with open(self.output_dir / f"{date}.jsonl", "a") as f:
                for cs in items:
                    f.write(json.dumps(cs, separators=(",", ":")) + "\n")
        return dropped

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

        Changesets are filed by creation date; ones whose date is already frozen as of the
        batch's starting position are dropped (see the module docstring).

        Returns (new last_seq, number of sequences processed).
        """
        seqs = range(last_seq + 1, min(latest_seq, last_seq + max_seqs) + 1)
        if not seqs:
            return last_seq, 0
        start_epoch = self.load_epoch(last_seq)
        if start_epoch is None:
            try:
                start_epoch = self.seq_epoch(last_seq)
            except Exception:
                logger.exception("Could not get the time of changeset seq %d; will retry", last_seq)
                return last_seq, 0
        done = dropped = total = 0
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
                        # Files in a batch are minutes apart, so the starting time is a
                        # safe stand-in: it can only accept a little more, never less.
                        dropped += self._write(changesets, start_epoch)
                        total += len(changesets)
                        logger.debug("Changeset seq %d: %d changesets", seq, len(changesets))
                    last_seq = seq
                    self.save_state(seq)
                    done += 1
            finally:
                for future in futures:
                    future.cancel()  # don't start downloads we're not going to use
        if done:
            try:
                self.save_state(last_seq, self.seq_epoch(last_seq))
            except Exception:
                # Without the position time nothing is finalized until a later batch records it
                logger.exception("Could not record the time of changeset seq %d", last_seq)
            self._log_progress(last_seq - done + 1, last_seq, done, total, dropped)
        return last_seq, done

    def _log_progress(self, first, last, files, changesets, dropped):
        """One summary line per catch-up batch; when keeping up, one per `log_interval`."""
        pending = self._pending_log
        pending["files"] += files
        pending["changesets"] += changesets
        pending["dropped"] += dropped
        pending["first"] = first if pending["first"] is None else pending["first"]
        pending["last"] = last
        now = time.monotonic()
        if files < 2 and now - self._last_log < self.log_interval:
            return  # keeping up: fold into the next summary
        logger.info("Changesets: %d file%s (seq %d..%d), %d changesets%s",
                    pending["files"], "" if pending["files"] == 1 else "s",
                    pending["first"], pending["last"], pending["changesets"],
                    f", {pending['dropped']} updates to changesets older than "
                    f"{FREEZE_DAYS:g} days dropped" if pending["dropped"] else "")
        self._pending_log = {"files": 0, "changesets": 0, "dropped": 0, "first": None, "last": None}
        self._last_log = now
