"""Watch OSM replication and extract every change, using day, hour and minute diffs.

State is one position (``<data_dir>/state/osc_position.json``): the minute sequence and/or
hour/day label time we have covered through. Each pass asks ``Planner`` for the route to
"now" (the fewest files that get there), runs each diff through ``Augmenter`` (planet
osmChange + OSM API lookups -> augmented-diff-style features) and appends the features to
``<data_dir>/deletions/<date>.geojsonl``, where <date> is the feature's own timestamp, so a
day diff fills the right files.
"""

import gzip
import json
import logging
import os
import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from daemon.geojson_writer import GeoJSONWriter
from daemon.osc_augmenter import USER_AGENT, ApiClient, Augmenter, download_osc
from daemon.replication_planner import HttpStateSource, Planner, Position, Step

logger = logging.getLogger(__name__)

DEFAULT_MAX_DAYS = float(os.environ.get("CATCHUP_MAX_DAYS", "7"))
# Features are timestamped when the edit was uploaded, which can trail a diff's label by
# a minute or so; a date is only treated as complete this long after its last edit.
COMPLETE_MARGIN = 3600
RETRY_DELAY = 60


def _default_planner(session):
    return Planner(HttpStateSource(session))


def _default_download(step: Step, spool: str) -> str:
    session = requests.Session()  # sessions aren't shared across threads
    session.headers["User-Agent"] = USER_AGENT
    return download_osc(session, step.seq, step.interval, spool)


class Watcher:
    def __init__(self, data_dir: Path, max_days=DEFAULT_MAX_DAYS, session=None, api=None,
                 download_workers=2, lookahead=3, planner_factory=None, download=None,
                 augmenter_factory=None):
        self.data_dir = Path(data_dir)
        self.state_dir = self.data_dir / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir = self.data_dir / "work"  # big temp files live on the data volume
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.writer = GeoJSONWriter(self.data_dir / "deletions")
        self.max_days = max_days
        self.download_workers = download_workers
        self.lookahead = lookahead  # planet files fetched ahead of the step being processed
        self.api = api or ApiClient()
        if session is None:
            session = requests.Session()
            session.headers["User-Agent"] = USER_AGENT
        self.planner_factory = planner_factory or (lambda: _default_planner(session))
        self.download = download or _default_download
        self.augmenter_factory = augmenter_factory or (
            lambda api, work_dir: Augmenter(api, work_dir=work_dir))
        # Held while appending to daily files and by anything that reads them (parquet
        # builds), so a reader never sees a half-written batch.
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._clean_work_dir()

    def _clean_work_dir(self):
        """Remove temp dirs left behind by a previous run that was killed."""
        for leftover in self.work_dir.iterdir():
            shutil.rmtree(leftover, ignore_errors=True)

    # ---- position ----

    @property
    def state_file(self):
        return self.state_dir / "osc_position.json"

    def load_position(self, planner: Planner) -> Position:
        """Where to resume, clamped to the allowed catch-up window."""
        lm_seq, lm_ts = planner.latest["minute"]
        if self.state_file.exists():
            saved = json.loads(self.state_file.read_text())
            position = Position(minute=saved.get("minute_seq"), label=saved.get("label"))
        elif (self.state_dir / "last_seq.txt").exists():
            # Older versions tracked a planet minute sequence number; it's the same numbering
            position = Position(minute=int((self.state_dir / "last_seq.txt").read_text().strip()))
            logger.info("Migrating saved minute sequence %d", position.minute)
        else:
            logger.info("No saved position, starting from latest minute %d", lm_seq)
            return Position(minute=lm_seq)
        oldest = lm_ts - int(self.max_days * 86400)
        if planner.epoch(position) < oldest:
            skip_to = planner.last_minute_at_or_before(oldest)
            logger.warning("Position is more than %s days behind; skipping ahead to minute %d "
                           "(this leaves a gap in the data)", self.max_days, skip_to)
            return Position(minute=skip_to)
        return position

    def save_position(self, position: Position, epoch: int):
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps({"minute_seq": position.minute, "label": position.label,
                                   "epoch": epoch}))
        tmp.replace(self.state_file)  # atomic

    def complete_before(self) -> str:
        """ISO date: every feature dated earlier than this has been written."""
        if not self.state_file.exists():
            return "0000-00-00"
        epoch = json.loads(self.state_file.read_text()).get("epoch")
        if epoch is None:
            return "0000-00-00"
        return datetime.fromtimestamp(epoch - COMPLETE_MARGIN, timezone.utc).strftime("%Y-%m-%d")

    # ---- running ----

    def catch_up(self, max_steps=None):
        """Process diffs until we reach "now" (or `max_steps`). Returns the steps completed.

        The whole route is planned from the last processed time. The planet files for the
        next few steps download in parallel while the current one is processed (they're
        static, so that's cheap); all OSM API traffic stays on the single throttled
        ApiClient. When a route finishes the planner is asked again, since newer minutes
        will have been published meanwhile.
        """
        done, exclude = [], set()
        spool = Path(tempfile.mkdtemp(prefix="spool-", dir=self.work_dir))
        pool = ThreadPoolExecutor(max_workers=self.download_workers)
        pending = {}  # (interval, seq) -> Future[path]
        try:
            planner = self.planner_factory()
            position = self.load_position(planner)
            while max_steps is None or len(done) < max_steps:
                route = planner.route(position, exclude=exclude)
                if not route:
                    break
                refresh = True
                for i, step in enumerate(route):
                    if max_steps is not None and len(done) >= max_steps:
                        refresh = False
                        break
                    for upcoming in route[i:i + self.lookahead + 1]:
                        key = (upcoming.interval, upcoming.seq)
                        if key not in pending:
                            pending[key] = pool.submit(self.download, upcoming, str(spool))
                    try:
                        count = self.process_step(step, pending.pop((step.interval, step.seq)).result())
                    except requests.HTTPError as exc:
                        if step.interval != "minute" and exc.response is not None \
                                and exc.response.status_code == 404:
                            logger.warning("%s diff %d not downloadable, using finer diffs",
                                           step.interval, step.seq)
                            exclude.add("day" if step.interval == "day" else "hour")
                            self._drop_pending(pending)
                            break  # re-route from where we are
                        raise
                    position = step.end
                    epoch = planner.epoch(position)
                    self.save_position(position, epoch)
                    done.append(step)
                    exclude.clear()
                    logger.info("%s %d done: %d features, %.1f hours behind", step.interval,
                                step.seq, count, (planner.latest["minute"][1] - epoch) / 3600)
                if refresh:
                    planner = self.planner_factory()  # newer diffs may exist by now
        finally:
            self._drop_pending(pending)
            pool.shutdown(wait=True, cancel_futures=True)
            shutil.rmtree(spool, ignore_errors=True)
        return done

    @staticmethod
    def _drop_pending(pending):
        for future in pending.values():
            if not future.cancel():
                try:
                    os.remove(future.result())
                except Exception:
                    pass
        pending.clear()

    def process_step(self, step: Step, osc_path) -> int:
        """Run one diff. Features are staged per date and only appended once the whole
        diff succeeded, so a failed step can be retried without duplicates."""
        stage = Path(tempfile.mkdtemp(prefix="stage-", dir=self.work_dir))
        try:
            handles, count = {}, 0
            augmenter = self.augmenter_factory(self.api, str(self.work_dir))
            with gzip.open(osc_path, "rb") as osc:
                for feature in augmenter.process(osc):
                    date = feature["properties"]["timestamp"][:10]
                    if date not in handles:
                        handles[date] = open(stage / f"{date}.geojsonl", "w")
                    handles[date].write(json.dumps(feature, separators=(",", ":")) + "\n")
                    count += 1
            for handle in handles.values():
                handle.close()
            with self.lock:
                for date in sorted(handles):
                    with open(stage / f"{date}.geojsonl", "rb") as src, \
                            open(self.writer.output_dir / f"{date}.geojsonl", "ab") as dst:
                        shutil.copyfileobj(src, dst)
            return count
        finally:
            shutil.rmtree(stage, ignore_errors=True)
            try:
                os.remove(osc_path)  # free spool space as soon as the step is done
            except OSError:
                pass

    # ---- background thread ----

    def run_forever(self, poll_interval=60):
        while not self._stop.is_set():
            delay = poll_interval
            try:
                self.catch_up()
            except Exception:
                logger.exception("Catch-up failed; retrying")
                delay = RETRY_DELAY
            self._stop.wait(delay)

    def start(self, poll_interval=60):
        """Run catch-up passes in a background thread (a day diff alone takes hours)."""
        self._thread = threading.Thread(target=self.run_forever, args=(poll_interval,),
                                        name="watcher", daemon=True)
        self._thread.start()
        return self._thread

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)


def main():
    """Print the route the watcher would take to "now" without processing anything."""
    import argparse
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "./data"))
    parser.add_argument("--days-back", type=float, help="pretend we are this far behind")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    watcher = Watcher(Path(args.data_dir))
    planner = watcher.planner_factory()
    if args.days_back is not None:
        start = Position(minute=planner.last_minute_at_or_before(
            planner.latest["minute"][1] - int(args.days_back * 86400)))
    else:
        start = watcher.load_position(planner)
    route = planner.route(start)
    runs = []
    for step in route:
        if runs and runs[-1][0] == step.interval:
            runs[-1][2], runs[-1][3] = step.seq, runs[-1][3] + 1
        else:
            runs.append([step.interval, step.seq, step.seq, 1])
    behind = (planner.latest["minute"][1] - planner.epoch(start)) / 3600
    print(f"{behind:.1f} hours behind = {planner.latest_minute - planner.minute_of(start)} "
          f"minute diffs; route is {len(route)} files:")
    for interval, first, last, count in runs:
        print(f"  {count:4d} {interval:6} {first}" + (f" .. {last}" if count > 1 else ""))


if __name__ == "__main__":
    main()
