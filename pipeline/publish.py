"""Publish daily raw files as parquet, merging late rows into what is already published.

Each kind of data (element changes, changesets) accumulates in one raw file per date. A
date is *live* until the watcher has moved past it (the caller's `cutoff`), and *settled*
afterwards. Publishing works like this:

* Live date: the raw file holds the whole day so far, so the parquet is rebuilt from it each
  time it changes.
* Settled date: publish, then delete the raw file. Rows that arrive for that date later (a
  comment on an old changeset, a backfill) create a new raw file holding only the new rows.
  Building a parquet from just those would replace the published day with a handful of rows,
  so they are *merged* into the published parquet instead. R2 is treated as the source of
  truth for what has been published, since local copies are pruned after the retention period.

A raw file is only deleted once its exact version is in the published parquet, so nothing is
lost if an upload fails; the next run retries it.
"""

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_MERGE_INTERVAL = 3600


class DailyPublisher:
    def __init__(self, name, raw_dir, raw_suffix, parquet_dir, builder, remote_prefix,
                 uploader=None, work_dir=None, merge_interval=DEFAULT_MERGE_INTERVAL,
                 clock=time.monotonic):
        self.name = name
        self.raw_dir = Path(raw_dir)
        self.raw_suffix = raw_suffix
        self.parquet_dir = Path(parquet_dir)
        self.builder = builder
        self.remote_prefix = remote_prefix
        self.uploader = uploader
        self.work_dir = Path(work_dir) if work_dir else self.parquet_dir
        self.merge_interval = merge_interval
        self.clock = clock
        self.published = {}      # date -> mtime of the raw file version in the published parquet
        self._complete = set()   # dates whose raw file this process has seen as the whole day
        self._merged_at = {}     # date -> when we last merged late rows into it

    # ---- paths ----

    def _local_parquet(self, date):
        return self.parquet_dir / f"date={date}" / "data.parquet"

    def _remote_key(self, date):
        return f"{self.remote_prefix}/date={date}/data.parquet"

    # ---- publishing ----

    def run(self, cutoff: str, keep_from: str | None = None):
        """Publish every changed raw file; delete those that are settled and published.

        `cutoff` is an ISO date: dates before it are settled. A failure on one date is logged
        and retried next run without holding up the others.

        `keep_from` is the oldest date still kept locally. Without R2 there is nothing older to
        merge into and nowhere to publish it, so raw files for earlier dates are just removed
        (with R2 they are merged into what is published, however old).
        """
        for raw in sorted(self.raw_dir.glob(f"*{self.raw_suffix}")):
            date = raw.stem
            settled = date < cutoff
            try:
                if self.uploader is None and keep_from is not None and date < keep_from:
                    raw.unlink()
                    logger.info("Dropped %s for %s: older than the local retention and no R2", raw.name, self.name)
                    continue
                mtime = raw.stat().st_mtime
                if self.published.get(date) != mtime:
                    self._publish(date, mtime, settled)
                if settled and self.published.get(date) == raw.stat().st_mtime:
                    raw.unlink()
                    self._complete.discard(date)
                    logger.info("Cleaned up %s", raw.name)
            except Exception:
                logger.exception("Failed to publish %s for %s; will retry", self.name, date)

    def _publish(self, date, mtime, settled):
        if settled and date not in self._complete:
            if self._merge(date, mtime):
                return
        built = self.builder.build(date)
        if not built and self.builder.built_mtime(date) != mtime:
            self.published[date] = mtime  # the raw file is empty: nothing to publish
            return
        self._upload(date)
        self.published[date] = mtime
        if not settled:
            self._complete.add(date)  # a live day's raw file is the whole day so far

    def _merge(self, date, mtime) -> bool:
        """Merge the raw file into the published parquet. False if there is none to merge into."""
        last = self._merged_at.get(date)
        if last is not None and self.clock() - last < self.merge_interval:
            return True  # coalesce a stream of late rows into one merge per interval
        downloaded = None
        try:
            base = None
            if self.uploader is not None:
                downloaded = self.work_dir / f"base-{self.name}-{date}.parquet"
                downloaded.parent.mkdir(parents=True, exist_ok=True)
                if self.uploader.download_if_exists(self._remote_key(date), downloaded):
                    base = downloaded
            if base is None and self.uploader is None and self._local_parquet(date).exists():
                # With R2, only what is on R2 counts as published: a local file may be left
                # over from an upload that failed, and merging into it would find nothing new
                # and skip the upload for good. Without R2 the local file is the published one.
                base = self._local_parquet(date)
            if base is None:
                return False  # first publication of this date
            added = self.builder.merge(date, base)
            if added:
                self._upload(date)
            self.published[date] = mtime
            self._merged_at[date] = self.clock()
            return True
        finally:
            if downloaded is not None:
                downloaded.unlink(missing_ok=True)

    def _upload(self, date):
        if self.uploader is not None:
            self.uploader.upload_file(self._local_parquet(date), self._remote_key(date))
