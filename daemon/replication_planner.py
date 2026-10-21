"""Choose which planet replication diffs to process: day, then hour, then minute.

A diff labelled L contains exactly the minute diffs whose state timestamp falls in
(previous label, L]. (Checked against hour 123654, labelled 13:00:00, which ends at
the minute stamped 12:59:21.) Element timestamps do NOT follow the labels.

Day and hour labels are strictly periodic and midnight is both, so day and hour
diffs tile with each other by arithmetic alone; only where a label meets a *minute*
diff do we need minute state files:

  * a minute position M is "on" hour label L when ts(M) <= L < ts(M+1): two lookups
  * from label L, minute diffs resume at the last minute with ts <= L: one search

So the route from an arbitrary position to now is: minute diffs up to the next hour
label, hour diffs to midnight, whole days, then hours and finally minutes back down to
the newest minute. `Planner.route` computes all of it at once from the last processed
time and the three latest state files; the only extra reads are a few at each end
(finding where those two minute stretches begin and end), however far behind we are.
"""

import bisect
from dataclasses import dataclass
from datetime import datetime, timezone

PLANET_URL = "https://planet.openstreetmap.org/replication"
PERIOD = {"hour": 3600, "day": 86400}


@dataclass(frozen=True)
class Position:
    """Where we are: covered through minute `minute`, and/or through label time `label`.

    `label` (epoch seconds, on an hour boundary) means every minute diff with state
    timestamp <= label is done. `minute` may be unknown (None) while on a label.
    """
    minute: int | None = None
    label: int | None = None


@dataclass(frozen=True)
class Step:
    interval: str       # 'day' | 'hour' | 'minute'
    seq: int            # sequence within that interval
    end: Position       # where we are once this step is done


def parse_state(text: str) -> int:
    """Epoch seconds from the timestamp line of a state.txt."""
    for line in text.splitlines():
        if line.startswith("timestamp="):
            stamp = line.split("=", 1)[1].replace("\\", "")
            return int(datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ")
                       .replace(tzinfo=timezone.utc).timestamp())
    raise ValueError("no timestamp in state file")


def _parse_seq(text: str) -> int:
    for line in text.splitlines():
        if line.startswith("sequenceNumber="):
            return int(line.split("=", 1)[1])
    raise ValueError("no sequenceNumber in state file")


class HttpStateSource:
    """Reads planet state files: .latest(interval) and .timestamp(interval, seq)."""

    def __init__(self, session):
        self.session = session
        self.requests = 0

    def _get(self, url):
        self.requests += 1
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def latest(self, interval):
        text = self._get(f"{PLANET_URL}/{interval}/state.txt")
        return _parse_seq(text), parse_state(text)

    def timestamp(self, interval, seq):
        s = f"{seq:09d}"
        return parse_state(self._get(f"{PLANET_URL}/{interval}/{s[0:3]}/{s[3:6]}/{s[6:9]}.state.txt"))


class Planner:
    def __init__(self, source):
        self.source = source
        self.latest = {i: source.latest(i) for i in ("day", "hour", "minute")}  # -> (seq, epoch)
        self._minute_ts = {}
        self._known = []  # sorted (epoch, seq) of every minute state read so far

    # ---- minute state lookups ----

    def minute_ts(self, seq):
        if seq not in self._minute_ts:
            ts = self._minute_ts[seq] = self.source.timestamp("minute", seq)
            bisect.insort(self._known, (ts, seq))
        return self._minute_ts[seq]

    @property
    def latest_minute(self):
        return self.latest["minute"][0]

    def last_minute_at_or_before(self, epoch):
        """Newest minute sequence whose state timestamp is <= epoch (None if in the future)."""
        lm_seq, lm_ts = self.latest["minute"]
        if epoch > lm_ts:
            return None
        # Minutes are ~60s apart but drift, so start from the nearest minute already read,
        # gallop until the answer is bracketed, then bisect.
        i = bisect.bisect_left(self._known, (epoch, -1))
        near = [self._known[j] for j in (i - 1, i) if 0 <= j < len(self._known)]
        ref_ts, ref_seq = min(near, key=lambda k: abs(k[0] - epoch)) if near else (lm_ts, lm_seq)
        guess = min(lm_seq, ref_seq + round((epoch - ref_ts) / 60))
        if self.minute_ts(guess) <= epoch:
            lo, step = guess, 1
            while True:
                hi = lo + step
                if hi > lm_seq:
                    hi = lm_seq + 1  # sentinel: past the newest minute, never probed
                    break
                if self.minute_ts(hi) > epoch:
                    break
                lo, step = hi, step * 2
        else:
            hi, step = guess, 1
            while True:
                lo = hi - step
                if self.minute_ts(lo) <= epoch:
                    break
                hi, step = lo, step * 2
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if self.minute_ts(mid) <= epoch:
                lo = mid
            else:
                hi = mid
        return lo

    # ---- positions ----

    def epoch(self, pos: Position) -> int:
        """The time a position has caught up to."""
        return pos.label if pos.label is not None else self.minute_ts(pos.minute)

    def minute_of(self, pos: Position) -> int:
        """The minute sequence a position is covered through."""
        if pos.minute is not None:
            return pos.minute
        return self.last_minute_at_or_before(pos.label)

    # ---- coarse diffs ----

    def seq_for(self, interval, label):
        latest_seq, latest_label = self.latest[interval]
        return latest_seq - (latest_label - label) // PERIOD[interval]

    def published(self, interval, label):
        return label <= self.latest[interval][1]

    # ---- planning ----

    def route(self, pos: Position, exclude=()):
        """Every diff to process, in order, to get from `pos` to the newest minute.

        `exclude` lists intervals that proved unavailable ("hour" disables all coarse diffs).
        """
        h, d = PERIOD["hour"], PERIOD["day"]
        lm_seq, lm_ts = self.latest["minute"]
        latest_hour = self.latest["hour"][1]
        coarse_ok = "hour" not in exclude
        steps = []

        def minutes(first, last):
            steps.extend(Step("minute", s, Position(minute=s)) for s in range(first, last + 1))

        label, m = pos.label, pos.minute
        if label is None:
            if m >= lm_seq:
                return []
            on_label = (self.minute_ts(m + 1) // h) * h      # largest hour label <= next minute
            if self.minute_ts(m) <= on_label:
                label = on_label                              # we already sit exactly on a label
            else:
                first_label = on_label + h                    # minute diffs up to this label
                if coarse_ok and first_label <= min(latest_hour, lm_ts):
                    end = self.last_minute_at_or_before(first_label)
                    minutes(m + 1, end)
                    steps[-1] = Step("minute", end, Position(minute=end, label=first_label))
                    label, m = first_label, end
                else:
                    minutes(m + 1, lm_seq)
                    return steps

        if coarse_ok:
            while label + h <= latest_hour:
                if ("day" not in exclude and label % d == 0
                        and label + d <= latest_hour and self.published("day", label + d)):
                    interval, nxt = "day", label + d
                else:
                    interval, nxt = "hour", label + h
                steps.append(Step(interval, self.seq_for(interval, nxt), Position(label=nxt)))
                label, m = nxt, None

        if m is None:
            m = self.last_minute_at_or_before(label)
        minutes(m + 1, lm_seq)
        return steps
