from collections import Counter
from datetime import datetime, timezone

import pytest

from daemon.replication_planner import PERIOD, Planner, Position, Step, parse_state

BASE = int(datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc).timestamp())
FIRST = 1000


class FakeSource:
    """Minute states ~60s apart with a little jitter; day/hour published ~2 min after their label."""

    def __init__(self, n_minutes):
        self.last = FIRST + n_minutes - 1
        self.probes = 0

    def ts(self, seq):
        return BASE + (seq - FIRST) * 60 + (seq % 3)

    def timestamp(self, interval, seq):
        assert interval == "minute" and seq <= self.last  # history before FIRST exists too
        self.probes += 1
        return self.ts(seq)

    def latest(self, interval):
        lm_ts = self.ts(self.last)
        if interval == "minute":
            return self.last, lm_ts
        p = PERIOD[interval]
        label = ((lm_ts - 120) // p) * p
        return label // p, label


class DriftingSource(FakeSource):
    """Minutes are really ~61.5s apart, so a 60s-per-minute guess drifts badly."""

    def ts(self, seq):
        return BASE + int((seq - FIRST) * 61.5)


def check_tiling(planner, start, steps):
    """Every minute after `start` is covered exactly once, and each coarse window matches its labels."""
    covered = Counter()
    position = start
    for step in steps:
        before, after = planner.minute_of(position), planner.minute_of(step.end)
        assert after > before
        for m in range(before + 1, after + 1):
            covered[m] += 1
        if step.interval != "minute":
            hi = planner.epoch(step.end)
            lo = hi - PERIOD[step.interval]
            assert all(lo < planner.minute_ts(m) <= hi for m in range(before + 1, after + 1))
            assert planner.minute_ts(after + 1) > hi  # nothing of this window left over
        position = step.end
    final = planner.minute_of(position)
    assert final == planner.latest_minute
    assert all(covered[m] == 1 for m in range(planner.minute_of(start) + 1, final + 1))


def midnight_label(planner):
    """Some midnight label inside the published range (early, so many days are ahead)."""
    return (BASE // 86400 + 1) * 86400


@pytest.mark.parametrize("offset", [0, 7, 59, 60, 61, 1439, 1440, 1441, 2000, 3333])
def test_plan_tiles_every_minute_exactly_once(offset):
    planner = Planner(FakeSource(n_minutes=60 * 24 * 6 + 777))
    start = Position(minute=FIRST + offset)
    check_tiling(planner, start, planner.route(start))


def test_caught_up_has_no_steps():
    planner = Planner(FakeSource(n_minutes=500))
    assert planner.route(Position(minute=planner.latest_minute)) == []


def test_climbs_with_minutes_then_hours_then_days_then_back_down():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 5 + 400))
    intervals = [s.interval for s in planner.route(Position(minute=FIRST + 20))]
    runs = [i for n, i in enumerate(intervals) if n == 0 or intervals[n - 1] != i]
    assert runs == ["minute", "hour", "day", "hour", "minute"]


def test_midnight_label_takes_whole_days_first():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 4 + 90))
    start = Position(label=midnight_label(planner))
    step = planner.route(start)[0]
    assert step.interval == "day"
    assert step.end == Position(label=start.label + 86400)
    assert step.seq == planner.seq_for("day", start.label + 86400)


def test_mid_day_label_takes_hours_not_days():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 4 + 90))
    start = Position(label=midnight_label(planner) + 3 * 3600)
    assert planner.route(start)[0].interval == "hour"


def test_unaligned_minute_never_uses_a_coarse_diff():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 3))
    route = planner.route(Position(minute=FIRST + 30))
    assert route[0] == Step("minute", FIRST + 31, Position(minute=FIRST + 31))


def test_minute_on_an_hour_label_switches_to_hours():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 3))
    # last minute with ts <= the first hour label after BASE
    label = (BASE // 3600 + 1) * 3600
    m = planner.last_minute_at_or_before(label)
    step = planner.route(Position(minute=m))[0]
    assert step.interval == "hour" and step.end == Position(label=label + 3600)


def test_excluded_interval_falls_back():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 4 + 90))
    start = Position(label=midnight_label(planner))
    assert planner.route(start, exclude=("day",))[0].interval == "hour"
    assert planner.route(start, exclude=("day", "hour"))[0].interval == "minute"


def test_unpublished_hour_is_not_used():
    # 30 minutes into a new hour: that hour's diff isn't published yet, so only minutes remain
    planner = Planner(FakeSource(n_minutes=60 * 5 + 30))
    start = Position(label=planner.latest["hour"][1])
    steps = planner.route(start)
    assert steps and all(s.interval == "minute" for s in steps)
    assert planner.minute_of(steps[-1].end) == planner.latest_minute


def test_step_count_is_small_for_a_long_outage():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 30))
    start = Position(minute=FIRST + 17)
    steps = planner.route(start)
    assert len(steps) < 60 + 24 + 30 + 24 + 60  # climb, days, descend: not ~43,000 minutes
    check_tiling(planner, start, steps)


def test_climb_ends_on_the_hour_label():
    planner = Planner(FakeSource(n_minutes=60 * 24 * 3))
    route = planner.route(Position(minute=FIRST + 20))
    climb = []
    for s in route:
        if s.interval != "minute":
            break
        climb.append(s)
    assert climb[-1].end.label is not None and climb[-1].end.label % 3600 == 0
    assert all(s.end.label is None for s in climb[:-1])


def probes_to_plan(days, source_cls=DriftingSource):
    source = source_cls(n_minutes=60 * 24 * 60)
    planner = Planner(source)
    start = Position(minute=planner.latest_minute - 60 * 24 * days)
    steps = planner.route(start)
    check_tiling(planner, start, steps)  # also keeps probes honest: tiling reads more states
    source.probes = 0
    planner = Planner(source)
    planner.route(start)
    return source.probes


def test_state_reads_do_not_grow_with_the_backlog():
    short, long_ = probes_to_plan(3), probes_to_plan(50)
    assert long_ <= short + 5          # 47 extra days cost (almost) nothing
    assert long_ < 40                  # a handful of reads, however far behind


def test_parse_state_unescapes_timestamp():
    text = "#Wed Oct 21 13:02:09 UTC 2026\nsequenceNumber=123654\ntimestamp=2026-10-21T13\\:00\\:00Z\n"
    assert parse_state(text) == int(datetime(2026, 10, 21, 13, tzinfo=timezone.utc).timestamp())
