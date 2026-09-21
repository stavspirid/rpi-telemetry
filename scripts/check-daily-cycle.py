#!/usr/bin/env python3
"""Daily-cycle completeness check for metrics_log.txt.

    ./scripts/check-daily-cycle.py metrics_log.txt

The capture window is meant to start at 23:00 local time. This answers a
narrower question than check-realtime.py's gap report: for each
23:00-anchored 24h cycle present in the log, is the 23:00:00 start row
there, is the 22:59:59-next-day end row there, and were all 86400
seconds in between actually written?

A cycle is found by looking for a logged row whose timestamp is exactly
23:00:00 local time on some date; the window then runs to 23:00:00 the
next day (exclusive), i.e. its last second is 22:59:59. If the log ends
before that last second, the cycle is reported as still in progress
rather than as having missing rows.
"""
import sys
from datetime import datetime

DAY = 86400


def load_seconds(path):
    seconds = set()
    for n, line in enumerate(open(path), 1):
        line = line.strip()
        if not line:
            continue
        p = line.split(",")
        if len(p) != 8:
            sys.exit(f"{path}:{n}: expected 8 fields, got {len(p)}")
        try:
            seconds.add(int(p[0]))
        except ValueError as e:
            sys.exit(f"{path}:{n}: {e}")
    return seconds


def local_23(d):
    """Epoch of 23:00:00 local time on date d (a date object)."""
    return int(datetime(d.year, d.month, d.day, 23, 0, 0).timestamp())


def find_cycle_starts(seconds):
    """Every local date whose 23:00:00 second was actually logged."""
    dates = {datetime.fromtimestamp(s).date() for s in seconds}
    return [start for d in sorted(dates)
            if (start := local_23(d)) in seconds]


def fmt(ts, with_date=True):
    f = "%Y-%m-%d %H:%M:%S" if with_date else "%H:%M:%S"
    return datetime.fromtimestamp(ts).strftime(f)


def gap_runs(missing):
    runs = []
    lo = prev = missing[0]
    for s in missing[1:]:
        if s != prev + 1:
            runs.append((lo, prev))
            lo = s
        prev = s
    runs.append((lo, prev))
    return runs


def check_cycle(start, seconds, log_max):
    end = start + DAY - 1  # last second of the window: 22:59:59 next day
    print(f"\n=== cycle {fmt(start)} -> {fmt(end)} local ===")

    print(f"  {'[ok]' if start in seconds else '[XX]'} "
          f"start row {'present' if start in seconds else 'MISSING'} ({fmt(start)})")

    truncated = end > log_max
    check_end = log_max if truncated else end
    if truncated:
        print(f"  [--] log ends before this cycle completes "
              f"(last row {fmt(log_max)}); treating the rest as in-progress, "
              f"not missing")
    else:
        print(f"  {'[ok]' if end in seconds else '[XX]'} "
              f"end row {'present' if end in seconds else 'MISSING'} ({fmt(end)})")

    missing = [s for s in range(start, check_end + 1) if s not in seconds]
    expected = check_end - start + 1
    print(f"  rows            : {expected - len(missing)} / {expected} expected seconds"
          + (" (partial cycle so far)" if truncated else ""))

    if missing:
        runs = gap_runs(missing)
        print(f"  missed seconds  : {len(missing)} in {len(runs)} gap(s)")
        for lo, hi in runs[:10]:
            print(f"      {fmt(lo)} .. {fmt(hi, with_date=False)} local  "
                  f"({hi - lo + 1} s)")
        if len(runs) > 10:
            print(f"      ... and {len(runs) - 10} more")
    else:
        print("  missed seconds  : 0")

    ok = (start in seconds) and not missing and (truncated or end in seconds)
    return ok, truncated


def main(path):
    seconds = load_seconds(path)
    if not seconds:
        sys.exit("no rows")

    log_min, log_max = min(seconds), max(seconds)
    print(f"=== {path} ===")
    print(f"  rows in file : {len(seconds)}")
    print(f"  span         : {fmt(log_min)} .. {fmt(log_max)} local")

    starts = find_cycle_starts(seconds)
    if not starts:
        print("\nNo row logged at 23:00:00 local -- no 23:00-anchored cycle to check.")
        return 1

    ok_all, any_complete = True, False
    for start in starts:
        ok, truncated = check_cycle(start, seconds, log_max)
        if not truncated:
            any_complete = True
            ok_all &= ok
        else:
            ok_all &= ok  # in-progress cycle still reports missing seconds inside its logged range

    print("\n--- verdict ---")
    if not any_complete:
        print("  [--] no fully-elapsed 24h cycle in this log yet (still in progress)")
    else:
        print(f"  [{'ok' if ok_all else 'XX'}] "
              f"{'every 23:00 cycle is gap-free' if ok_all else 'at least one 23:00 cycle has missing or truncated rows'}")
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "metrics_log.txt"))
