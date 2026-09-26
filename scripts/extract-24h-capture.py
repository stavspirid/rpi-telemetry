#!/usr/bin/env python3
"""Extract one 23:00-anchored 24h cycle out of metrics_log.txt.

    ./scripts/extract-24h-capture.py [metrics_log.txt] [-o 24_hour_capture.txt] [--date YYYY-MM-DD]

Finds the same 23:00:00 -> 22:59:59(next day) cycles.
Picks the first COMPLETE one (or the one
starting on --date, if given), and writes just its rows -- unchanged,
in order -- to the output file.

Refuses to extract a cycle that is truncated (log ends before 22:59:59
the next day) or has missing seconds, since the whole point is a clean
24h file.
"""
import argparse
import sys
from datetime import datetime

DAY = 86400


def load_rows(path):
    rows = []  # (epoch, raw_line)
    for n, line in enumerate(open(path), 1):
        stripped = line.rstrip("\n")
        if not stripped:
            continue
        p = stripped.split(",")
        if len(p) != 8:
            sys.exit(f"{path}:{n}: expected 8 fields, got {len(p)}")
        try:
            epoch = int(p[0])
        except ValueError as e:
            sys.exit(f"{path}:{n}: {e}")
        rows.append((epoch, stripped))
    return rows


def local_23(d):
    return int(datetime(d.year, d.month, d.day, 23, 0, 0).timestamp())


def fmt(ts):
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def find_complete_cycles(seconds, log_max):
    dates = {datetime.fromtimestamp(s).date() for s in seconds}
    cycles = []
    for d in sorted(dates):
        start = local_23(d)
        if start not in seconds:
            continue
        end = start + DAY - 1
        if end > log_max:
            continue  # truncated, log doesn't reach that far
        missing = sum(1 for s in range(start, end + 1) if s not in seconds)
        cycles.append((start, end, missing))
    return cycles


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("input", nargs="?", default="metrics_log.txt")
    ap.add_argument("-o", "--output", default="24_hour_capture.txt")
    ap.add_argument("--date", help="local date the cycle starts on (YYYY-MM-DD, 23:00:00). "
                                    "Default: first complete cycle found.")
    args = ap.parse_args()

    rows = load_rows(args.input)
    if not rows:
        sys.exit("no rows")
    seconds = {e for e, _ in rows}
    log_max = max(seconds)

    cycles = find_complete_cycles(seconds, log_max)
    if not cycles:
        sys.exit(f"no complete 23:00-anchored 24h cycle found in {args.input}")

    if args.date:
        wanted = local_23(datetime.strptime(args.date, "%Y-%m-%d").date())
        match = [c for c in cycles if c[0] == wanted]
        if not match:
            sys.exit(f"no complete cycle starts at 23:00:00 on {args.date}")
        start, end, missing = match[0]
    else:
        start, end, missing = cycles[0]
        if len(cycles) > 1:
            print(f"note: {len(cycles)} complete cycles found, using the first "
                  f"({fmt(start)}); pass --date to pick another", file=sys.stderr)

    if missing:
        sys.exit(f"cycle {fmt(start)} -> {fmt(end)} has {missing} missing second(s) ")

    kept = [line for e, line in rows if start <= e <= end]
    with open(args.output, "w") as f:
        f.write("\n".join(kept) + "\n")

    print(f"wrote {len(kept)} rows ({fmt(start)} -> {fmt(end)} local) to {args.output}")


if __name__ == "__main__":
    main()
