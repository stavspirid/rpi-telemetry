#!/usr/bin/env python3
"""Real-time sanity check for metrics_log.txt.

    ./scripts/check-realtime.py metrics_log.txt

Reports jitter, drift, gaps and load. This is the numeric check, not the
report's plots -- it answers "did the periodic thread actually hold 1 Hz
on this hardware?" and nothing else.

Jitter convention: the monitor's ideal deadline is always a whole second,
so the Nanoseconds column IS the signed offset from it.  A row whose
Seconds is not exactly one past the previous row is the row that flushed
a stall (see the overrun guard in monitor.c); its Nanoseconds is not a
meaningful jitter sample, so it is excluded and counted as a gap instead.
"""
import sys
import statistics as st

FIELDS = ("Seconds", "Nanoseconds", "Commit", "Identity", "Account",
          "Info", "Buffer_Pct", "CPU_Pct")


def main(path):
    rows = []
    for n, line in enumerate(open(path), 1):
        line = line.strip()
        if not line:
            continue
        p = line.split(",")
        if len(p) != 8:
            sys.exit(f"{path}:{n}: expected 8 fields, got {len(p)}")
        try:
            rows.append((int(p[0]), int(p[1]), int(p[2]), int(p[3]),
                         int(p[4]), int(p[5]), float(p[6]), float(p[7])))
        except ValueError as e:
            sys.exit(f"{path}:{n}: {e}")
    if len(rows) < 2:
        sys.exit("need at least 2 rows")

    span = rows[-1][0] - rows[0][0] + 1
    dups = len(rows) - len({r[0] for r in rows})

    jitter, gaps, skipped = [], [], 0
    for i, r in enumerate(rows):
        ns = r[1]
        ms = ns / 1e6 if ns < 5e8 else (ns - 1e9) / 1e6
        if i == 0:
            jitter.append(ms)
            continue
        step = r[0] - rows[i - 1][0]
        if step == 1:
            jitter.append(ms)
        else:
            gaps.append((rows[i - 1][0], step - 1))
            skipped += step - 1

    rate = [r[2] + r[3] + r[4] + r[5] for r in rows]
    cpu = [r[7] for r in rows]
    buf = [r[6] for r in rows]

    def p(v, q):
        s = sorted(v)
        return s[min(len(s) - 1, int(q * len(s)))]

    print(f"\n=== {path} ===")
    print(f"  rows              : {len(rows)}")
    print(f"  wall span         : {span} s  ({span/3600:.2f} h)")
    print(f"  first / last      : {rows[0][0]} .. {rows[-1][0]}")

    print("\n--- timing ---")
    print(f"  duplicate seconds : {dups}")
    print(f"  skipped seconds   : {skipped} in {len(gaps)} gap(s)")
    if gaps:
        for after, n in gaps[:10]:
            print(f"      after {after}: {n} s missing")
        if len(gaps) > 10:
            print(f"      ... and {len(gaps)-10} more")
    # Drift: with absolute deadlines, row i must land on first+i unless a
    # second was skipped. Anything left over is genuine accumulated slip.
    slip = (rows[-1][0] - rows[0][0]) - (len(rows) - 1) - skipped
    print(f"  accumulated drift : {slip} s   "
          f"({'OK -- zero by construction' if slip == 0 else 'INVESTIGATE'})")

    print(f"\n--- jitter ({len(jitter)} clean samples, ms) ---")
    print(f"  mean / median     : {st.mean(jitter):+.3f} / {st.median(jitter):+.3f}")
    print(f"  stddev            : {st.pstdev(jitter):.3f}")
    print(f"  min / max         : {min(jitter):+.3f} / {max(jitter):+.3f}")
    print(f"  p50 / p99 / p999  : {p(jitter,.50):.3f} / {p(jitter,.99):.3f} / {p(jitter,.999):.3f}")
    over = [j for j in jitter if abs(j) > 10]
    print(f"  |jitter| > 10 ms  : {len(over)} ({100*len(over)/len(jitter):.3f}%)")

    print("\n--- load ---")
    print(f"  msg/s mean / max  : {st.mean(rate):.1f} / {max(rate)}")
    print(f"  total messages    : {sum(rate)}")
    print(f"  silent seconds    : {sum(1 for r in rate if r == 0)}"
          "   (network outage or idle stream)")
    print(f"  CPU% mean  / max   : {st.mean(cpu):.1f} / {max(cpu):.1f}")
    print(f"  buffer% max        : {max(buf):.2f}")

    print("\n--- verdict ---")
    ok = True
    for cond, good, bad in (
        (dups == 0, "no duplicate timestamps", f"{dups} duplicate timestamps"),
        (slip == 0, "no accumulated drift", f"{slip} s of drift"),
        (max(abs(j) for j in jitter) < 50,
         "max |jitter| under 50 ms", f"max |jitter| {max(abs(j) for j in jitter):.1f} ms"),
        (skipped == 0, "no skipped seconds", f"{skipped} skipped seconds"),
    ):
        print(f"  [{'ok' if cond else 'XX'}] {good if cond else bad}")
        ok &= cond
    print()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "metrics_log.txt"))
