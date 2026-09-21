# Jetstream Real-Time Telemetry Logger

Final assignment - Real-Time Embedded Systems

## Overview

Three POSIX threads around the bounded circular buffer from assignment 1,
consuming the Bluesky Jetstream firehose and appending one CSV line per second
to `metrics_log.txt`. One file per thread:

| File | Role |
| --- | --- |
| `producer.c` | **Thread 1** - `libwebsockets` client. Event-driven: every raw JSON frame is timestamped, pushed into the circular buffer, and the callback returns to the network immediately. Also owns the websocket context, so nothing else includes `libwebsockets.h`. |
| `consumer.c` | **Thread 2** - wakes on `notEmpty`, parses with `cJSON` and increments the mutex-protected per-kind counters. No `printf`, no file I/O. Owns `classify()` and the Welford wait-time statistics. |
| `monitor.c` | **Thread 3** - wakes strictly on the second, snapshots and zeroes the counters, measures buffer occupancy and CPU, appends one line. Owns the `/proc/stat` sampling. |
| `telemetry.c` | `main`: arguments, signals, thread lifecycle, and the four shared per-kind counters. |
| `queue.c` / `queue.h` | Circular buffer, extended from assignment 1. |

```
Seconds,Nanoseconds,Commit_Count,Identity_Count,Account_Count,Info_Count,Buffer_Occupancy_Pct,CPU_Pct
```

## Requirements

- GCC
- `pthreads` (`-lpthread`), `libm` (`-lm`)
- `libwebsockets` (`-lwebsockets`), `cJSON` (`-lcjson`)

```bash
sudo apt install build-essential libwebsockets-dev libcjson-dev ca-certificates
```

## Build and Use

```bash
make
./telemetry                       # until SIGINT/SIGTERM
./telemetry -o log.txt -d 3600    # stop after 3600 logged seconds
```

Run as root for `SCHED_FIFO` and `mlockall`; without them it still runs, warns,
and produces identical output with worse jitter.

## Makefile Targets

| Target | Description |
| --- | --- |
| `make` | Compile the binary |
| `make unit` | `classify()` unit tests under ASan/UBSan/LSan (milliseconds) |
| `make test` | 60-second live run, then `check-realtime.py` on the result |
| `make tsan` | Rebuild under ThreadSanitizer to check for races |
| `make memcheck` | 30-second run under Valgrind |
| `make clean` | Remove the compiled binary |

## Configuration

| Constant | Where | Default |
| --- | --- | --- |
| `QUEUESIZE` | `queue.h` | 256 slots |
| `SLOTSIZE` | `queue.h` | 16384 B |
| `MONITOR_RT_PRIO` | `telemetry.h` | 50 (`SCHED_FIFO`; 0 disables) |
| `JS_HOST` / `JS_PATH` | `telemetry.h` | Jetstream endpoint |

Jetstream filtered to `app.bsky.feed.post` runs at ~50 msg/s, and the consumer
turns a frame around in 60-130 us, so the ring is empty almost all the time.
256 slots is ~5 s of absorption at that rate while making one slot worth 0.39%,
so `Buffer_Occupancy_Pct` can resolve a burst. Slots are 16 KB because 8 KB was
marginal: probing the live stream gave a maximum frame of 6984 B, but larger
frames do occur and were being discarded as oversize.

## What changed from assignment 1

**`queue_entry` carries a message instead of a `workFunction`.** The work is
fixed now, so the payload is the frame itself: a 16 KB slot plus its length.
`enqueue_time` is unchanged and still feeds the same Welford statistics, which
now measure how long a frame sits in the buffer before the consumer reaches it.

**The locking convention is unchanged.** `queueAdd`, `queueDel` and the new
`queueCount` still do no locking of their own; every caller takes `fifo->mut`
around the call and signals after releasing it, exactly as in `prod-cons.c`.

**The producer no longer waits on `notFull`.** This is the one real behavioural
change. In assignment 1 a full buffer blocked the producer on
`pthread_cond_wait`. Here the producer *is* the libwebsockets event loop, so
blocking it stalls the socket and loses frames at the TCP level where they
cannot be counted. The frame is dropped and counted instead. The consumer still
signals `notFull`, so reverting is a one-line change.

**Slots are preallocated.** One ~4 MB `malloc` at startup covers the entire
run; there is no allocation on the network path at all, which is most of the
"no leaks over 24 hours" argument.

**Two mutexes instead of one.** `fifo->mut` guards the ring; `kind_lock`
guards the four per-kind counters the assignment requires to be global and
mutex-protected. No thread holds one while taking the other, so lock ordering
cannot deadlock.

**Fewer mutexes than that suggests, though.** Assignment 1 locked the Welford
statistics because `q` consumers updated them concurrently. Here there is
exactly one consumer, and `main` reads the result only after joining it, so
that lock is gone. The same applies to the drop/oversize/malformed diagnostic
totals: each has a single writer thread, and `pthread_join` is the
happens-before edge that makes reading them afterwards safe.

**Shutdown is unchanged for the consumer.** It still parks in
`pthread_cond_wait`, is still cancelled with `pthread_cancel`, and still uses
`consumer_mutex_cleanup` via `pthread_cleanup_push`. The producer cannot be
cancelled safely from inside libwebsockets, so it gets `lws_cancel_service()`
and unwinds its own event loop; the monitor polls `g_running`.

**Signals are collected with `sigtimedwait`, not a handler.** `SIGINT` and
`SIGTERM` are blocked in `main` before any thread starts, so every thread
inherits the mask. Without this, calling `pthread_cancel` from a signal handler
would not be async-signal-safe.

## Real-time design notes

**Absolute deadlines.** `clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, ...)`
against a deadline advanced by `next.tv_sec += 1`, never `now + 1s`. A late
wakeup does not push the following deadline out, so timing error cannot
accumulate: drift is zero by construction over any run length.

**Jitter is already in the log.** The ideal deadline is always a whole second,
so the nanoseconds field *is* the signed jitter:
`tv_nsec < 5e8 ? tv_nsec/1e6 : (tv_nsec - 1e9)/1e6` milliseconds. That reading
is only valid while `|jitter| < 500 ms`, which is what the overrun guard
protects.

**Overrun guard.** If a deadline has already passed when the monitor reaches it
- a scheduling stall, an NTP step, a suspended process - advancing by another
second would leave it in the past too and the loop would fire back-to-back to
catch up. That stamps several rows with the same `Seconds` and breaks the
whole-second invariant: a row written 3.68 s late carried
`tv_nsec = 682240320`, which the formula above turns into **-317.8 ms**,
plotting a large overrun as a small negative jitter. Instead the monitor
re-aligns to the next whole second and counts what it skipped (reported on
stderr at exit).

*Post-processing rule:* drop any row whose `Seconds` is not exactly one greater
than the previous row's. That row is the one that flushed the stall, so its
counters cover several seconds and its `tv_nsec` is not meaningful jitter.
`scripts/check-realtime.py` already does this.

**Priority inheritance.** The monitor runs `SCHED_FIFO` priority 50 and briefly
takes both locks. Both are created with `PTHREAD_PRIO_INHERIT`, so a consumer
preempted while holding a lock is boosted rather than leaving the monitor
blocked for an unbounded time. `mlockall` keeps every page resident.

**Peak occupancy is tracked separately.** `Buffer_Occupancy_Pct` is an
instantaneous 1 Hz sample, as the assignment specifies, so it misses a burst
that arrives and drains between two samples. `queueAdd` keeps a high-water mark
and `main` prints it at exit. In one stalled test the 1 Hz column reported
0.39% while the true peak had been 22 slots (8.59%).

**Classification is a pure function.** `classify(json, len)` returns a `K_*`
index (or `K_BADJSON`) and touches no counters, no globals and no I/O, so it is
unit-testable without a queue, a socket or a thread.
`cJSON_GetObjectItemCaseSensitive` is applied to the root object only, so a
`"kind"` embedded in a post's text or a nested object cannot be mistaken for
the real field. Anything that parses but is not commit/identity/account is a
system/error message and counts as `info`, which is what the assignment's
fourth counter is for; unparseable input is counted separately as malformed.

**The websocket context is owned by `producer.c`, not shared.** `stream_init()`
runs in `main` before any thread exists and `stream_destroy()` after they are
all joined, so the pointer never changes while another thread can see it. That
is what makes `stream_wake()` safe to call from `main` and from the monitor
with no lock at all, and it keeps `libwebsockets.h` out of every other file.

**Failures are loud.** A negative return from `lws_service()` means the context
is gone, not that a connection dropped (the retry policy handles those without
returning here). It sets `g_failed`, stops the program, and `main` exits
non-zero so `systemd Restart=on-failure` can restart the capture rather than
leaving the monitor to append `0,0,0,0` rows for the rest of the day.

**Network resilience.** `lws_retry_bo_t` gives exponential backoff
(1/2/4/8/16 s, then 16 s forever) with 20% jitter. `secs_since_valid_ping = 30`
and `secs_since_valid_hangup = 60` detect a silently wedged TCP connection, the
usual overnight failure mode on Pi Zero W Wi-Fi. During an outage the monitor
keeps writing lines with zero counts, so the gap appears in the data rather
than as missing rows.

## On the Raspberry Pi Zero W

The Zero W is ARM1176 (**ARMv6**), so it needs 32-bit Raspberry Pi OS - the
64-bit image will not boot, and Ubuntu does not build for ARMv6 at all. It has
no Ethernet, so Wi-Fi and SSH must be baked into the image before first boot
(Raspberry Pi Imager, OS customisation), and its radio is 2.4 GHz only.

```bash
sudo ./scripts/pi-setup.sh        # deps, Wi-Fi power save, chrony, limits
make && make unit                 # ~1-2 min natively on the Zero W

sudo cyclictest -m -t1 -p 50 -i 1000000 -l 600 -q   # hardware jitter floor
make test                                           # then compare
```

Measure the floor first: whatever `cyclictest` reports at the same priority and
period is what the kernel can do, and the program cannot beat it. Stock
Raspberry Pi OS ships a `CONFIG_PREEMPT` kernel, not `PREEMPT_RT`.

`make tsan` does **not** work here: ThreadSanitizer has no 32-bit ARM support.
Run it on a development machine; `make memcheck` and ASan do work on the Pi.

| Script | Purpose |
| --- | --- |
| `scripts/pi-setup.sh` | One-time Pi setup, idempotent, needs root |
| `scripts/check-realtime.py` | Jitter/drift/gap report from a log file |
| `scripts/telemetry.service` | Unattended capture unit |

`telemetry.service` uses `Requires=time-sync.target` because the Zero W has no
hardware RTC and the monitor sleeps on absolute `CLOCK_REALTIME` deadlines.

## Before the 24-hour run

```bash
sudo iw wlan0 set power_save off        # #1 cause of overnight dropouts
sudo timedatectl set-timezone Europe/Athens
timedatectl status                      # confirm NTP synchronised
vcgencmd get_throttled                  # 0x0 means no undervoltage
```

Not yet implemented: gating the first CSV line on a wall-clock start epoch so
the capture is exactly 86400 lines from 00:00:00. Currently `-d 86400` counts
from launch, so launch on the second.
