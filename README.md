# rtes-jetstream

Real-time multithreaded telemetry logger for the Bluesky Jetstream firehose,
running on a Raspberry Pi Zero W. Final assignment, Real-Time Embedded
Systems 2026.

Built directly on the producer/consumer queue from assignment 1. Three POSIX
threads around the same bounded circular buffer, appending one CSV line per
second to `metrics_log.txt`:

```
Seconds,Nanoseconds,Commit_Count,Identity_Count,Account_Count,Info_Count,Buffer_Occupancy_Pct,CPU_Pct
```

## Files

| File | Role |
| --- | --- |
| `queue.h` / `queue.c` | Circular buffer, extended from assignment 1 |
| `stats.c` | Welford wait-time stats, kind counters, `/proc/stat` CPU |

| `producer.c` | libwebsockets client (thread 1) |
| `consumer.c` | JSON parse and classification (thread 2) |
| `tests/test_classify.c` | Unit tests for `classify()` |
| `monitor.c` | 1 Hz absolute-deadline logger (thread 3) |
| `telemetry.c` | `main`: arguments, signals, thread lifecycle |

## Build and use

```sh
sudo apt install build-essential libwebsockets-dev libcjson-dev ca-certificates
make
sudo ./telemetry -o metrics_log.txt          # until SIGINT/SIGTERM
sudo ./telemetry -o metrics_log.txt -d 3600  # stop after 3600 logged seconds
```

`sudo` is only needed for `SCHED_FIFO` and `mlockall`. Without it the program
still runs, warns, and produces identical output with worse jitter.

| Target | Description |
| --- | --- |
| `make` | Compile the binary |
| `make unit` | `classify()` unit tests under ASan/UBSan/LSan (milliseconds) |
| `make test` | 60-second run, prints the first lines of the CSV |
| `make tsan` | Rebuild under ThreadSanitizer to check for races |
| `make memcheck` | 30-second run under Valgrind |

## What changed from assignment 1

**`queue_entry` carries a message instead of a `workFunction`.** The work is
fixed now (parse the frame, classify its `kind`), so the payload is the frame
itself: a fixed 8 KB slot plus its length. `enqueue_time` is kept unchanged
and still feeds the same Welford statistics, which now measure how long a
frame sits in the buffer before the consumer reaches it.

**The locking convention is unchanged.** `queueAdd`, `queueDel` and the new
`queueCount` still do no locking of their own; every caller takes `fifo->mut`
around the call and signals after releasing it, exactly as in `prod-cons.c`.

**`queueAdd` takes a pointer.** `queue_entry` is now ~8 KB, so passing by
value would have cost two extra copies per message. Both `queueAdd` and
`queueDel` copy only `len + 1` bytes, not the whole slot.

**The producer no longer waits on `notFull`.** This is the one real
behavioural change. In assignment 1 a full buffer blocked the producer on
`pthread_cond_wait(fifo->notFull, ...)`. Here the producer *is* the
libwebsockets event loop, so blocking it stalls the socket and loses frames at
the TCP level, where they cannot be counted. Instead the frame is dropped and
`counters_bump_drop()` records it. The consumer still signals `notFull` after
every dequeue, so reverting to the blocking behaviour is a one-line change.

**Slots are preallocated.** `buf[QUEUESIZE]` lives inside the `queue` struct
as before, so one ~4 MB `malloc` at startup covers the entire run. There is no
allocation on the network path at all, which is most of the "no leaks over 24
hours" argument.

**The buffer is 256 x 16 KB, not 2048 x 8 KB.** Jetstream filtered to
`app.bsky.feed.post` runs at ~50 msg/s and the consumer turns a frame around in
60-130 us, so 2048 slots was ~40 s of buffering for a ring that is empty
almost all the time: `Buffer_Occupancy_Pct` read `0.00` on every single row,
and one slot was worth 0.05%. At 256 slots one slot is 0.39%, the column can
resolve a burst, and there is still ~5 s of absorption at the nominal rate.
Slots went to 16 KB because 8 KB was marginal -- probing the live stream gave a
maximum frame of 6984 B, but larger frames do occur and were being discarded
as oversize.

**Shutdown is unchanged for the consumer.** It still parks in
`pthread_cond_wait`, is still cancelled with `pthread_cancel`, and still uses
`consumer_mutex_cleanup` via `pthread_cleanup_push`. The producer cannot be
cancelled safely from inside libwebsockets, so it gets `lws_cancel_service()`
and unwinds its own event loop; the monitor polls `g_running`.

**Signals are collected with `sigtimedwait`, not a handler.** `SIGINT` and
`SIGTERM` are blocked in `main` before any thread starts, so every thread
inherits the mask and `main` collects them synchronously. Without this,
calling `pthread_cancel` from a signal handler would not be
async-signal-safe.

## Real-time design notes

**Classification is a pure function.** `classify(json, len)` takes a buffer and
returns a `K_*` index (or `K_BADJSON`), touching no counters, no globals and no
I/O, so it is unit-testable without a queue, a socket or a thread --
`make unit` runs 20 cases in milliseconds under ASan/UBSan/LSan, including
200k repeat parses to prove it does not leak over a 24-hour run.
`cJSON_GetObjectItemCaseSensitive` is applied to the ROOT object only, so a
`"kind"` embedded in a post's text or in a nested object cannot be mistaken for
the real field (a whole-message `strstr` would get that wrong).

**Anything that is not commit/identity/account counts as info.** There used to
be a `K_OTHER` bucket that nothing ever logged, so an unrecognised frame was
silently lost. The assignment glosses the fourth counter as *info (system/error
messages)*, which is exactly what such a frame is, so it now lands in
`Info_Count` where it is visible. A 75 s live sample saw commit=3632,
identity=20, account=22 and zero of anything else, so in steady state this
changes nothing; it matters only when something unusual arrives, which is when
the column needs to work. Unparseable input is separate and still counted as
`badjson`.

**Two independent locks.** `fifo->mut` guards the ring; `counters_t.lock`
guards the message counters. The consumer parses JSON outside both, so the
monitor's once-per-second snapshot never waits behind a `cJSON_Parse`. No
thread ever holds one while taking the other, so lock ordering cannot
deadlock.

**Priority inheritance.** The monitor runs `SCHED_FIFO` priority 50 and
briefly takes both locks. Both are created with `PTHREAD_PRIO_INHERIT`, so a
consumer preempted while holding a lock is boosted rather than leaving the
monitor blocked for an unbounded time.

**Absolute deadlines.** `clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, ...)`
against a deadline advanced by `next.tv_sec += 1`, never `now + 1s`. A late
wakeup does not push the following deadline out, so timing error cannot
accumulate: drift is zero by construction over any run length.

**Jitter is already in the log.** The ideal deadline is always a whole second,
so the nanoseconds field *is* the signed jitter:
`tv_nsec < 5e8 ? tv_nsec/1e6 : (tv_nsec - 1e9)/1e6` milliseconds. That reading
is only valid while `|jitter| < 500 ms`, which is why the overrun guard below
exists.

**Overrun guard.** If a deadline has already passed by the time the monitor
reaches it -- a scheduling stall, an NTP step, a suspended process -- advancing
`next` by another second would leave it in the past too, and the loop would
fire back-to-back to catch up. That stamps several rows with the same `Seconds`
value and breaks the whole-second invariant the jitter reading depends on: a
row written 3.68 s late carried `tv_nsec = 682240320`, which the formula above
turns into **-317.8 ms**, plotting a large overrun as a small negative jitter.
Instead the monitor re-aligns to the next whole second in the future and counts
what it skipped (reported on stderr at exit). Every logged row then sits on the
grid with true jitter in `tv_nsec`; the seconds that could not be sampled are
simply absent from the `Seconds` column.

*Post-processing rule:* drop any row whose `Seconds` is not exactly one greater
than the previous row's. That row is the one that flushed the stall, so its
counters cover several seconds and its `tv_nsec` is not meaningful jitter.

**Peak occupancy is tracked separately.** `Buffer_Occupancy_Pct` is an
instantaneous 1 Hz sample, as the assignment specifies, so it misses any burst
that arrives and drains between two samples. `queuePeak()` updates a high-water
mark on every enqueue and `main` prints it at exit. In one stalled test run the
1 Hz column reported 0.39% while the true peak had been 22 slots (8.59%).

**Failures are loud.** A negative return from `lws_service()` means the context
itself is gone, not that a connection dropped (the retry policy handles those
without ever returning here). The producer used to just end its loop and
return, leaving `g_running` set, so `main` stayed parked in `sigtimedwait` and
the monitor kept appending `0,0,0,0` rows for the rest of the day. Now it sets
`g_failed`, stops the program, and `main` exits non-zero so a supervisor
(`systemd Restart=on-failure`) can restart the capture.

**`producer_wake()` is serialised against context teardown.** `context` is
written by the producer thread and read by `producer_wake()`, which `main` and
the monitor call from their own threads; `ctx_lock` closes the
read-non-NULL / destroy / `lws_cancel_service(freed)` use-after-free window.

**Network resilience.** `lws_retry_bo_t` gives exponential backoff
(1/2/4/8/16 s, then 16 s forever) with 20% jitter. `secs_since_valid_ping = 30`
and `secs_since_valid_hangup = 60` detect a silently wedged TCP connection,
the usual overnight failure mode on Pi Zero W WiFi. During an outage the
monitor keeps writing lines with zero counts, so the gap appears in the data
rather than as missing rows.

**Fragment reassembly.** One JSON message can arrive split across several
`CLIENT_RECEIVE` callbacks. `lws_is_first_fragment` / `lws_is_final_fragment`
bracket the reassembly; anything exceeding `SLOTSIZE` is counted as oversize
rather than truncated into invalid JSON.

## Before the 24-hour run

```sh
sudo iw wlan0 set power_save off        # #1 cause of overnight dropouts
sudo timedatectl set-timezone Europe/Athens
timedatectl status                      # confirm NTP synchronised
sudo systemctl stop unattended-upgrades
vcgencmd get_throttled                  # 0x0 means no undervoltage
```

Not yet implemented: gating the first CSV line on a wall-clock start epoch so
the capture is exactly 86400 lines from 00:00:00. Currently `-d 86400` counts
from launch, so launch on the second.
