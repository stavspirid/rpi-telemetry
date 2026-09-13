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
as before, so one 16 MB `malloc` at startup covers the entire run. There is no
allocation on the network path at all, which is most of the "no leaks over 24
hours" argument.

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
`tv_nsec < 5e8 ? tv_nsec/1e6 : (tv_nsec - 1e9)/1e6` milliseconds.

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
