#ifndef TELEMETRY_H
#define TELEMETRY_H

#include <stdatomic.h>

#include "queue.h"

/* Tunable constants */
#define JS_HOST "jetstream1.us-east.bsky.network"
#define JS_PORT 443
#define JS_PATH "/subscribe?wantedCollections=app.bsky.feed.post"

#define LOG_PATH "metrics_log.txt"
#define MONITOR_RT_PRIO 50 /* SCHED_FIFO priority; 0 disables */

/*
 * The "kind" field carried by every Jetstream frame.
 *
 * There is no K_OTHER bucket. The assignment glosses the fourth counter
 * as "info (system/error messages)", so anything that parses but is not
 * one of the three data kinds is exactly that and belongs in K_INFO,
 * where it shows up in the CSV instead of being silently dropped.
 */
enum { K_COMMIT = 0, K_IDENTITY, K_ACCOUNT, K_INFO, K_NUM };

#define K_BADJSON (-1) /* classify() return for unparseable input */

/*
 * Shared state, defined in telemetry.c.
 *
 * g_running is atomic_int rather than volatile sig_atomic_t: no signal
 * handler is ever installed (signals are collected with sigtimedwait),
 * so sig_atomic_t buys nothing, while the plain volatile access is a
 * real data race that ThreadSanitizer rightly reports.
 */
extern queue      *g_fifo;
extern atomic_int  g_running;
extern atomic_int  g_failed; /* non-zero exit => supervisor restarts */
extern const char *g_log_path;
extern long        g_run_seconds; /* 0 = run until signalled */

/*
 * The four per-kind counters (telemetry.c). The assignment requires
 * them to be global and mutex-protected: the consumer increments them,
 * the monitor snapshots and zeroes them once a second. That lock is
 * separate from the queue's, and no thread ever holds one while taking
 * the other, so the two cannot be acquired in opposite orders.
 */
void counters_count(int kind);
void counters_snapshot(unsigned long out[K_NUM]);

/* Thread entry points */
void *producer(void *unused); /* Thread 1, producer.c */
void *consumer(void *unused); /* Thread 2, consumer.c */
void *monitor(void *unused);  /* Thread 3, monitor.c  */

/*
 * Stream lifecycle (producer.c). The libwebsockets context is created
 * before any thread exists and destroyed once they are all joined,
 * which is what makes stream_wake() safe to call from main and from the
 * monitor with no lock at all: the pointer never changes while another
 * thread can see it. Keeping it static inside producer.c also keeps
 * libwebsockets out of every other file.
 */
int  stream_init(void);
void stream_wake(void);
void stream_destroy(void);

/*
 * Diagnostics printed by main once every thread is joined. Each is
 * written by exactly one thread, so pthread_join is all the
 * synchronisation they need.
 */
extern unsigned long g_drops, g_oversize; /* producer.c */
extern unsigned long g_parsed, g_badjson; /* consumer.c */

void wait_stats_print(void); /* consumer.c */

/* Classification. Exposed so tests/test_classify.c can reach it. */
int classify(const char *json, size_t len);

#endif /* TELEMETRY_H */
