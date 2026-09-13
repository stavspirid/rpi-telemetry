#ifndef TELEMETRY_H
#define TELEMETRY_H

#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>

#include "queue.h"

/* Tunable constants */
#define JS_HOST "jetstream1.us-east.bsky.network"
#define JS_PORT 443
#define JS_PATH "/subscribe?wantedCollections=app.bsky.feed.post"

#define LOG_PATH "metrics_log.txt"
#define MONITOR_RT_PRIO  50 /* SCHED_FIFO priority; 0 disables */

/*
 * The "kind" field carried by every Jetstream frame.
 *
 * There used to be a K_OTHER bucket for frames whose kind was absent
 * or unrecognised. Nothing ever logged it, so such a frame was
 * silently lost. The assignment glosses the fourth counter as
 * "info (mynymata systimatos/sfalmaton)" -- system/error messages --
 * so anything that parses but is not one of the three data kinds is
 * exactly that, and belongs in K_INFO where it shows up in the CSV.
 * A 75 s live sample saw commit=3632 identity=20 account=22 and zero
 * of everything else, so in steady state this changes nothing; it
 * only matters when something unusual arrives, which is precisely
 * when the column needs to work.
 */
enum { K_COMMIT = 0, K_IDENTITY, K_ACCOUNT, K_INFO, K_NUM };

/* classify() return for input that is not valid JSON at all. */
#define K_BADJSON (-1)

/*
 * Classify one raw JSON frame by its TOP-LEVEL "kind" field.
 * Returns K_COMMIT / K_IDENTITY / K_ACCOUNT / K_INFO, or K_BADJSON.
 *
 * Pure: no counters touched, no I/O, no globals. That is what makes
 * it unit-testable in isolation (see tests/test_classify.c).
 */
int classify(const char *json, size_t len);

/* Thread argument bundle */
typedef struct {
    queue      *fifo;
    const char *log_path;
    long        run_seconds; /* 0 = run until signalled */
    int         id;
} thread_args;

/* Online statistics (Welford), carried over from assignment 1 */
typedef struct {
    long            count;
    double          mean;
    double          M2; /* running sum of squared deviations */
    double          min;
    double          max;
    pthread_mutex_t lock;
} stats_t;

void stats_init(stats_t *s);
void stats_update(stats_t *s, double value);
void stats_print(const stats_t *s, const char *title, const char *unit);

/* Per-kind message counters (own lock, separate from the queue's) */
void counters_init(void);
void counters_destroy(void);
void counters_count(int kind); /* kind is a K_* index */
void counters_snapshot(unsigned long out[K_NUM]); /* copies, then zeroes */

void counters_bump_drop(void);
void counters_bump_oversize(void);
void counters_bump_badjson(void);
void counters_totals(unsigned long *parsed, unsigned long *drops,
                     unsigned long *oversize, unsigned long *badjson);

/* CPU usage, sampled from /proc/stat */
typedef struct {
    unsigned long long total;
    unsigned long long idle;
} cpu_sample;

int    cpu_read(cpu_sample *s);
void   cpu_close(void);
double cpu_usage_pct(const cpu_sample *prev, const cpu_sample *cur);

/* Thread entry points */
void *producer(void *args);
void *consumer(void *args);
void *monitor(void *args);

/* Wakes the producer out of lws_service() so it can see !g_running.
   Safe to call from another thread. */
void producer_wake(void);

/* Defined in telemetry.c.

   g_running was a volatile sig_atomic_t. No signal handler is ever
   installed (signals are collected with sigtimedwait), so the
   sig_atomic_t buys nothing, while the plain volatile access is a
   genuine data race that ThreadSanitizer rightly reports. atomic_int
   reads and writes with the same syntax and makes `make tsan` clean. */
extern atomic_int g_running;

/* Set by any thread that dies for a reason other than a clean stop,
   so main can exit non-zero and a supervisor can restart the capture
   instead of leaving it to log zeroes for the rest of the day. */
extern atomic_int g_failed;

extern stats_t g_wait; /* enqueue -> dequeue wait time, in us */

#endif /* TELEMETRY_H */
