#ifndef TELEMETRY_H
#define TELEMETRY_H

#include <pthread.h>
#include <signal.h>

#include "queue.h"

/* Tunable constants */
#define JS_HOST "jetstream1.us-east.bsky.network"
#define JS_PORT 443
#define JS_PATH "/subscribe?wantedCollections=app.bsky.feed.post"

#define LOG_PATH "metrics_log.txt"
#define MONITOR_RT_PRIO  50 /* SCHED_FIFO priority; 0 disables */

/* The "kind" field carried by every Jetstream frame */
enum { K_COMMIT = 0, K_IDENTITY, K_ACCOUNT, K_INFO, K_OTHER, K_NUM };

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
void counters_count(const char *kind);
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

/* Defined in telemetry.c */
extern volatile sig_atomic_t g_running;
extern stats_t g_wait; /* enqueue -> dequeue wait time, in us */

#endif /* TELEMETRY_H */
