/*
 * File    : stats.c
 *
 * Desc    : Three things that all need protecting from concurrent
 *           access, kept together:
 *             - the Welford wait-time statistics from assignment 1
 *             - the per-kind message counters
 *             - CPU usage sampled from /proc/stat
 */

#include <fcntl.h>
#include <float.h>
#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "telemetry.h"


/* ------------------------------------------------------------------ *
 *  Online statistics (unchanged from assignment 1)
 * ------------------------------------------------------------------ */

void stats_init(stats_t *s) {
    s->count = 0;
    s->mean  = 0.0;
    s->M2    = 0.0;
    s->min   = DBL_MAX;
    s->max   = -DBL_MAX; /* fp numbers are symmetrical */
    pthread_mutex_init(&s->lock, NULL);
}

void stats_update(stats_t *s, double value) {
    pthread_mutex_lock(&s->lock);
    s->count++;
    double delta = value - s->mean;
    s->mean += delta / s->count;
    double delta2 = value - s->mean;
    s->M2 += delta * delta2;
    if (value < s->min) s->min = value;
    if (value > s->max) s->max = value;
    pthread_mutex_unlock(&s->lock);
}

void stats_print(const stats_t *s, const char *title, const char *unit) {
    double stddev = (s->count > 1) ? sqrt(s->M2 / (s->count - 1)) : 0.0;
    printf("\n=== %s ===\n", title);
    printf("  Samples : %ld\n", s->count);
    printf("  Mean    : %.3f %s\n", s->mean, unit);
    printf("  Std Dev : %.3f %s\n", stddev, unit);
    printf("  Min     : %.3f %s\n", (s->count ? s->min : 0.0), unit);
    printf("  Max     : %.3f %s\n", (s->count ? s->max : 0.0), unit);
    printf("=================================================\n\n");
}

/* ------------------------------------------------------------------ *
 *  Message counters
 *
 *  A separate lock from the queue's. The consumer holds it for one
 *  increment; the monitor holds it for a snapshot-and-zero. Neither
 *  thread ever holds this lock while taking the queue lock, so the
 *  two can never be acquired in opposite orders.
 * ------------------------------------------------------------------ */

typedef struct {
    unsigned long   kind[K_NUM]; /* reset every second by the monitor */
    unsigned long   parsed;      /* lifetime totals below */
    unsigned long   drops;
    unsigned long   oversize;
    unsigned long   badjson;
    pthread_mutex_t lock;
} counters_t;

static counters_t g_counters;

void counters_init(void) {
    memset(&g_counters, 0, sizeof(g_counters));

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);
    pthread_mutex_init(&g_counters.lock, &attr);
    pthread_mutexattr_destroy(&attr);
}

void counters_destroy(void) {
    pthread_mutex_destroy(&g_counters.lock);
}

void counters_count(const char *kind) {
    int idx = K_OTHER;

    if (kind) {
        if      (strcmp(kind, "commit")   == 0) idx = K_COMMIT;
        else if (strcmp(kind, "identity") == 0) idx = K_IDENTITY;
        else if (strcmp(kind, "account")  == 0) idx = K_ACCOUNT;
        else if (strcmp(kind, "info")     == 0) idx = K_INFO;
    }

    pthread_mutex_lock(&g_counters.lock);
    g_counters.kind[idx]++;
    g_counters.parsed++;
    pthread_mutex_unlock(&g_counters.lock);
}

void counters_snapshot(unsigned long out[K_NUM]) {
    pthread_mutex_lock(&g_counters.lock);
    memcpy(out, g_counters.kind, sizeof(g_counters.kind));
    memset(g_counters.kind, 0, sizeof(g_counters.kind));
    pthread_mutex_unlock(&g_counters.lock);
}

void counters_bump_drop(void) {
    pthread_mutex_lock(&g_counters.lock);
    g_counters.drops++;
    pthread_mutex_unlock(&g_counters.lock);
}

void counters_bump_oversize(void) {
    pthread_mutex_lock(&g_counters.lock);
    g_counters.oversize++;
    pthread_mutex_unlock(&g_counters.lock);
}

void counters_bump_badjson(void) {
    pthread_mutex_lock(&g_counters.lock);
    g_counters.badjson++;
    pthread_mutex_unlock(&g_counters.lock);
}

void counters_totals(unsigned long *parsed, unsigned long *drops,
                     unsigned long *oversize, unsigned long *badjson) {
    pthread_mutex_lock(&g_counters.lock);
    *parsed   = g_counters.parsed;
    *drops    = g_counters.drops;
    *oversize = g_counters.oversize;
    *badjson  = g_counters.badjson;
    pthread_mutex_unlock(&g_counters.lock);
}

/* ------------------------------------------------------------------ *
 *  CPU usage
 *
 *  /proc/stat is opened once and re-read with pread(fd, ..., 0).
 *  One syscall per second, no stdio buffering, no allocation.
 *  Only the monitor thread calls these, so no lock is needed.
 * ------------------------------------------------------------------ */

static int stat_fd = -1;

int cpu_read(cpu_sample *s) {
    char               buf[256];
    unsigned long long v[10];
    ssize_t            n;
    int                fields, i;

    if (stat_fd < 0) {
        stat_fd = open("/proc/stat", O_RDONLY | O_CLOEXEC);
        if (stat_fd < 0) return -1;
    }

    n = pread(stat_fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return -1;
    buf[n] = '\0';

    memset(v, 0, sizeof(v));
    fields = sscanf(buf, "cpu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                    &v[0], &v[1], &v[2], &v[3], &v[4],
                    &v[5], &v[6], &v[7], &v[8], &v[9]);
    if (fields < 4) return -1;

    s->total = 0;
    for (i = 0; i < fields; i++) s->total += v[i];

    s->idle = v[3] + (fields > 4 ? v[4] : 0ULL); /* idle + iowait */

    return 0;
}

void cpu_close(void) {
    if (stat_fd >= 0) {
        close(stat_fd);
        stat_fd = -1;
    }
}

double cpu_usage_pct(const cpu_sample *prev, const cpu_sample *cur) {
    unsigned long long d_total, d_idle;

    if (cur->total <= prev->total) return 0.0;

    d_total = cur->total - prev->total;
    d_idle  = cur->idle - prev->idle;
    if (d_idle > d_total) d_idle = d_total;

    return 100.0 * (double)(d_total - d_idle) / (double)d_total;
}
