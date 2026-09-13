/*
 * File    : monitor.c
 *
 * Desc    : Thread 3. Wakes strictly on the second, snapshots the
 *           counters, measures buffer occupancy and CPU usage, and
 *           appends one CSV line to the log file.
 *
 *           Timing uses clock_nanosleep with TIMER_ABSTIME against a
 *           deadline advanced by += 1s, never "now + 1s". A late wakeup
 *           does not push the next deadline out, so error cannot
 *           accumulate: drift is zero by construction.
 *
 *           Also owns the /proc/stat CPU sampling, since this is the
 *           only thread that reads it.
 */

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "telemetry.h"

/* ------------------------------------------------------------------ *
 *  CPU usage, sampled from /proc/stat
 *
 *  Opened once and re-read with pread(fd, ..., 0): one syscall per
 *  second, no stdio buffering, no allocation.
 * ------------------------------------------------------------------ */

typedef struct {
    unsigned long long total;
    unsigned long long idle;
} cpu_sample;

static int stat_fd = -1;

static int cpu_read(cpu_sample *s) {
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
    fields = sscanf(
        buf, "cpu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu", &v[0],
        &v[1], &v[2], &v[3], &v[4], &v[5], &v[6], &v[7], &v[8], &v[9]);
    if (fields < 4) return -1;

    s->total = 0;
    for (i = 0; i < fields; i++) s->total += v[i];

    s->idle = v[3] + (fields > 4 ? v[4] : 0ULL); /* idle + iowait */

    return 0;
}

static double cpu_usage_pct(const cpu_sample *prev, const cpu_sample *cur) {
    unsigned long long d_total, d_idle;

    if (cur->total <= prev->total) return 0.0;

    d_total = cur->total - prev->total;
    d_idle  = cur->idle - prev->idle;
    if (d_idle > d_total) d_idle = d_total;

    return 100.0 * (double)(d_total - d_idle) / (double)d_total;
}

/* ------------------------------------------------------------------ *
 *  The thread
 * ------------------------------------------------------------------ */

static void set_realtime_priority(void) {
#if MONITOR_RT_PRIO > 0
    struct sched_param sp;

    memset(&sp, 0, sizeof(sp));
    sp.sched_priority = MONITOR_RT_PRIO;

    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp) != 0)
        fprintf(stderr,
                "monitor: SCHED_FIFO unavailable, staying on SCHED_OTHER "
                "(run as root for lower jitter)\n");
#endif
}

void *monitor(void *unused) {
    struct timespec next, now;
    cpu_sample      prev, cur;
    unsigned long   count[K_NUM];
    long            occupied;
    double          occupancy, cpu_pct;
    FILE           *f;
    long            written  = 0;
    long            overruns = 0, skipped = 0, worst = 0;
    int             rc;

    (void)unused;

    set_realtime_priority();

    f = fopen(g_log_path, "a");
    if (!f) {
        perror("monitor: fopen");
        g_failed  = 1;
        g_running = 0;
        stream_wake();
        return NULL;
    }
    setvbuf(f, NULL, _IOLBF, 0); /* flush on every newline */

    if (cpu_read(&prev) != 0) memset(&prev, 0, sizeof(prev));

    /* First deadline: the next whole second on the wall clock. */
    clock_gettime(CLOCK_REALTIME, &next);
    next.tv_nsec = 0;
    next.tv_sec += 1;

    while (g_running) {
        do {
            rc = clock_nanosleep(CLOCK_REALTIME, TIMER_ABSTIME, &next, NULL);
        } while (rc == EINTR && g_running);

        if (!g_running) break;
        if (rc != 0) {
            fprintf(stderr, "monitor: clock_nanosleep: %s\n", strerror(rc));
            g_failed  = 1;
            g_running = 0;
            break;
        }

        /*
         * Actual wake time. Because the ideal deadline is always a whole
         * second, jitter is implicit in the nanoseconds field: positive
         * jitter is tv_nsec, negative is tv_nsec - 1e9.
         */
        clock_gettime(CLOCK_REALTIME, &now);

        counters_snapshot(count);

        /* Caller-locks convention, same as producer and consumer. */
        pthread_mutex_lock(g_fifo->mut);
        occupied = queueCount(g_fifo);
        pthread_mutex_unlock(g_fifo->mut);
        occupancy = 100.0 * (double)occupied / (double)QUEUESIZE;

        cpu_pct = 0.0;
        if (cpu_read(&cur) == 0) {
            cpu_pct = cpu_usage_pct(&prev, &cur);
            prev    = cur;
        }

        fprintf(f, "%ld,%ld,%lu,%lu,%lu,%lu,%.2f,%.2f\n", (long)now.tv_sec,
                (long)now.tv_nsec, count[K_COMMIT], count[K_IDENTITY],
                count[K_ACCOUNT], count[K_INFO], occupancy, cpu_pct);

        written++;
        if (g_run_seconds > 0 && written >= g_run_seconds) {
            g_running = 0;
            break;
        }

        next.tv_sec += 1; /* absolute deadline => zero drift */

        /*
         * Overrun guard. If that deadline has already passed -- a
         * scheduling stall, an NTP step, a suspended process -- firing
         * back-to-back to catch up would stamp several rows with the
         * same second, and would break the invariant that the ideal
         * deadline is always a whole second. That invariant is what lets
         * tv_nsec be read as signed jitter, so a row written 3.68 s late
         * (tv_nsec = 682240320) would be plotted as -317.8 ms: a large
         * overrun shown as a small negative jitter. Re-align to the next
         * whole second instead; the seconds that could not be sampled
         * are then visible as a gap in the Seconds column.
         */
        clock_gettime(CLOCK_REALTIME, &now);
        if (next.tv_sec <= now.tv_sec) {
            long n = (long)(now.tv_sec - next.tv_sec) + 1;

            overruns++;
            skipped += n;
            if (n > worst) worst = n;

            next.tv_sec  = now.tv_sec + 1;
            next.tv_nsec = 0;
        }
    }

    fclose(f);
    if (stat_fd >= 0) close(stat_fd);

    if (overruns)
        fprintf(stderr,
                "monitor: %ld deadline overrun(s), %ld second(s) skipped, "
                "worst %ld s. Every logged row is still on the whole "
                "second; the gaps are visible in the Seconds column.\n",
                overruns, skipped, worst);

    /* Let main out of its wait loop if we stopped on our own. */
    stream_wake();

    return NULL;
}
