/*
 * File    : monitor.c
 *
 * Desc    : Thread 3. Wakes strictly on the second, snapshots the
 *           counters, measures buffer occupancy and CPU usage, and
 *           appends one CSV line to the log file.
 *
 *           Timing uses clock_nanosleep with TIMER_ABSTIME against a
 *           deadline advanced by += 1s, never "now + 1s". A late
 *           wakeup does not push the next deadline out, so error
 *           cannot accumulate: drift is zero by construction.
 *
 *           The one exception is an overrun, where the deadline has
 *           already passed by the time we get to it. See the overrun
 *           guard at the bottom of the loop.
 */

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "telemetry.h"


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

void *monitor(void *args) {
    thread_args *targs = (thread_args *)args;
    queue       *fifo  = targs->fifo;

    struct timespec next;
    struct timespec now;
    cpu_sample      prev;
    cpu_sample      cur;
    unsigned long   count[K_NUM];
    long            occupied;
    double          occupancy;
    double          cpu_pct;
    FILE           *f;
    long            written      = 0;
    long            overruns     = 0; /* deadlines that had already passed */
    long            skipped_secs = 0;
    long            worst_skip   = 0;
    int             rc;

    set_realtime_priority();

    f = fopen(targs->log_path, "a");
    if (!f) {
        perror("monitor: fopen");
        g_failed  = 1;
        g_running = 0;
        producer_wake();
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
         * Actual wake time. Because the ideal deadline is always a
         * whole second, jitter is implicit in the nanoseconds field:
         * positive jitter is tv_nsec, negative is tv_nsec - 1e9.
         */
        clock_gettime(CLOCK_REALTIME, &now);

        counters_snapshot(count);

        /* Caller-locks convention, same as the producer/consumer. */
        pthread_mutex_lock(fifo->mut);
        occupied = queueCount(fifo);
        pthread_mutex_unlock(fifo->mut);
        occupancy = 100.0 * (double)occupied / (double)QUEUESIZE;

        cpu_pct = 0.0;
        if (cpu_read(&cur) == 0) {
            cpu_pct = cpu_usage_pct(&prev, &cur);
            prev    = cur;
        }

        fprintf(f, "%ld,%ld,%lu,%lu,%lu,%lu,%.2f,%.2f\n",
                (long)now.tv_sec, (long)now.tv_nsec,
                count[K_COMMIT], count[K_IDENTITY],
                count[K_ACCOUNT], count[K_INFO],
                occupancy, cpu_pct);

        written++;
        if (targs->run_seconds > 0 && written >= targs->run_seconds) {
            g_running = 0;
            break;
        }

        next.tv_sec += 1; /* absolute deadline => zero drift */

        /*
         * Overrun guard.
         *
         * If the deadline we just set is ALREADY in the past -- a
         * scheduling stall, an NTP step, a suspended process --
         * clock_nanosleep returns instantly and the loop fires
         * back-to-back trying to catch up. That is wrong twice over.
         * It stamps several rows with the same Seconds value (a 4 s
         * stall produced four rows 300 us apart, all stamped the same
         * second, while three seconds vanished from the column). And
         * it breaks the invariant that the ideal deadline is always a
         * whole second -- the invariant that lets tv_nsec be read as
         * signed jitter. A row written 3.68 s late carried
         * tv_nsec = 682240320, which that reading turns into
         * -317.8 ms: a large overrun plotted as a small NEGATIVE
         * jitter, in the one plot whose whole purpose is to show it.
         *
         * So re-align to the next whole second in the future instead.
         * Every row that is written then sits on the grid and its
         * tv_nsec is true jitter; the seconds we could not sample are
         * simply absent from the Seconds column, where post-processing
         * can see them as a gap.
         */
        clock_gettime(CLOCK_REALTIME, &now);
        if (next.tv_sec <= now.tv_sec) {
            long skipped = (long)(now.tv_sec - next.tv_sec) + 1;

            overruns++;
            skipped_secs += skipped;
            if (skipped > worst_skip) worst_skip = skipped;

            next.tv_sec  = now.tv_sec + 1;
            next.tv_nsec = 0;
        }
    }

    fclose(f);

    if (overruns)
        fprintf(stderr,
                "monitor: %ld deadline overrun(s), %ld second(s) skipped, "
                "worst %ld s. Every logged row is still on the whole "
                "second; the gaps are visible in the Seconds column.\n",
                overruns, skipped_secs, worst_skip);

    /* Let main out of its wait loop if we stopped on our own. */
    producer_wake();

    return NULL;
}
