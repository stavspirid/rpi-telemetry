/*
 * File    : telemetry.c
 *
 * Title   : Real-time Jetstream telemetry logger.
 *
 * Desc    : Producer/Consumer over a bounded circular buffer, built on
 *           the queue from assignment 1.
 *             Thread 1 (producer.c) : libwebsockets client, event-driven
 *             Thread 2 (consumer.c) : parses JSON, classifies "kind"
 *             Thread 3 (monitor.c)  : strict 1 Hz CSV logger
 *
 *           This file holds main and the shared per-kind counters.
 *
 * Usage   : ./telemetry [-o logfile] [-d seconds]
 *
 * Compile : make  (gcc -O2 -Wall -lwebsockets -lcjson -lpthread -lm)
 */

#include "telemetry.h"

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>

queue      *g_fifo;
atomic_int  g_running     = 1;
atomic_int  g_failed      = 0;
const char *g_log_path    = LOG_PATH;
long        g_run_seconds = 0;

/* ------------------------------------------------------------------ *
 *  The four per-kind counters
 *
 *  Global and mutex-protected, as the assignment requires. The consumer
 *  increments; the monitor snapshots and zeroes once a second.
 * ------------------------------------------------------------------ */

static unsigned long   kind_count[K_NUM];
static pthread_mutex_t kind_lock;

void counters_count(int kind) {
    pthread_mutex_lock(&kind_lock);
    kind_count[kind]++;
    pthread_mutex_unlock(&kind_lock);
}

void counters_snapshot(unsigned long out[K_NUM]) {
    pthread_mutex_lock(&kind_lock);
    memcpy(out, kind_count, sizeof(kind_count));
    memset(kind_count, 0, sizeof(kind_count));
    pthread_mutex_unlock(&kind_lock);
}

// main

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [-o logfile] [-d seconds]\n"
            "  -o  output CSV file (default: %s)\n"
            "  -d  stop after N logged seconds (default: run until "
            "SIGINT/SIGTERM)\n",
            prog, LOG_PATH);
}

int main(int argc, char *argv[]) {
    int i;

    /* Parse command line arguments */
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-o") && i + 1 < argc) {
            g_log_path = argv[++i];
        } else if (!strcmp(argv[i], "-d") && i + 1 < argc) {
            g_run_seconds = atol(argv[++i]);
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    /*
     * Block SIGINT/SIGTERM in main BEFORE any thread is created, so
     * every thread inherits the mask. Signals are then collected
     * synchronously with sigtimedwait instead of by a handler. No
     * handler means no async-signal-safety problem when we go on to
     * call pthread_cancel and into libwebsockets.
     */
    sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &sigs, NULL) != 0) {
        perror("pthread_sigmask");
        return 1;
    }

    /* Keep every page resident: a major fault in the monitor thread is
       a millisecond of jitter we do not need. */
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0)
        fprintf(stderr, "Warning: mlockall failed (%s), continuing.\n",
                strerror(errno));

    /* Initialise shared queue and counters */
    g_fifo = queueInit();
    if (!g_fifo) {
        fprintf(stderr, "queueInit failed.\n");
        return 1;
    }

    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);
    pthread_mutex_init(&kind_lock, &attr);
    pthread_mutexattr_destroy(&attr);

    /* The websocket context is created here, before any thread exists,
       and destroyed below once they are all joined. */
    if (stream_init() != 0) {
        fprintf(stderr, "stream_init failed.\n");
        return 1;
    }

    printf("Logging to %s, queue %d slots x %d B (%.1f MB).\n", g_log_path,
           QUEUESIZE, SLOTSIZE, (double)sizeof(queue) / (1024.0 * 1024.0));

    pthread_t prod_t, cons_t, mon_t;

    /* Consumer first, so it is ready before any frame arrives */
    if (pthread_create(&cons_t, NULL, consumer, NULL) != 0 ||
        pthread_create(&prod_t, NULL, producer, NULL) != 0 ||
        pthread_create(&mon_t, NULL, monitor, NULL) != 0) {
        fprintf(stderr, "pthread_create failed.\n");
        return 1;
    }

    /* Wait for a signal, or for the monitor to finish a bounded run */
    struct timespec poll_interval = {1, 0};
    while (g_running) {
        if (sigtimedwait(&sigs, NULL, &poll_interval) > 0) {
            printf("Signal received. Shutting down...\n");
            break;
        }
    }

    g_running = 0;

    /* The producer cannot be cancelled safely from inside libwebsockets,
       so wake its event loop and let it unwind itself. */
    stream_wake();
    pthread_join(prod_t, NULL);
    pthread_join(mon_t, NULL);

    /* Consumer is parked in pthread_cond_wait. Cancel and reap it,
       exactly as in assignment 1. */
    pthread_cancel(cons_t);
    pthread_join(cons_t, NULL);

    /* Final statistics. Every thread is joined, so the single-writer
       counters can be read without a lock. */
    wait_stats_print();
    printf("  Messages parsed  : %lu\n", g_parsed);
    printf("  Dropped (full)   : %lu\n", g_drops);
    printf("  Oversize frames  : %lu\n", g_oversize);
    printf("  Malformed JSON   : %lu\n", g_badjson);
    printf("  Peak buffer use  : %ld / %d slots (%.2f%%)\n\n", g_fifo->peak,
           QUEUESIZE, 100.0 * (double)g_fifo->peak / (double)QUEUESIZE);

    /* Cleanup */
    stream_destroy();
    pthread_mutex_destroy(&kind_lock);
    queueDelete(g_fifo);

    /* Non-zero tells a supervisor (systemd Restart=on-failure) that the
       capture aborted rather than finishing cleanly. */
    return g_failed ? 1 : 0;
}
