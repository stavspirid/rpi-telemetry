/*
 * File    : telemetry.c
 *
 * Title   : Real-time Jetstream telemetry logger.
 *
 * Desc    : Producer/Consumer over a bounded circular buffer, built
 *           on the queue from assignment 1.
 *             Thread 1 (producer) : libwebsockets client, event-driven
 *             Thread 2 (consumer) : parses JSON, classifies "kind"
 *             Thread 3 (monitor)  : strict 1 Hz CSV logger
 *
 * Usage   : ./telemetry [-o logfile] [-d seconds]
 *
 * Compile : make    (gcc -O2 -Wall -lwebsockets -lcjson -lpthread -lm)
 */

#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>

#include "telemetry.h"

atomic_int g_running = 1;
atomic_int g_failed  = 0;
stats_t    g_wait;    /* enqueue -> dequeue wait time, in us */

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

    const char *log_path    = LOG_PATH;
    long        run_seconds = 0;

    // Parse command line arguments
    // configure output log file and run duration
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-o") && i + 1 < argc) {
            log_path = argv[++i];
        } else if (!strcmp(argv[i], "-d") && i + 1 < argc) {
            run_seconds = atol(argv[++i]);
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

    /* Keep every page resident: a major fault in the monitor thread
       is a millisecond of jitter we do not need. */
    if (mlockall(MCL_CURRENT | MCL_FUTURE) != 0)
        fprintf(stderr, "Warning: mlockall failed (%s), continuing.\n",
                strerror(errno));

    /* Initialise shared queue and statistics */
    queue *fifo = queueInit();
    if (!fifo) {
        fprintf(stderr, "queueInit failed.\n");
        return 1;
    }
    stats_init(&g_wait);
    counters_init();

    printf("Logging to %s, queue %d slots x %d B (%.1f MB).\n", log_path,
           QUEUESIZE, SLOTSIZE, (double)sizeof(queue) / (1024.0 * 1024.0));

    /* One argument bundle per thread, as in assignment 1 */
    thread_args targs[3];
    pthread_t   prod_t, cons_t, mon_t;

    for (i = 0; i < 3; i++) {
        targs[i].fifo        = fifo;
        targs[i].log_path    = log_path;
        targs[i].run_seconds = run_seconds;
        targs[i].id          = i;
    }

    /* Consumer first, so it is ready before any frame arrives */
    if (pthread_create(&cons_t, NULL, consumer, &targs[1]) != 0 ||
        pthread_create(&prod_t, NULL, producer, &targs[0]) != 0 ||
        pthread_create(&mon_t, NULL, monitor, &targs[2]) != 0) {
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

    /* Producer cannot be cancelled safely from inside libwebsockets,
       so wake its event loop and let it unwind itself. */
    producer_wake();
    pthread_join(prod_t, NULL);
    pthread_join(mon_t, NULL);

    /* Consumer is parked in pthread_cond_wait. Cancel and reap it,
       exactly as in assignment 1. */
    pthread_cancel(cons_t);
    pthread_join(cons_t, NULL);

    /* Final statistics */
    unsigned long parsed, drops, oversize, badjson;
    counters_totals(&parsed, &drops, &oversize, &badjson);

    /* The CSV samples occupancy once a second, as specified, so it
       misses any burst that arrives and drains in between. The
       high-water mark does not. */
    pthread_mutex_lock(fifo->mut);
    long peak = queuePeak(fifo);
    pthread_mutex_unlock(fifo->mut);

    stats_print(&g_wait, "Queue Wait Time (enqueue -> dequeue)", "us");
    printf("  Messages parsed  : %lu\n", parsed);
    printf("  Dropped (full)   : %lu\n", drops);
    printf("  Oversize frames  : %lu\n", oversize);
    printf("  Malformed JSON   : %lu\n", badjson);
    printf("  Peak buffer use  : %ld / %d slots (%.2f%%)\n\n", peak, QUEUESIZE,
           100.0 * (double)peak / (double)QUEUESIZE);

    /* Cleanup */
    pthread_mutex_destroy(&g_wait.lock);
    counters_destroy();
    cpu_close();
    queueDelete(fifo);

    /* Non-zero tells a supervisor (systemd Restart=on-failure, a shell
       loop) that the capture aborted rather than finishing cleanly. */
    return g_failed ? 1 : 0;
}
