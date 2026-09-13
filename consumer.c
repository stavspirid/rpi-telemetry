/*
 * File    : consumer.c
 *
 * Desc    : Thread 2. Wakes on notEmpty, dequeues one JSON frame,
 *           parses it and increments the counter for its "kind".
 *
 *           No printf and no file I/O anywhere in this thread, as
 *           required. Everything it learns goes into the
 *           mutex-protected counters.
 */

#include <cjson/cJSON.h>
#include <sys/time.h>

#include "telemetry.h"


/* Cleanup handler: unlocks the mutex if the consumer is cancelled */
static void consumer_mutex_cleanup(void *arg) {
    pthread_mutex_unlock((pthread_mutex_t *)arg);
}

void *consumer(void *args) {
    thread_args *targs = (thread_args *)args;
    queue       *fifo  = targs->fifo;

    /*
     * Declare BEFORE pthread_cleanup_push because that macro opens a
     * new scope block; variables declared inside it would be invisible
     * after pthread_cleanup_pop closes that block.
     */
    queue_entry     entry; /* ~8 KB on the thread stack, no malloc */
    struct timeval  dequeue_time;
    double          wait_us;
    cJSON          *root;
    cJSON          *kind;

    /*
     * Deferred cancellation (the default, stated explicitly). The
     * thread is only cancelled at cancellation points, in practice
     * inside pthread_cond_wait below.
     */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    /* only stops when cancelled in main */
    while (1) {
        pthread_mutex_lock(fifo->mut);

        /* Releases the mutex if the thread is cancelled in cond_wait */
        pthread_cleanup_push(consumer_mutex_cleanup, fifo->mut);

        while (fifo->empty)
            pthread_cond_wait(fifo->notEmpty, fifo->mut); /* cancel point */

        queueDel(fifo, &entry);

        /* Timestamp inside the lock */
        gettimeofday(&dequeue_time, NULL);

        /* 0 = do not execute the handler (remove the handler). */
        pthread_cleanup_pop(0);

        pthread_mutex_unlock(fifo->mut);
        pthread_cond_signal(fifo->notFull);

        /* Wait time from enqueue to dequeue (in us). */
        wait_us =
            (double)(dequeue_time.tv_sec - entry.enqueue_time.tv_sec) * 1e6 +
            (double)(dequeue_time.tv_usec - entry.enqueue_time.tv_usec);
        stats_update(&g_wait, wait_us);

        /*
         * Parse OUTSIDE the queue lock. This is the expensive part of
         * the thread and holding the lock across it would stall both
         * the producer and the monitor.
         */
        root = cJSON_ParseWithLength(entry.msg, entry.len);
        if (!root) {
            counters_bump_badjson();
            continue;
        }

        kind = cJSON_GetObjectItemCaseSensitive(root, "kind");
        counters_count(cJSON_IsString(kind) ? kind->valuestring : NULL);

        cJSON_Delete(root);
    }

    return NULL; /* never reached */
}
