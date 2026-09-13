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
#include <string.h>
#include <sys/time.h>

#include "telemetry.h"


/*
 * Classification, split out of the consumer loop so it can be unit
 * tested without a queue, a socket or a thread. See
 * tests/test_classify.c ("make unit").
 *
 * cJSON_GetObjectItemCaseSensitive looks only at the ROOT object, so
 * a "kind" that appears inside a post's text, or inside any nested
 * object, cannot be mistaken for the real one -- a whole-message
 * strstr would get that wrong.
 */
int classify(const char *json, size_t len) {
    cJSON *root = cJSON_ParseWithLength(json, len);
    if (!root) return K_BADJSON;

    /* Not one of the three data kinds => a system/error message. */
    int idx = K_INFO;

    cJSON *kind = cJSON_GetObjectItemCaseSensitive(root, "kind");
    if (cJSON_IsString(kind) && kind->valuestring) {
        const char *k = kind->valuestring;
        if      (strcmp(k, "commit")   == 0) idx = K_COMMIT;
        else if (strcmp(k, "identity") == 0) idx = K_IDENTITY;
        else if (strcmp(k, "account")  == 0) idx = K_ACCOUNT;
    }

    cJSON_Delete(root);
    return idx;
}


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
    queue_entry     entry; /* ~16 KB on the thread stack, no malloc */
    struct timeval  dequeue_time;
    double          wait_us;
    int             kind;

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
        kind = classify(entry.msg, entry.len);
        if (kind == K_BADJSON)
            counters_bump_badjson();
        else
            counters_count(kind);
    }

    return NULL; /* never reached */
}
