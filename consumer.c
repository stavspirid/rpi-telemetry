/*
 * File    : consumer.c
 *
 * Desc    : Thread 2. Wakes on notEmpty, dequeues one JSON frame,
 *           parses it and increments the counter for its "kind".
 *
 *           No printf and no file I/O anywhere in this thread, as
 *           required. (wait_stats_print below is called by main after
 *           the thread has been joined, not from the thread itself.)
 *
 *           Also owns the Welford wait-time statistics carried over
 *           from assignment 1, since this is the only thread that
 *           feeds them.
 */

#include <cjson/cJSON.h>
#include <float.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "telemetry.h"

unsigned long g_parsed, g_badjson;  // this thread only, no lock needed

typedef struct {
    long   count;
    double mean;
    double M2;  // running sum of squared deviations
    double min;
    double max;
} stats_t;

static stats_t g_wait = {.min = DBL_MAX, .max = -DBL_MAX};

static void stats_update(stats_t *s, double value) {
    s->count++;
    double delta = value - s->mean;
    s->mean += delta / s->count;
    double delta2 = value - s->mean;
    s->M2 += delta * delta2;
    if (value < s->min) s->min = value;
    if (value > s->max) s->max = value;
}

// Called from main, after this thread has been joined. 
void wait_stats_print(void) {
    const stats_t *s      = &g_wait;
    double         stddev = (s->count > 1) ? sqrt(s->M2 / (s->count - 1)) : 0.0;

    printf("\n=== Queue Wait Time (enqueue -> dequeue) ===\n");
    printf("  Samples : %ld\n", s->count);
    printf("  Mean    : %.3f us\n", s->mean);
    printf("  Std Dev : %.3f us\n", stddev);
    printf("  Min     : %.3f us\n", (s->count ? s->min : 0.0));
    printf("  Max     : %.3f us\n", (s->count ? s->max : 0.0));
    printf("============================================\n\n");
}


// Classification

/*
 * Classify one raw JSON frame by its TOP-LEVEL "kind" field.
 * Returns K_COMMIT / K_IDENTITY / K_ACCOUNT / K_INFO, or K_BADJSON.
 *
 * cJSON_GetObjectItemCaseSensitive looks only at the root object
 * A "kind" appearing inside a post's text or inside a nested object
 * cannot be mistaken for the real one
 */

int classify(const char *json, size_t len) {
    cJSON *root = cJSON_ParseWithLength(json, len);
    if (!root) return K_BADJSON;

    int idx = K_INFO;

    cJSON *kind = cJSON_GetObjectItemCaseSensitive(root, "kind");
    if (cJSON_IsString(kind) && kind->valuestring) {
        const char *k = kind->valuestring;
        if (strcmp(k, "commit") == 0)
            idx = K_COMMIT;
        else if (strcmp(k, "identity") == 0)
            idx = K_IDENTITY;
        else if (strcmp(k, "account") == 0)
            idx = K_ACCOUNT;
    }

    cJSON_Delete(root);
    return idx;
}

// Thread entry

// Cleanup handler that unlocks the mutex
static void consumer_mutex_cleanup(void *arg) {
    pthread_mutex_unlock((pthread_mutex_t *)arg);
}

void *consumer(void *unused) {
    queue_entry    entry;       // ~16 KB on the thread stack, no malloc
    struct timeval dequeue_time;
    double         wait_us;
    int            kind;

    (void)unused;

    // Set the thread to be cancelable
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);

    // only stops when cancelled in main
    while (1) {
        pthread_mutex_lock(g_fifo->mut);

        // Releases the mutex if the thread is cancelled in cond_wait 
        pthread_cleanup_push(consumer_mutex_cleanup, g_fifo->mut);

        while (g_fifo->empty)
            pthread_cond_wait(g_fifo->notEmpty, g_fifo->mut); // cancel point 

        queueDel(g_fifo, &entry);

        // Timestamp inside the lock 
        gettimeofday(&dequeue_time, NULL);

        // 0 = do not execute the handler (remove the handler). 
        pthread_cleanup_pop(0);

        pthread_mutex_unlock(g_fifo->mut);
        pthread_cond_signal(g_fifo->notFull);

        // Wait time from enqueue to dequeue (in us). 
        wait_us =
            (double)(dequeue_time.tv_sec - entry.enqueue_time.tv_sec) * 1e6 +
            (double)(dequeue_time.tv_usec - entry.enqueue_time.tv_usec);
        stats_update(&g_wait, wait_us);

        //  Parse OUTSIDE the queue lock
        kind = classify(entry.msg, entry.len);
        if (kind == K_BADJSON) {
            g_badjson++;
        } else {
            counters_count(kind);
            g_parsed++;
        }
    }

    return NULL; // never reached 
}
