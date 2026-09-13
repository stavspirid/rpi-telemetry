#include "queue.h"

#include <string.h>

queue *queueInit(void) {
    queue *q = (queue *)malloc(sizeof(queue)); /* ~4 MB, once, up front */
    if (!q) return NULL;

    q->empty = 1;
    q->full  = 0;
    q->head  = 0;
    q->tail  = 0;
    q->peak  = 0;

    /*
     * Priority inheritance. The monitor runs SCHED_FIFO and briefly
     * takes this lock. Without PRIO_INHERIT, a consumer preempted while
     * holding it would block the monitor for an unbounded time, which
     * is exactly the jitter we are trying to avoid.
     */
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);

    q->mut = (pthread_mutex_t *)malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(q->mut, &attr);
    pthread_mutexattr_destroy(&attr);

    q->notFull = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
    pthread_cond_init(q->notFull, NULL);

    q->notEmpty = (pthread_cond_t *)malloc(sizeof(pthread_cond_t));
    pthread_cond_init(q->notEmpty, NULL);

    return q;
}

void queueDelete(queue *q) {
    pthread_mutex_destroy(q->mut);
    free(q->mut);
    pthread_cond_destroy(q->notFull);
    free(q->notFull);
    pthread_cond_destroy(q->notEmpty);
    free(q->notEmpty);
    free(q);
}

void queueAdd(queue *q, const queue_entry *in) {
    queue_entry *slot = &q->buf[q->tail];
    long         used;

    /* Copy only the bytes in use, not the whole slot. */
    slot->len = (in->len < SLOTSIZE) ? in->len : SLOTSIZE - 1;
    memcpy(slot->msg, in->msg, slot->len + 1);
    slot->enqueue_time = in->enqueue_time;

    q->tail++;
    if (q->tail == QUEUESIZE) q->tail = 0;
    if (q->tail == q->head) q->full = 1;
    q->empty = 0;

    /*
     * High-water mark. The CSV samples occupancy once a second, as the
     * assignment specifies, so it misses a burst that arrives and
     * drains in between; this does not. One compare per message, under
     * a lock the caller already holds.
     */
    used = queueCount(q);
    if (used > q->peak) q->peak = used;
}

void queueDel(queue *q, queue_entry *out) {
    const queue_entry *slot = &q->buf[q->head];

    out->len = slot->len;
    memcpy(out->msg, slot->msg, slot->len + 1);
    out->enqueue_time = slot->enqueue_time;

    q->head++;
    if (q->head == QUEUESIZE) q->head = 0;
    if (q->head == q->tail) q->empty = 1;
    q->full = 0;
}

/* Number of occupied slots. Caller holds q->mut. */
long queueCount(const queue *q) {
    if (q->full) return QUEUESIZE;
    if (q->empty) return 0;
    if (q->tail > q->head) return q->tail - q->head;
    return QUEUESIZE - q->head + q->tail;
}
