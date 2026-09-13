#ifndef QUEUE_H
#define QUEUE_H

#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdlib.h>

/* Tunable constants */
#define QUEUESIZE 2048  /* circular buffer capacity, in messages */
#define SLOTSIZE  8192  /* bytes per slot, incl. NUL terminator */

/*
 * Queue entry: one raw JSON frame + enqueue timestamp.
 *
 * Assignment 1 carried a workFunction here. The work is fixed now
 * (parse the frame, classify its "kind"), so the payload is the
 * message itself. Slots are fixed size, which means the producer
 * never calls malloc on the network path: nothing to leak, nothing
 * to fragment over 24 hours.
 */
typedef struct {
    char           msg[SLOTSIZE]; /* raw JSON text, NUL-terminated */
    size_t         len;           /* bytes in msg, excluding the NUL */
    struct timeval enqueue_time;  /* for the wait-time statistics */
} queue_entry;

/* FIFO queue (circular buffer, mutex + two condition variables) */
typedef struct {
    queue_entry      buf[QUEUESIZE];
    long             head, tail;
    int              full, empty;
    pthread_mutex_t *mut;
    pthread_cond_t  *notFull, *notEmpty;
} queue;

/*
 * As in assignment 1, queueAdd/queueDel/queueCount do NOT lock.
 * The caller holds q->mut across the call and signals after
 * releasing it.
 *
 * queueAdd takes a pointer rather than a value: queue_entry is now
 * ~8 KB, so passing by value would cost two extra copies per message.
 */
queue *queueInit(void);
void   queueDelete(queue *q);
void   queueAdd(queue *q, const queue_entry *in);
void   queueDel(queue *q, queue_entry *out);
long   queueCount(const queue *q);

#endif /* QUEUE_H */
