#ifndef QUEUE_H
#define QUEUE_H

#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>

/* Tunable constants */
#define QUEUESIZE 256  /* circular buffer capacity, in messages */
#define SLOTSIZE 16384 /* bytes per slot, incl. NUL terminator */

/*
 * Queue entry: one raw JSON frame + enqueue timestamp.
 *
 * Assignment 1 carried a workFunction here. The work is fixed now
 * (parse the frame, classify its "kind"), so the payload is the
 * message itself. Slots are fixed size, so the producer never calls
 * malloc on the network path.
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
    long             peak; /* high-water mark, in occupied slots */
    int              full, empty;
    pthread_mutex_t *mut;
    pthread_cond_t  *notFull, *notEmpty;
} queue;

/*
 * As in assignment 1, these do NOT lock. The caller holds q->mut across
 * the call and signals after releasing it. queueAdd takes a pointer
 * because queue_entry is ~16 KB and passing by value would cost two
 * extra copies per message.
 */
queue *queueInit(void);
void   queueDelete(queue *q);
void   queueAdd(queue *q, const queue_entry *in);
void   queueDel(queue *q, queue_entry *out);
long   queueCount(const queue *q);

#endif /* QUEUE_H */
