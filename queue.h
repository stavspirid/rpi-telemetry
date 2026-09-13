#ifndef QUEUE_H
#define QUEUE_H

#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdlib.h>

/*
 * Tunable constants
 *
 * QUEUESIZE is deliberately modest. Jetstream filtered to
 * app.bsky.feed.post delivers ~50 msg/s, and the consumer turns a
 * frame around in 60-130 us, so the ring is empty almost all of the
 * time. 256 slots is still ~5 s of absorption at the nominal rate
 * (and ~1 s at a 4x burst), while making one slot worth 0.39% instead
 * of 0.05%: the Buffer_Occupancy_Pct column can actually resolve a
 * burst. It also drops the resident footprint from 16 MB to ~4 MB,
 * which matters because the whole thing is mlockall'd on a 512 MB Pi.
 *
 * SLOTSIZE is 16 KB because 8 KB was marginal: probing the live
 * stream showed a maximum frame of 6984 B, but frames above 8 KB do
 * occur and were being discarded as oversize.
 */
#define QUEUESIZE 256   /* circular buffer capacity, in messages */
#define SLOTSIZE  16384 /* bytes per slot, incl. NUL terminator */

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
    long             peak; /* high-water mark, in occupied slots */
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
 * ~16 KB, so passing by value would cost two extra copies per message.
 */
queue *queueInit(void);
void   queueDelete(queue *q);
void   queueAdd(queue *q, const queue_entry *in);
void   queueDel(queue *q, queue_entry *out);
long   queueCount(const queue *q);

/*
 * High-water mark since startup. The 1 Hz Buffer_Occupancy_Pct column
 * is an instantaneous sample, as the assignment specifies, so it
 * almost always reads 0 and cannot show a burst that started and
 * drained between two samples. This is updated on every enqueue, so
 * it catches those; it is reported at shutdown, not in the CSV.
 */
long   queuePeak(const queue *q);

#endif /* QUEUE_H */
