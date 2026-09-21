/*
 * File    : producer.c
 *
 * Desc    : Thread 1. Asynchronous libwebsockets client. Every raw text
 *           frame that arrives is reassembled, timestamped and pushed
 *           into the circular buffer, then the callback returns to the
 *           network immediately.
 *
 *           Also owns the libwebsockets context: stream_init() from
 *           main before any thread starts, stream_destroy() after they
 *           are all joined. Nothing else in the program includes
 *           libwebsockets.h.
 */

#include <libwebsockets.h>
#include <string.h>
#include <sys/time.h>

#include "telemetry.h"

unsigned long g_drops, g_oversize; /* this thread only, no lock needed */

static struct lws_context    *context;
static lws_sorted_usec_list_t sul_connect; /* scheduled reconnect */
static uint16_t               retry_count;

/* One JSON message can arrive split across several CLIENT_RECEIVE
   calls, so frames are reassembled here before being enqueued. */
static queue_entry asm_entry;
static size_t      asm_len;
static int         asm_overflow;

/* Exponential backoff: 1s, 2s, 4s, 8s, 16s, then 16s forever. */
static const uint32_t backoff_ms[] = {1000, 2000, 4000, 8000, 16000};

static const lws_retry_bo_t retry_policy = {
    .retry_ms_table       = backoff_ms,
    .retry_ms_table_count = LWS_ARRAY_SIZE(backoff_ms),
    .conceal_count        = LWS_RETRY_CONCEAL_ALWAYS,

    /* No traffic for 30s -> send a PING.
       Still nothing 30s later -> treat the link as dead and reconnect. */
    .secs_since_valid_ping   = 30,
    .secs_since_valid_hangup = 60,

    .jitter_percent = 20, /* randomise the backoff +/-20% */
};

/*
 * Enqueue one completed frame.
 *
 * Locking follows assignment 1: take fifo->mut, queueAdd, unlock, then
 * signal. The difference is the full case. There the producer waited on
 * notFull; here it must not, because this thread IS the libwebsockets
 * event loop and blocking it would stall the socket and lose frames at
 * the TCP level, where they cannot be counted. Drop and count instead.
 */
static void enqueue_frame(void) {
    int dropped = 0;

    pthread_mutex_lock(g_fifo->mut);

    if (g_fifo->full) {
        dropped = 1;
    } else {
        gettimeofday(&asm_entry.enqueue_time, NULL);
        asm_entry.len = asm_len;
        queueAdd(g_fifo, &asm_entry);
    }

    pthread_mutex_unlock(g_fifo->mut);

    if (dropped)
        g_drops++;
    else
        pthread_cond_signal(g_fifo->notEmpty);
}

static void connect_client(lws_sorted_usec_list_t *sul) {
    struct lws_client_connect_info i;

    memset(&i, 0, sizeof(i));

    i.context               = context;
    i.address               = JS_HOST;
    i.port                  = JS_PORT;
    i.path                  = JS_PATH;
    i.host                  = i.address;
    i.origin                = i.address;
    i.ssl_connection        = LCCSCF_USE_SSL;
    i.protocol              = "jetstream";
    i.local_protocol_name   = "jetstream";
    i.retry_and_idle_policy = &retry_policy;

    if (!lws_client_connect_via_info(&i))
        lws_retry_sul_schedule(context, 0, sul, &retry_policy, connect_client,
                               &retry_count);
}

static int callback_jetstream(struct lws *wsi, enum lws_callback_reasons reason,
                              void *user, void *in, size_t len) {
    (void)user;

    switch (reason) {
        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            lwsl_user("jetstream: connected\n");
            retry_count  = 0;
            asm_len      = 0;
            asm_overflow = 0;
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            if (lws_is_first_fragment(wsi)) {       // start of a new frame
                asm_len      = 0;
                asm_overflow = 0;
            }

            if (asm_len + len < SLOTSIZE) {
                memcpy(asm_entry.msg + asm_len, in, len);   // append to the reassembly buffer
                asm_len += len;
            } else {
                asm_overflow = 1;
            }

            if (lws_is_final_fragment(wsi)) {
                if (asm_overflow) {
                    g_oversize++;
                } else {
                    asm_entry.msg[asm_len] = '\0';
                    enqueue_frame();
                }
                asm_len      = 0;
                asm_overflow = 0;
            }
            break;

        case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
            lwsl_warn("jetstream: connect error: %s\n",
                      in ? (char *)in : "(none)");
            goto reconnect;

        case LWS_CALLBACK_CLIENT_CLOSED:
            lwsl_warn("jetstream: connection closed\n");
            goto reconnect;

        default:
            break;
    }

    return 0;

reconnect:
    asm_len      = 0;
    asm_overflow = 0;

    if (g_running)
        lws_retry_sul_schedule(context, 0, &sul_connect, &retry_policy,
                               connect_client, &retry_count);
    return 0;
}

static const struct lws_protocols protocols[] = {
    /* name, callback, per_session_data_size, rx_buffer_size, id, user,
       tx_packet_size. A 16 KB rx buffer keeps Jetstream messages in a
       single fragment. */
    {"jetstream", callback_jetstream, 0, 16384, 0, NULL, 0},
    {NULL, NULL, 0, 0, 0, NULL, 0}};

/* Called by main BEFORE any thread is created. */
int stream_init(void) {
    struct lws_context_creation_info info;

    lws_set_log_level(LLL_ERR | LLL_WARN | LLL_USER, NULL);

    memset(&info, 0, sizeof(info));
    info.options             = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.port                = CONTEXT_PORT_NO_LISTEN;
    info.protocols           = protocols;
    info.fd_limit_per_thread = 8;

    context = lws_create_context(&info);
    return context ? 0 : -1;
}

/* Wakes the event loop so the producer can see !g_running. Safe from
   any thread for as long as the three threads are alive. */
void stream_wake(void) { lws_cancel_service(context); }

/* Called by main AFTER every thread is joined. */
void stream_destroy(void) {
    lws_context_destroy(context);
    context = NULL;
}

void *producer(void *unused) {
    (void)unused;

    connect_client(&sul_connect);

    /*
     * lws_service sleeps inside poll() until there is something to do,
     * so this thread burns no CPU while the stream is quiet.
     *
     * A negative return is not a dropped connection -- the retry policy
     * handles those without ever coming back here -- it means the
     * context itself is gone. Fail loudly, or the monitor would keep
     * appending 0,0,0,0 rows for the rest of the day.
     */
    while (g_running) {
        if (lws_service(context, 0) < 0) {
            lwsl_err("producer: lws_service failed, aborting capture\n");
            g_failed  = 1;
            g_running = 0;
        }
    }

    return NULL;
}
