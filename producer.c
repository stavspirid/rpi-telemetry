/*
 * File    : producer.c
 *
 * Desc    : Thread 1. Asynchronous libwebsockets client. Every raw
 *           text frame that arrives is reassembled, timestamped and
 *           pushed into the circular buffer, then the callback
 *           returns to the network immediately.
 */

#include <libwebsockets.h>
#include <string.h>
#include <sys/time.h>

#include "telemetry.h"


static struct lws_context    *context;
static struct lws            *client_wsi;   // WebSocket instance
static lws_sorted_usec_list_t sul_connect;  // sorted list of scheduled callbacks
static uint16_t               retry_count;

/*
 * A single JSON message can arrive split across several CLIENT_RECEIVE calls
 * so frames are reassembled into this buffer before being enqueued
 */
static queue_entry asm_entry;
static size_t      asm_len;
static int         asm_overflow;

/* Exponential backoff: 1s, 2s, 4s, 8s, 16s, then 16s forever. */
static const uint32_t backoff_ms[] = {1000, 2000, 4000, 8000, 16000};

static const lws_retry_bo_t retry_policy = {
    .retry_ms_table       = backoff_ms,
    .retry_ms_table_count = LWS_ARRAY_SIZE(backoff_ms),
    .conceal_count        = LWS_RETRY_CONCEAL_ALWAYS,

    /*
     * No traffic for 30s -> send a PING. 
     * Still nothing 30s later -> treat the link as dead and reconnect
     */
    .secs_since_valid_ping   = 30,
    .secs_since_valid_hangup = 60,

    .jitter_percent = 20,   // Randomise the backoff +/-20%
};

/*
 * Enqueue one completed frame.
 *
 * Locking follows assignment 1: the caller takes fifo->mut, calls
 * queueAdd, unlocks, then signals. The difference is the full case.
 * There the producer waited on notFull; here it must not, because
 * this thread IS the libwebsockets event loop and blocking it would
 * stall the socket. We drop the frame and count it instead.
 */
static void enqueue_frame(queue *fifo) {
    int dropped = 0;

    pthread_mutex_lock(fifo->mut);

    if (fifo->full) {
        dropped = 1;
    } else {
        gettimeofday(&asm_entry.enqueue_time, NULL);
        asm_entry.len = asm_len;
        queueAdd(fifo, &asm_entry);
    }

    pthread_mutex_unlock(fifo->mut);

    if (dropped)
        counters_bump_drop();
    else
        pthread_cond_signal(fifo->notEmpty);
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
    i.pwsi                  = &client_wsi;
    i.retry_and_idle_policy = &retry_policy;

    if (!lws_client_connect_via_info(&i))
        lws_retry_sul_schedule(context, 0, sul, &retry_policy,
                               connect_client, &retry_count);
}

static int callback_jetstream(struct lws *wsi, enum lws_callback_reasons reason,
                              void *user, void *in, size_t len) {
    /* The queue is handed to lws as the context user pointer, so the
       callback does not need a global. */
    queue *fifo = (queue *)lws_context_user(lws_get_context(wsi));

    (void)user;

    switch (reason) {

        case LWS_CALLBACK_CLIENT_ESTABLISHED:
            lwsl_user("jetstream: connected\n");
            retry_count  = 0;
            asm_len      = 0;
            asm_overflow = 0;
            break;

        case LWS_CALLBACK_CLIENT_RECEIVE:
            if (lws_is_first_fragment(wsi)) {   // start of a new frame
                asm_len      = 0;
                asm_overflow = 0;
            }

            if (asm_len + len < SLOTSIZE) {
                memcpy(asm_entry.msg + asm_len, in, len);       // append to the reassembly buffer
                asm_len += len;
            } else {
                asm_overflow = 1;
            }

            if (lws_is_final_fragment(wsi)) {   
                if (asm_overflow) {
                    counters_bump_oversize();
                } else {
                    asm_entry.msg[asm_len] = '\0';
                    enqueue_frame(fifo);
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
    client_wsi   = NULL;
    asm_len      = 0;
    asm_overflow = 0;

    if (g_running)
        lws_retry_sul_schedule(context, 0, &sul_connect, &retry_policy,
                               connect_client, &retry_count);
    return 0;
}

static const struct lws_protocols protocols[] = {
    /* name, callback, per_session_data_size, rx_buffer_size, id, user,
       tx_packet_size. A 16 KB rx buffer keeps most Jetstream messages
       in a single fragment. */
    {"jetstream", callback_jetstream, 0, 16384, 0, NULL, 0},
    {NULL, NULL, 0, 0, 0, NULL, 0}};

void *producer(void *args) {
    thread_args *targs = (thread_args *)args;
    queue       *fifo  = targs->fifo;

    struct lws_context_creation_info info;

    lws_set_log_level(LLL_ERR | LLL_WARN | LLL_USER, NULL);

    memset(&info, 0, sizeof(info));
    info.options             = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
    info.port                = CONTEXT_PORT_NO_LISTEN;
    info.protocols           = protocols;
    info.fd_limit_per_thread = 8;
    info.user                = fifo;

    context = lws_create_context(&info);
    if (!context) {
        lwsl_err("producer: lws_create_context failed\n");
        g_running = 0;
        return NULL;
    }

    connect_client(&sul_connect);

    /* lws_service sleeps inside poll() until there is something to
       do, so this thread burns no CPU while the stream is quiet. */
    while (g_running && lws_service(context, 0) >= 0) {};

    lws_context_destroy(context);
    context = NULL;

    return NULL;
}

void producer_wake(void) {
    if (context) lws_cancel_service(context);
}
