/* Unit test for classify().
 *
 * Includes consumer.c directly (not linked) so the harness links only
 * what classify() actually needs: no libwebsockets, no main().
 *
 * Build & run:   make unit
 *
 * Every check is a real assert(): a failure aborts with a file/line
 * number, and the whole thing runs in a few milliseconds.
 */
#include <assert.h>

#include "consumer.c"
/*
 * consumer.c's thread entry point refers to these; classify() does not.
 * Defining them here lets the harness link without main() or the rest
 * of the program.
 */
queue *g_fifo;
void   counters_count(int kind) { (void)kind; }

static int n_checks = 0;
static int n_diffs  = 0;

static const char *kindname(int k) {
    switch (k) {
        case K_COMMIT:   return "COMMIT";
        case K_IDENTITY: return "IDENTITY";
        case K_ACCOUNT:  return "ACCOUNT";
        case K_INFO:     return "INFO";
        case K_BADJSON:  return "BADJSON";
        default:         return "???";
    }
}

static void check(const char *js, int want, const char *name) {
    int got = classify(js, strlen(js));
    printf("  %-44s -> %-8s (want %-8s) %s\n", name, kindname(got), kindname(want),
           got == want ? "OK" : "FAIL");
    assert(got == want);
    n_checks++;
}

/* For cases where this implementation deliberately differs from the
   original harness. Reports instead of aborting. */
static void differs(const char *js, int theirs, int ours, const char *name,
                    const char *why) {
    int got = classify(js, strlen(js));
    printf("  %-44s -> %-8s (theirs: %-8s) DIFFERS\n", name, kindname(got),
           kindname(theirs));
    printf("      %s\n", why);
    assert(got == ours);
    n_diffs++;
}

int main(void) {
    printf("\n--- the three documented Jetstream kinds, well-formed ---\n");

    check("{\"did\":\"did:plc:x\",\"time_us\":1751500000000000,\"kind\":\"commit\","
        "\"commit\":{\"rev\":\"a\",\"operation\":\"create\","
          "\"collection\":\"app.bsky.feed.post\",\"record\":{\"text\":\"hello world\"}}}",
        K_COMMIT, "well-formed commit");

    check("{\"did\":\"did:plc:x\",\"time_us\":1,\"kind\":\"identity\","
        "\"identity\":{\"did\":\"did:plc:x\",\"handle\":\"a.bsky.social\"}}",
        K_IDENTITY, "well-formed identity");

    check("{\"did\":\"did:plc:x\",\"time_us\":1,\"kind\":\"account\","
        "\"account\":{\"active\":true,\"did\":\"did:plc:x\"}}",
        K_ACCOUNT, "well-formed account");

    printf("\n--- a delete commit has no \"record\" field at all ---\n");

    check("{\"did\":\"did:plc:x\",\"time_us\":1,\"kind\":\"commit\","
        "\"commit\":{\"rev\":\"a\",\"operation\":\"delete\","
        "\"collection\":\"app.bsky.feed.post\",\"rkey\":\"3l3d\"}}",
        K_COMMIT, "delete commit (no record field)");

    printf("\n--- unknown/system messages fall through to INFO ---\n");

    check("{\"kind\":\"error\",\"message\":\"oops\"}", K_INFO, "server error message");
    check("{\"no_kind_here\":true}",                   K_INFO, "missing kind field");
    check("{}",                                        K_INFO, "empty object");
    check("{\"kind\":\"info\",\"info\":{\"name\":\"OutdatedCursor\"}}",
          K_INFO, "literal kind=info");
    check("{\"kind\":42}",                             K_INFO, "kind present but not a string");
    check("{\"kind\":null}",                           K_INFO, "kind is null");

    printf("\n--- adversarial: a fake \"kind\" must not be picked up ---\n");

    check("{\"did\":\"did:plc:x\",\"time_us\":1,\"kind\":\"commit\","
          "\"commit\":{\"record\":{\"text\":\"lol \\\"kind\\\":\\\"identity\\\" haha\"}}}",
        K_COMMIT, "fake kind in post text");

    check("{\"did\":\"did:plc:x\",\"time_us\":1,\"kind\":\"account\","
        "\"account\":{\"active\":true},"
        "\"extra\":{\"kind\":\"commit\"}}",
        K_ACCOUNT, "fake kind in a nested object");

    /* Extra: the real kind appearing AFTER the decoy, so a naive
       first-match scan would also be wrong. */
    check("{\"commit\":{\"record\":{\"text\":\"\\\"kind\\\":\\\"identity\\\"\"}},"
        "\"kind\":\"commit\"}",
        K_COMMIT, "decoy before the real kind");

    printf("\n--- case sensitivity: \"Kind\" is not \"kind\" ---\n");
    check("{\"Kind\":\"commit\"}", K_INFO, "capitalised Kind is not the field");

    printf("\n--- truncated JSON ---\n");

    differs("{\"did\":\"did:plc:x\",\"time_us\":1751500000000000,\"kind\":\"commit\","
        "\"commit\":{\"record\":{\"text\":\"this got cut off mid-sente",
        K_COMMIT, K_BADJSON, "truncated mid-message",
        "No prefix-scan fallback here, by design: this pipeline never\n"
            "      enqueues truncated JSON. producer.c rejects an over-length\n"
            "      frame whole (counters_bump_oversize) rather than storing a\n"
            "      prefix, so a truncated frame can only mean genuine corruption,\n"
            "      which belongs in the badjson counter, not in a kind bucket.");

    differs("{\"did\":\"did:plc:x\",\"time_us\":1,\"kind\":\"identity\","
        "\"identity\":{\"handle\":\"trunc",
            K_IDENTITY, K_BADJSON, "truncated identity",
            "Same as above.");

    printf("\n--- malformed input must not crash ---\n");
    check("not json at all",  K_BADJSON, "plain text");
    check("",                 K_BADJSON, "empty string");
    check("{\"kind\":\"commit\"", K_BADJSON, "unclosed object");
    check("[1,2,3]",          K_INFO,    "top-level array (no kind field)");

    printf("\n--- large message (5000-element array) ---\n");
    {
        static char big[100000];
        int off = sprintf(big, "{\"kind\":\"commit\",\"a\":[");
        for (int i = 0; i < 5000; i++) off += sprintf(big + off, "%d,", i);
        sprintf(big + off - 1, "]}");
        check(big, K_COMMIT, "5000-element array");
    }

    /* classify() is called once per message for 24 hours: it must not
       leak. LSan checks that at exit. */
    printf("\n--- 200k repeat parses (leak/stability under sanitizers) ---\n");
    for (int i = 0; i < 200000; i++) {
        const char *m = "{\"did\":\"did:plc:x\",\"kind\":\"commit\","
            "\"commit\":{\"record\":{\"text\":\"x\"}}}";
        if (classify(m, strlen(m)) != K_COMMIT) {
            printf("  FAIL at %d\n", i);
            return 1;
        }
    }
    printf("  %-44s -> OK\n", "200000 iterations, stable");
    n_checks++;

    printf("\n%d checks passed, %d documented differences.\n\n", n_checks,
           n_diffs);
    return 0;
}
