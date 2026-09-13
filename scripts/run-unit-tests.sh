#!/bin/sh
# Unit tests under AddressSanitizer + UBSan + LeakSanitizer.
# classify() runs once per message for 24 hours; a leak here is a leak there.
set -e
cd "$(dirname "$0")/.."

CC=${CC:-gcc}
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT

$CC -O1 -g -Wall -Wextra -std=gnu11 -D_GNU_SOURCE -pthread \
    -fsanitize=address,undefined -fno-omit-frame-pointer \
    tests/test_classify.c consumer.c queue.c stats.c \
    -o "$OUT/test_classify" -lcjson -lm

# consumer.c pulls in libwebsockets symbols only via telemetry.h decls,
# so no -lwebsockets is needed; producer.c is deliberately not linked.
LD_PRELOAD=$($CC -print-file-name=libasan.so) ASAN_OPTIONS=detect_leaks=1 \
    "$OUT/test_classify"
