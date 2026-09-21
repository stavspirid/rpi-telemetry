CC      = gcc
CFLAGS  = -O2 -Wall -Wextra -std=gnu11 -D_GNU_SOURCE -pthread
LDFLAGS = -pthread -lwebsockets -lcjson -lm
TARGET  = telemetry
SRC     = telemetry.c producer.c consumer.c monitor.c queue.c
DEPS    = telemetry.h queue.h

.PHONY: all unit test tsan memcheck clean

all: $(TARGET)

$(TARGET): $(SRC) $(DEPS)
	$(CC) $(CFLAGS) -o $@ $(SRC) $(LDFLAGS)

# Unit tests for classify(), under ASan/UBSan/LSan. Milliseconds, so run
# it after every change to consumer.c. The harness includes consumer.c,
# so it needs neither libwebsockets nor the rest of the program.
unit:
	$(CC) -O1 -g -Wall -Wextra -std=gnu11 -D_GNU_SOURCE -pthread -I. \
	      -fsanitize=address,undefined -fno-omit-frame-pointer \
	      tests/test_classify.c queue.c -o /tmp/test_classify -lcjson -lm -latomic
	@LD_PRELOAD=$$($(CC) -print-file-name=libasan.so) /tmp/test_classify

# Short live run to sanity-check the CSV before committing to 24 hours.
test: $(TARGET)
	./$(TARGET) -o /tmp/test_log.txt -d 60
	./scripts/check-realtime.py /tmp/test_log.txt

# Race detector. Run for a few minutes before the real capture.
# Not available on the Pi Zero W: TSan has no 32-bit ARM support.
tsan: $(SRC) $(DEPS)
	$(CC) -O1 -g -Wall -Wextra -std=gnu11 -D_GNU_SOURCE -pthread \
	      -fsanitize=thread -o $(TARGET) $(SRC) $(LDFLAGS)

# Leak check. Expect "definitely lost: 0 bytes".
memcheck: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=definite \
	         ./$(TARGET) -o /tmp/vg_log.txt -d 30

clean:
	rm -f $(TARGET)
