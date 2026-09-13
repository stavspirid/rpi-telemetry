CC      = gcc
CFLAGS  = -O2 -Wall -Wextra -std=gnu11 -D_GNU_SOURCE -pthread
LDFLAGS = -pthread
LDLIBS  = -lwebsockets -lcjson -lm

TARGET  = telemetry
SRCS    = telemetry.c queue.c stats.c producer.c consumer.c monitor.c
OBJS    = $(SRCS:.c=.o)
DEPS    = telemetry.h queue.h

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ $(LDLIBS)

%.o: %.c $(DEPS)
	$(CC) $(CFLAGS) -c -o $@ $<

# Short run to sanity-check the CSV before committing to 24 hours.
test: $(TARGET)
	sudo ./$(TARGET) -o /tmp/test_log.txt -d 60
	@echo "--- first 5 lines ---"
	@head -5 /tmp/test_log.txt

# Race detector. Run for a few minutes before the real capture.
tsan: CFLAGS  = -O1 -g -Wall -Wextra -std=gnu11 -D_GNU_SOURCE -pthread -fsanitize=thread
tsan: LDFLAGS = -pthread -fsanitize=thread
tsan: clean $(TARGET)

# Leak check. Expect "definitely lost: 0 bytes".
memcheck: $(TARGET)
	valgrind --leak-check=full --show-leak-kinds=definite \
	         ./$(TARGET) -o /tmp/vg_log.txt -d 30

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all test tsan memcheck clean
