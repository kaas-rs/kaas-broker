#!/usr/bin/env bash
# Test kafka-get-offsets.sh against kaas.
#
# Scenarios:
#   1. --time -2 (earliest) on a fresh topic = 0
#   2. --time -1 (latest)   on a fresh topic = 0
#   3. After producing N records, latest offset = N

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> Scenario 1: earliest offset is 0"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -2)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:0\$"

echo ">> Scenario 2: latest offset is 0"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:0\$"

echo ">> producing 7 records"
for i in $(seq 1 7); do echo "msg-$i"; done | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 3: latest offset is 7"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:7\$" || { echo "FAIL: expected 7" >&2; exit 1; }

echo ">> PASS"
