#!/usr/bin/env bash
# Test kafka-delete-records.sh against kaas.
#
# Scenarios:
#   1. Produce 10 records, delete-records before offset 7, verify earliest=7

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> producing 10 records"
for i in $(seq 1 10); do echo "msg-$i"; done | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

cat >"$TMP/offsets.json" <<EOF
{ "partitions": [ { "topic": "$TOPIC", "partition": 0, "offset": 7 } ], "version": 1 }
EOF

echo ">> Scenario 1: delete-records --offset-json-file (offset=7)"
"$KAFKA_BIN/kafka-delete-records.sh" --bootstrap-server "$BOOTSTRAP" \
  --offset-json-file "$TMP/offsets.json"

echo ">> verifying: earliest offset == 7"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -2)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:7\$" || { echo "FAIL: earliest != 7" >&2; exit 1; }

echo ">> PASS"
