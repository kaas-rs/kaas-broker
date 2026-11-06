#!/usr/bin/env bash
# Test kafka-log-dirs.sh against kaas.
#
# Scenarios:
#   1. --describe with no --topic-list returns all log dirs
#   2. --describe filtered to a specific topic returns only that topic

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> Scenario 1: --describe (all dirs)"
"$KAFKA_BIN/kafka-log-dirs.sh" --bootstrap-server "$BOOTSTRAP" --describe | tee "$TMP/all.json"
grep -q '"logDir"' "$TMP/all.json" || { echo "FAIL: no logDir in response" >&2; exit 1; }

echo ">> Scenario 2: --describe --topic-list $TOPIC"
out=$("$KAFKA_BIN/kafka-log-dirs.sh" --bootstrap-server "$BOOTSTRAP" \
  --describe --topic-list "$TOPIC")
echo "$out"
# Java tool flattens topic+partition into a single string field per
# partition: {"partition":"<topic>-<n>",...}. The topic name is never
# a standalone JSON field, so look for the embedded form.
echo "$out" | grep -q "\"partition\":\"$TOPIC-" || { echo "FAIL: topic missing" >&2; exit 1; }
# Filter sanity: when --topic-list is set, the response must NOT
# include partitions for unrelated topics (catches a future broker
# regression where the filter is ignored).
echo "$out" | grep -oE '"partition":"[^"]+"' | grep -v "\"partition\":\"$TOPIC-" \
  | grep -q . && { echo "FAIL: filter returned unrelated partitions" >&2; exit 1; }
true

echo ">> PASS"
