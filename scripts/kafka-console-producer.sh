#!/usr/bin/env bash
# Test kafka-console-producer.sh against kaas.
#
# Scenarios:
#   1. Produce a few lines to a fresh topic
#   2. Produce keyed records, with parse.key=true and a key separator

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

reader_flag=$(newer_flag kafka-console-producer.sh --reader-property --property)
producer_flag=$(newer_flag kafka-console-producer.sh --command-property --producer-property)

echo ">> Scenario 1: produce 3 plain lines"
printf 'one\ntwo\nthree\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 2: produce keyed records (k:v)"
printf 'k1:v1\nk2:v2\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    "$reader_flag" parse.key=true "$reader_flag" key.separator=:

echo ">> verifying with kafka-get-offsets.sh: high-watermark should be 5"
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out"
echo "$out" | grep -qE "${TOPIC}:0:5\$" || { echo "FAIL: end-offset != 5" >&2; exit 1; }

echo ">> Scenario 3: idempotent producer (acks=all, retries default)"
# Java client defaults to enable.idempotence=true since 3.0; this
# scenario exercises the gh #12 InitProducerId + sequence-tracking
# path. We set it explicitly so a future Kafka CLI default change
# doesn't silently mask the test.
printf 'idem-1\nidem-2\nidem-3\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
    "$producer_flag" enable.idempotence=true \
    "$producer_flag" acks=all
out=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
echo "$out" | grep -qE "${TOPIC}:0:8\$" || { echo "FAIL: end-offset != 8 after idempotent batch" >&2; exit 1; }

# Note: Apache's `kafka-console-producer.sh` cannot drive a
# transactional producer end-to-end. The CLI never calls
# `initTransactions()` / `beginTransaction()` / `commitTransaction()`
# even when `transactional.id` is set — `send()` throws
# IllegalStateException from the Java client's state machine
# before any wire request reaches the broker. The gh #22-#27 chain
# is exercised at the wire level by `kafka-txn-coordinator.sh` and
# end-to-end by the broker integration test `bins/kaas/tests/eos_v2.rs`.

echo ">> PASS"
