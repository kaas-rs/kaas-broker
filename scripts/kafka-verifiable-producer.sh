#!/usr/bin/env bash
# Test kafka-verifiable-producer.sh against kaas.
#
# Scenarios:
#   1. Produce 1000 records with acks=all, verify success line count.
#   2. Produce 500 records with enable.idempotence=true explicitly set
#      and assert the producer never logged an
#      OutOfOrderSequenceException / InvalidProducerEpoch — those are
#      the wire errors gh #12 stage B emits on bug, NOT on the happy
#      path. They go to STDERR (Java's logger), so we capture both
#      streams and grep.

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> Scenario 1: 1000 records, acks=all"
"$KAFKA_BIN/kafka-verifiable-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --max-messages 1000 --acks -1 | tee "$TMP/out.json"

echo ">> assert ack=success line count >= 1000"
ok=$(grep -c '"name":"producer_send_success"' "$TMP/out.json" || true)
[ "$ok" -ge 1000 ] || { echo "FAIL: only $ok success lines" >&2; exit 1; }

echo ">> Scenario 2: 500 records, idempotence explicit"
# enable.idempotence=true + acks=all + max.in.flight=5 is the Java 3.0+
# default; we set them explicitly so a future Java default change
# doesn't silently drop the test's coverage of stage-B's path.
"$KAFKA_BIN/kafka-verifiable-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --max-messages 500 --acks -1 \
  --producer.config <(cat <<EOF
enable.idempotence=true
max.in.flight.requests.per.connection=5
retries=2147483647
EOF
) >"$TMP/idem.out" 2>"$TMP/idem.err"

ok=$(grep -c '"name":"producer_send_success"' "$TMP/idem.out" || true)
[ "$ok" -ge 500 ] || { echo "FAIL: only $ok idempotent success lines" >&2; cat "$TMP/idem.err" >&2; exit 1; }

# Stage-B regression markers. If any of these slip through on a clean
# producer run, the broker is mis-tracking sequence state — exact
# scenario: a retry that was supposed to dedupe surfaced as a fatal.
if grep -qE 'OutOfOrderSequenceException|InvalidProducerEpoch|UnknownProducerIdException' "$TMP/idem.err"; then
  echo "FAIL: idempotent producer hit a stage-B fatal error:" >&2
  grep -E 'OutOfOrderSequenceException|InvalidProducerEpoch|UnknownProducerIdException' "$TMP/idem.err" >&2
  exit 1
fi

echo ">> PASS"
