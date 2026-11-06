#!/usr/bin/env bash
# Test kafka-verifiable-consumer.sh against kaas.
#
# Scenarios:
#   1. Seed 1000 records; consumer reports records_consumed >= 1000

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> seeding 1000 records"
"$KAFKA_BIN/kafka-verifiable-producer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --max-messages 1000 --acks -1 >/dev/null

echo ">> Scenario 1: consume 1000 records"
"$KAFKA_BIN/kafka-verifiable-consumer.sh" \
  --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
  --group-id "verif-cg-$$" --max-messages 1000 --reset-policy earliest \
  | tee "$TMP/out.json"

# A clean run prints "shutdown_complete" — this is the consumer's
# explicit "I finished and closed cleanly" signal.
grep -q '"shutdown_complete"' "$TMP/out.json" \
  || { echo "FAIL: consumer did not reach shutdown_complete" >&2; exit 1; }

# Sum top-level "count" field of records_consumed events. The event
# format is {"name":"records_consumed","count":N,"partitions":[{"count":...}]}
# — the per-partition "count" is inside partitions[] AFTER the top-level
# one, so this match-and-extract picks the right one. Single awk so a
# missed match doesn't trip pipefail+set-e (the original bug).
count=$(awk '
  match($0, /"name":"records_consumed","count":[0-9]+/) {
    n = substr($0, RSTART, RLENGTH); sub(/.*:/, "", n); s += n
  }
  END { print s+0 }
' "$TMP/out.json")
echo "records_consumed total: $count"
[ "$count" -ge 1000 ] || { echo "FAIL: consumed $count < 1000" >&2; exit 1; }

echo ">> PASS"
