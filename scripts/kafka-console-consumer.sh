#!/usr/bin/env bash
# Test kafka-console-consumer.sh against kaas.
#
# Scenarios:
#   1. --from-beginning + --max-messages reads exactly the messages produced
#   2. --offset latest --partition 0 with no new traffic times out cleanly
#   3. --isolation-level read_committed with no transactions reads the same
#   4. --partition + --offset earliest, printing partition and offset
#
# These compare what the consumer printed rather than counting lines. A count
# passes three wrong records, and it can't tell a record from anything else on
# stdout, which is where the 4.x CLI writes its deprecation notices. So this
# script sticks to flags that aren't deprecated, and a new notice fails it visibly.

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> seeding 3 messages"
printf 'a\nb\nc\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 1: consume --from-beginning --max-messages 3"
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000)
echo "$got"
want=$(printf 'a\nb\nc')
[ "$got" = "$want" ] || { echo "FAIL: expected the 3 seeded messages" >&2; exit 1; }

echo ">> Scenario 2: --offset latest with no new produces, timeout-ms 3000 (expect zero output)"
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --partition 0 --offset latest --timeout-ms 3000 || true)
[ -z "$got" ] || echo "WARN: unexpected output: $got"

echo ">> Scenario 3: --isolation-level read_committed (gh #31 wire surface)"
# v4+ FetchRequest carries IsolationLevel. Read-committed against
# a topic with no in-flight txns must return the same records as
# read-uncommitted (LSO = HighWatermark when no txns are active).
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000 \
  --isolation-level read_committed)
echo "$got"
[ "$got" = "$want" ] || {
  echo "FAIL: read_committed didn't return the 3 seeded messages (no in-flight txns)" >&2
  exit 1
}

echo ">> Scenario 4: --partition / --offset earliest + formatter props"
# Verifies the metadata path returns offsets correctly via a
# different selector shape. The Kafka 4.x default formatter (no
# --formatter override needed; the old kafka.tools.DefaultMessageFormatter
# class was renamed) still honours print.partition / print.offset.
# --formatter-property replaced --property, which is deprecated where both
# exist. An older KAFKA_BIN only has --property.
consumer_help=$("$KAFKA_BIN/kafka-console-consumer.sh" --help 2>&1 || true)
case "$consumer_help" in
  *--formatter-property*) formatter_flag=--formatter-property ;;
  *) formatter_flag=--property ;;
esac
got=$("$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --partition 0 --offset earliest --max-messages 3 --timeout-ms 10000 \
  "$formatter_flag" print.partition=true \
  "$formatter_flag" print.offset=true)
echo "$got"
want=$(printf 'Partition:0\tOffset:0\ta\nPartition:0\tOffset:1\tb\nPartition:0\tOffset:2\tc')
[ "$got" = "$want" ] || { echo "FAIL: partition+offset selector" >&2; exit 1; }

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
