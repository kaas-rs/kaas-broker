#!/usr/bin/env bash
# Test kafka-e2e-latency.sh against kaas.
#
# Scenarios:
#   1. 1000 round-trips, 100-byte payloads, acks=1

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

# Named arguments where the tool has them: the positional form is deprecated and
# goes in Kafka 5.0, but it's all 3.7 has. newer_flag can't tell which this is,
# because the tool answers a lone --help with a positional-argument error. It only
# lists its flags when called with no arguments.
usage="$("$KAFKA_BIN/kafka-e2e-latency.sh" 2>&1 || true)"
case "$usage" in
  *--bootstrap-server*)
    latency_args=(--bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --num-records 1000
      --producer-acks 1 --record-size 100) ;;
  *)
    latency_args=("$BOOTSTRAP" "$TOPIC" 1000 1 100) ;;
esac

echo ">> Scenario 1: 1000 round-trips, 100B"
"$KAFKA_BIN/kafka-e2e-latency.sh" "${latency_args[@]}"

echo ">> PASS"
