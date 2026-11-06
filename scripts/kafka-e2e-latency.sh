#!/usr/bin/env bash
# Test kafka-e2e-latency.sh against kaas.
#
# Scenarios:
#   1. 1000 round-trips, 100-byte payloads, acks=1

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> Scenario 1: 1000 round-trips, 100B"
"$KAFKA_BIN/kafka-e2e-latency.sh" \
  "$BOOTSTRAP" "$TOPIC" 1000 1 100

echo ">> PASS"
