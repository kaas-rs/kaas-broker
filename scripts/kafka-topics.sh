#!/usr/bin/env bash
# Test kafka-topics.sh against kaas.
#
# Scenarios:
#   1. --create a topic with N partitions
#   2. --list and assert the topic appears
#   3. --describe and assert partition count
#   4. --delete and assert it's gone

. "$(dirname "$0")/_common.sh"

PARTITIONS=3

echo ">> Scenario 1: create topic '$TOPIC' with $PARTITIONS partitions"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> Scenario 2: --list contains '$TOPIC'"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"

echo ">> Scenario 3: --describe reports $PARTITIONS partitions"
desc=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
echo "$desc"
echo "$desc" | grep -qE "PartitionCount: ?$PARTITIONS"

echo ">> Scenario 4: --alter --partitions (CreatePartitions, gh #52)"
# Increase partition count from $PARTITIONS to $((PARTITIONS+2)).
# Apache requires the new count >= existing; we want to verify
# both the broker accepts the request AND the next --describe
# reflects the new count. gh #52 is open as status/gap so accept
# UnsupportedVersion / unimplemented gracefully — we surface the
# outcome but don't fail the script for that.
NEW_PARTS=$((PARTITIONS + 2))
if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
     --alter --topic "$TOPIC" --partitions "$NEW_PARTS" 2>"$TMP/alter.err"; then
  desc=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
  if echo "$desc" | grep -qE "PartitionCount: ?$NEW_PARTS"; then
    echo "CreatePartitions worked: now $NEW_PARTS partitions"
  else
    echo "FAIL: --alter --partitions accepted but partition count didn't change" >&2
    echo "$desc" >&2
    exit 1
  fi
else
  err=$(cat "$TMP/alter.err")
  case "$err" in
    *UNSUPPORTED_VERSION*|*UnsupportedVersion*|*unsupported*)
      echo "CreatePartitions not yet implemented (gh #52, status/gap) — OK to skip this scenario"
      ;;
    *)
      echo "FAIL: --alter --partitions failed with unexpected error:" >&2
      echo "$err" >&2
      exit 1
      ;;
  esac
fi

echo ">> Scenario 5: --delete and confirm"
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC"
sleep 2
if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$TOPIC"; then
  echo "FAIL: topic still listed after delete" >&2; exit 1
fi

echo ">> PASS"
