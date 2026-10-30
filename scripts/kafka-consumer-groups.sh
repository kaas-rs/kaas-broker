#!/usr/bin/env bash
# Test kafka-consumer-groups.sh against kaas.
#
# Scenarios:
#   1. --list (initially the test group is absent)
#   2. Produce + consume with --group <id> to create the group
#   3. --describe reads the committed offsets back (every partition at
#      its log end)
#   4. --reset-offsets --to-earliest / --to-latest / --shift-by -1
#      --execute, the last checked through --describe
#   5. --delete (gh #89): the group must actually disappear from
#      --list afterwards.

. "$(dirname "$0")/_common.sh"

GROUP="kaas-test-cg-$$"

# Every committed partition of $TOPIC as "partition current log-end".
# The group has no active members by then, and Kafka 4 fetches its
# offsets with the all-topics OffsetFetch form — which kaas answered
# with nothing until it learned to enumerate them.
committed() {
  "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
    --describe --group "$GROUP" 2>/dev/null | awk -v t="$TOPIC" '$2 == t {print $3, $4, $5}'
}

# expect_committed <shift>: every partition sits at max(log-end + shift, 0).
expect_committed() {
  local rows
  rows=$(committed)
  echo "$rows"
  [ -n "$rows" ] || { echo "FAIL: --describe shows no committed offsets" >&2; exit 1; }
  awk -v s="$1" '{
    want = $3 + s; if (want < 0) want = 0
    if ($2 != want) { print "FAIL: partition " $1 " at " $2 ", want " want; bad = 1 }
  } END { exit bad }' <<<"$rows" >&2 || exit 1
}

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 2 --replication-factor 1

echo ">> seeding 5 messages"
printf 'a\nb\nc\nd\ne\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"

echo ">> Scenario 1: --list (group should NOT exist yet)"
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --list

echo ">> Scenario 2: consume into group '$GROUP'"
"$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --group "$GROUP" --from-beginning \
  --max-messages 5 --timeout-ms 10000 >/dev/null

echo ">> Scenario 3: --describe reads every committed offset back"
expect_committed 0

echo ">> Scenario 4: --reset-offsets --to-earliest --execute"
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --reset-offsets --to-earliest --topic "$TOPIC" --group "$GROUP" --execute

echo ">> Scenario 4b: --reset-offsets --to-latest --execute"
# Exercises the OffsetCommit path via the latest-offset selector.
# Pre-#92 hash routing this could mismatch coordinator → group
# lookups; today the --execute must return rc=0 and print the
# NEW-OFFSET table.
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --reset-offsets --to-latest --topic "$TOPIC" --group "$GROUP" --execute

echo ">> Scenario 4c: --reset-offsets --shift-by -1 --execute"
# Shifts from the committed offsets --to-latest just set, so it needs
# the same all-topics OffsetFetch as --describe.
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --reset-offsets --shift-by -1 --topic "$TOPIC" --group "$GROUP" --execute
expect_committed -1

echo ">> Scenario 5: --delete must actually remove the group"
# Pre-#89 this returned UnsupportedVersionException and we
# tolerated it via "|| true". Now the broker advertises key 42
# and the call must succeed AND the group must vanish from
# subsequent --list output. Capture stderr so a regression
# (e.g. NON_EMPTY_GROUP if the consumer state leaks) is loud.
"$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" \
  --delete --group "$GROUP" 2>"$TMP/del.err" \
  || { echo "FAIL: --delete failed"; cat "$TMP/del.err" >&2; exit 1; }

if "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --list \
    | grep -qx "$GROUP"; then
  echo "FAIL: deleted group still appears in --list" >&2
  exit 1
fi

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

echo ">> PASS"
