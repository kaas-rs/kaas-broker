#!/usr/bin/env bash
# Test kafka-console-consumer.sh against kaas.
#
# Scenarios:
#   1. --from-beginning + --max-messages reads exactly the messages produced
#   2. --offset latest with no new traffic prints nothing
#   3. --isolation-level read_committed with no transactions reads the same
#   4. --partition + --offset earliest, printing partition and offset
#   5. --isolation-level read_committed after real transactions
#
# These compare what the consumer printed rather than counting lines. A count
# passes three wrong records, and it can't tell a record from anything else on
# stdout, which is where the 4.x CLI writes its deprecation notices. So this
# script sticks to flags that aren't deprecated, and a new notice fails it visibly.

. "$(dirname "$0")/_common.sh"

TXN_TOPIC="$TOPIC-txn"
TXN_RECORDS=2000

# Combined cleanup: _common.sh sets its own EXIT trap for $TMP; this replaces it
# with a superset, so a failed run doesn't leave its topics behind.
cleanup() {
  local rc=$?
  set +e
  for t in "$TOPIC" "$TXN_TOPIC"; do
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
      --delete --topic "$t" >/dev/null 2>&1
  done
  rm -rf "$TMP"
  exit "$rc"
}
trap cleanup EXIT

# The consumer with its records on stdout, and everything else (its banner, the
# timeout it ends on) kept for the failure report. A timeout exits 0, so a
# non-zero exit is a real failure.
#
# An unreachable broker also exits 0 with no output, so an empty result only
# means something once a scenario has read records from this bootstrap.
consume() {
  if ! "$KAFKA_BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP" "$@" \
      2>"$TMP/consumer.err"; then
    echo "FAIL: the consumer exited non-zero: $*" >&2
    cat "$TMP/consumer.err" >&2
    return 1
  fi
}

fail() {
  echo "FAIL: $1" >&2
  echo "--- consumer stderr:" >&2
  cat "$TMP/consumer.err" >&2 2>/dev/null || true
  exit 1
}

# expect <what's being checked> <want> <got>
expect() {
  [ "$3" = "$2" ] && return 0
  printf -- '--- want:\n%s\n--- got:\n%s\n' "$2" "$3" >&2
  fail "$1"
}

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> seeding 3 messages"
printf 'a\nb\nc\n' | \
  "$KAFKA_BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"
seeded=$(printf 'a\nb\nc')

echo ">> Scenario 1: consume --from-beginning --max-messages 3"
got=$(consume --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000)
expect "--from-beginning didn't return the 3 seeded messages" "$seeded" "$got"

echo ">> Scenario 2: --offset latest with no new produces, timeout-ms 3000 (expect zero output)"
got=$(consume --topic "$TOPIC" --partition 0 --offset latest --timeout-ms 3000)
expect "--offset latest returned records with none produced since" "" "$got"

echo ">> Scenario 3: --isolation-level read_committed (gh #31 wire surface)"
# v4+ FetchRequest carries IsolationLevel. Read-committed against
# a topic with no in-flight txns must return the same records as
# read-uncommitted (LSO = HighWatermark when no txns are active).
got=$(consume --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000 \
  --isolation-level read_committed)
expect "read_committed didn't return the 3 seeded messages (no in-flight txns)" "$seeded" "$got"

echo ">> Scenario 4: --partition / --offset earliest + formatter props"
# Verifies the metadata path returns offsets correctly via a
# different selector shape. The Kafka 4.x default formatter (no
# --formatter override needed; the old kafka.tools.DefaultMessageFormatter
# class was renamed) still honours print.partition / print.offset.
# --formatter-property replaced --property, which is deprecated where both
# exist. An older KAFKA_BIN only has --property. --help exits non-zero.
consumer_help=$("$KAFKA_BIN/kafka-console-consumer.sh" --help 2>&1 || true)
case "$consumer_help" in
  *--formatter-property*) formatter_flag=--formatter-property ;;
  *) formatter_flag=--property ;;
esac
got=$(consume --topic "$TOPIC" --partition 0 --offset earliest --max-messages 3 --timeout-ms 10000 \
  "$formatter_flag" print.partition=true \
  "$formatter_flag" print.offset=true)
expect "partition+offset selector" \
  "$(printf 'Partition:0\tOffset:0\ta\nPartition:0\tOffset:1\tb\nPartition:0\tOffset:2\tc')" "$got"

echo ">> Scenario 5: read_committed after real transactions (gh #299)"
# Scenario 3 has no transactions, so it would pass with the isolation level
# ignored. This is a real Java transactional producer: verification mustn't
# refuse it (gh #299), and every commit has to release the last stable offset, or
# read_committed stops short. The CLI can't abort or hold a transaction open, so
# hiding aborted records is left to bins/kaas/tests/eos_v2.rs.
#
# The transactional id is fixed so runs don't add an entry each to the txn state.
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TXN_TOPIC" --partitions 1 --replication-factor 1
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TXN_TOPIC" \
  --num-records "$TXN_RECORDS" --record-size 16 --throughput -1 \
  --transactional-id kaas-test-console-consumer --transaction-duration-ms 20 \
  --producer-props bootstrap.servers="$BOOTSTRAP" | tail -1

# Commit markers take offsets. Without any, the producer wasn't transactional and
# the count below would prove nothing.
log_end=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TXN_TOPIC" --time -1)
log_end=${log_end##*:}
echo "log end $log_end for $TXN_RECORDS records"
[ "$log_end" -gt "$TXN_RECORDS" ] || fail "no commit markers in the log: no transaction ran"

# The payloads are random, so this one has to count. No --max-messages, so a
# duplicate or a leaked marker can't hide behind the limit.
consume --topic "$TXN_TOPIC" --from-beginning --isolation-level read_committed \
  --timeout-ms 5000 > "$TMP/txn.out"
expect "read_committed after real transactions" "$TXN_RECORDS" "$(( $(wc -l < "$TMP/txn.out") ))"

echo ">> PASS"
