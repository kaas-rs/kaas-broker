#!/usr/bin/env bash
# Test kafka-configs.sh against kaas.
#
# Scenarios:
#   1. --describe broker config (read path)
#   2. --describe topic config (read path)
#   3. --alter topic config, then assert --describe reports it as a
#      dynamic (non-default) config — the gh #236 round trip. Also
#      asserts an unknown key is rejected rather than silently accepted.
#   4. --describe --all (broad DescribeConfigs surface).
#   5. --describe specific broker by id.
#   6-9. Client quota admin via AlterClientQuotas / DescribeClientQuotas
#      (api keys 49 / 48, gh #260): alter + describe round trip, the
#      three supported keys, the documented rejection of
#      controller_mutation_rate, and the unsupported <default> entity.
#      Throttling itself is covered by bins/kaas/tests/auth_smoke.rs
#      (produce_exceeds_quota_returns_throttle) and the throttled-user
#      bench, not here: this script authenticates as kaas-scripts.

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1

echo ">> Scenario 1: --describe broker config"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-default --describe

echo ">> Scenario 2: --describe topic config for '$TOPIC'"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe

echo ">> Scenario 3: --alter topic config retention.ms, then round-trip via --describe"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" \
  --alter --add-config retention.ms=60000
# The change is CR-mediated (broker patch → operator reconcile → .config.json),
# so give the operator a moment before asserting the read side.
deadline=$((SECONDS + 30))
until "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
        --entity-type topics --entity-name "$TOPIC" --describe 2>&1 \
      | grep -q 'retention.ms=60000'; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    # gh #236: --describe (non---all) prints only non-default entries, so
    # this fails both when the alter was dropped and when the override is
    # misreported as DEFAULT_CONFIG.
    echo "FAIL: retention.ms=60000 not reported as a dynamic config within 30s (gh #236)" >&2
    exit 1
  fi
  sleep 1
done
echo "override round-tripped as a dynamic topic config"

echo ">> Scenario 3b: --alter with an unknown key must be rejected, not swallowed"
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type topics --entity-name "$TOPIC" \
     --alter --add-config max.message.bytes=1048576 2>&1; then
  echo "FAIL: unsupported key max.message.bytes was accepted — silent-drop regression (gh #236)" >&2
  exit 1
else
  echo "(expected) unsupported key rejected with an error"
fi

echo ">> Scenario 4: --describe with --all (every config key, not just overridden)"
# Exercises the broader DescribeConfigs surface — every key
# returned must have its source (DEFAULT_CONFIG, TOPIC_CONFIG, etc).
# Pre-#93 only overridden keys were returned; the broker config
# should now expose its full key set.
out=$("$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe --all 2>&1)
echo "$out" | head -20
echo "$out" | grep -q 'cleanup.policy' || { echo "FAIL: cleanup.policy not in --describe --all output" >&2; exit 1; }

echo ">> Scenario 5: --describe specific broker config"
# Per-broker (id=0) describe vs default. Required for tools like
# kafbat-ui that query individual brokers.
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type brokers --entity-name 0 --describe 2>&1 | head -5

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --delete --topic "$TOPIC" || true

# ---------------------------------------------------------------------------
# Client quota scenarios (gh #260). Overrides set by AlterClientQuotas live
# in the serving broker's memory, so the user name is per-run and the
# trap removes exactly the keys this script set, whatever happens.
# ---------------------------------------------------------------------------

QUOTA_USER="kaas-quota-test-$$"
# Values Java prints plainly (Double.toString switches to E-notation at
# 1e7, so 10485760 would describe as 1.048576E7).
QUOTA_PRODUCER_BPS=1048576
QUOTA_CONSUMER_BPS=2097152
QUOTA_KEYS_SET=""

quota_cleanup() {
  [ -n "$QUOTA_KEYS_SET" ] || return 0
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
    --entity-type users --entity-name "$QUOTA_USER" \
    --alter --delete-config "$QUOTA_KEYS_SET" >/dev/null 2>&1 || true
}
trap 'quota_cleanup; rm -rf "$TMP"' EXIT

describe_quota() {
  "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
    --entity-type users --entity-name "$QUOTA_USER" --describe 2>&1
}

echo ">> Scenario 6: --alter user quota (producer_byte_rate=$QUOTA_PRODUCER_BPS)"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-name "$QUOTA_USER" \
  --alter --add-config "producer_byte_rate=$QUOTA_PRODUCER_BPS"
QUOTA_KEYS_SET="producer_byte_rate"

echo ">> Scenario 7: --describe user quota round trip"
out="$(describe_quota)"
echo "$out"
echo "$out" | grep -Eq "producer_byte_rate=${QUOTA_PRODUCER_BPS}(\.0)?\b" \
  || { echo ">> FAIL: describe did not return the producer_byte_rate just set" >&2; exit 1; }

echo ">> Scenario 8: --alter the three supported keys; controller_mutation_rate is refused"
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-name "$QUOTA_USER" \
  --alter --add-config "consumer_byte_rate=$QUOTA_CONSUMER_BPS,request_percentage=50"
QUOTA_KEYS_SET="producer_byte_rate,consumer_byte_rate,request_percentage"
out="$(describe_quota)"
echo "$out"
for want in "producer_byte_rate=${QUOTA_PRODUCER_BPS}" "consumer_byte_rate=${QUOTA_CONSUMER_BPS}" "request_percentage=50"; do
  echo "$out" | grep -Eq "${want}(\.0)?\b" \
    || { echo ">> FAIL: describe is missing $want" >&2; exit 1; }
done
# Documented deviation (docs/src/compat/api/acls-quotas.md): kaas has no
# controller mutation quota, so the key answers INVALID_CONFIG rather than
# being accepted and silently ignored.
if "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
     --entity-type users --entity-name "$QUOTA_USER" \
     --alter --add-config "controller_mutation_rate=10" >/dev/null 2>&1; then
  echo ">> FAIL: controller_mutation_rate was accepted; kaas cannot enforce it" >&2
  exit 1
fi

echo ">> Scenario 9: --describe the <default> user entity (unsupported; empty answer)"
# kaas users are CR-instantiated and have no <default> entity; the
# documented answer is an empty success, not an error.
"$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type users --entity-default --describe

echo ">> PASS (config read/alter round trip + client quota admin round trip)"
