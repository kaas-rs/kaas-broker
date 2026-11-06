#!/usr/bin/env bash
# Probes the transactional-coordinator wire surface that landed across
# gh #22 (rejoin epoch), gh #23-#28 (state machine + timeout reaper),
# gh #91 (TxnOwnership routing), and gh #29's architectural answer
# (slot files instead of __transaction_state).
#
# All scenarios use Apache shell tools — kafka-broker-api-versions,
# kafka-transactions — so the test is meaningful even though
# kafka-verifiable-producer in 4.x dropped --transactional-id (we
# can't drive a full txn from the shell anymore; see the comment in
# kafka-transactions.sh).

. "$(dirname "$0")/_common.sh"

echo ">> Scenario 1: ApiVersions advertises the transactional API set"
"$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" \
  > "$TMP/api-versions.out" 2>&1

# Required APIs for a working EOS-v2 client:
#   10  FindCoordinator        — KeyType=transaction routing (gh #91)
#   22  InitProducerId         — PID + epoch handout (gh #12, #22)
#   24  AddPartitionsToTxn     — gh #23
#   25  AddOffsetsToTxn        — gh #24
#   26  EndTxn                 — gh #25, #26
#   28  TxnOffsetCommit        — gh #27
required_apis=(10 22 24 25 26 28)
missing=()
for k in "${required_apis[@]}"; do
  if ! grep -qE "^[[:space:]]*(${k}|[A-Za-z_]+\(${k}\))[[:space:]]*:" "$TMP/api-versions.out"; then
    missing+=("$k")
  fi
done
if [ ${#missing[@]} -ne 0 ]; then
  echo "FAIL: missing transactional API key(s) in ApiVersions: ${missing[*]}" >&2
  echo "--- first 40 lines of api-versions output ---" >&2
  head -40 "$TMP/api-versions.out" >&2
  exit 1
fi
echo "   transactional APIs advertised: ${required_apis[*]}"

echo ">> Scenario 2: kafka-transactions.sh list (gh #114 — expect UNSUPPORTED_VERSION until ListTransactions lands)"
if "$KAFKA_BIN/kafka-transactions.sh" --bootstrap-server "$BOOTSTRAP" \
     list 2>"$TMP/list.err"; then
  echo "   ListTransactions implemented — parity win, update kafka-transactions.sh to assert result shape"
else
  err=$(cat "$TMP/list.err")
  case "$err" in
    *UNSUPPORTED_VERSION*|*UnsupportedVersion*|*unsupported*|*"does not support"*|*"is not supported"*)
      echo "   ListTransactions not yet wired (API 66) — gh #114 territory, known gap"
      ;;
    *)
      echo "FAIL: list failed with unexpected error:" >&2
      echo "$err" >&2
      exit 1
      ;;
  esac
fi

echo ">> Scenario 3: kafka-transactions.sh describe-producers (DescribeProducers API 61, KIP-664)"
# Pick any topic — describe-producers is partition-scoped but only
# needs the topic to exist. Create a throwaway one.
"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
delete_on_exit "$TOPIC"

if "$KAFKA_BIN/kafka-transactions.sh" --bootstrap-server "$BOOTSTRAP" \
     describe-producers --topic "$TOPIC" --partition 0 \
     >"$TMP/desc.out" 2>"$TMP/desc.err"; then
  # Apache returns a header line even when no producers are active.
  echo "   DescribeProducers wired — output:"
  sed 's/^/      /' "$TMP/desc.out"
else
  err=$(cat "$TMP/desc.err")
  case "$err" in
    *UNSUPPORTED_VERSION*|*UnsupportedVersion*|*unsupported*|*"does not support"*|*"is not supported"*)
      echo "   DescribeProducers not yet wired (API 61) — known gap"
      ;;
    *)
      echo "FAIL: describe-producers failed with unexpected error:" >&2
      echo "$err" >&2
      exit 1
      ;;
  esac
fi

# Tidy up: don't leave the throwaway topic behind on a shared bench
# cluster. delete is best-effort; the operator's reconciler cleans up
# storage either way.
echo ">> PASS (txn coordinator wire surface intact for KIP-447 same-broker EOS-v2)"
