#!/usr/bin/env bash
# Verify that an idempotent producer survives a broker restart
# mid-flight. This is the end-to-end check on gh #12 stage B2:
# without snapshot-on-disk persistence of the producerStates map,
# the post-restart broker would see an in-flight retry as
# "fresh PID, non-zero firstSeq" and reject it with
# OUT_OF_ORDER_SEQUENCE_NUMBER (45), killing the producer.
#
# Strategy:
#   1. Start a 50k-record idempotent run in the background using
#      kafka-producer-perf-test.
#   2. After ~1s (enough for some batches to land), restart the
#      StatefulSet pod that owns partition 0 with kubectl. Pick the
#      broker by reading the live assignment via /healthz on a peer.
#   3. Wait for the producer to finish — its exit code is the
#      pass/fail signal: 0 means it tolerated the restart cleanly,
#      non-zero means a fatal error (likely the stage-B-without-B2
#      OutOfOrderSequenceException).
#   4. Cross-check by reading the partition's high watermark — should
#      be exactly 50000 (no losses, no duplicates).
#
# Required env: kubectl with access to the kaas namespace. Skips
# cleanly when kubectl is missing or returns no pods.

. "$(dirname "$0")/_common.sh"

need kubectl

RECORDS="${RECORDS:-50000}"

# Sanity: we need at least one StatefulSet pod in the target ns.
if ! kubectl -n "$NAMESPACE" get sts "$STS" >/dev/null 2>&1; then
  skip "StatefulSet $NAMESPACE/$STS not found"
fi

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> launching $RECORDS-record idempotent producer in background"
# acks=-1 + the explicit idempotence config make the broker-side
# stage-B path active. throughput=-1 lets the producer ramp; we want
# in-flight batches when the restart hits.
(
  "$KAFKA_BIN/kafka-producer-perf-test.sh" \
    --topic "$TOPIC" \
    --num-records "$RECORDS" \
    --record-size 256 \
    --throughput -1 \
    --producer-props \
      bootstrap.servers="$BOOTSTRAP" \
      acks=-1 \
      enable.idempotence=true \
      max.in.flight.requests.per.connection=5 \
      retries=2147483647 \
      delivery.timeout.ms=120000 \
    > "$TMP/perf.out" 2> "$TMP/perf.err"
  echo $? > "$TMP/perf.exit"
) &
PERF_PID=$!

# Every failure past this point has to reap the background producer, or
# the script exits leaving a 50k-record perf run hammering the cluster
# the next script is about to use.
fail() {
  echo "FAIL: $*" >&2
  kill "$PERF_PID" 2>/dev/null
  exit 1
}

# Let some batches land so the broker has dedupe-window state worth
# preserving; without this the test would degenerate to "open the
# partition fresh" which is the easy case.
sleep 2

# Pick a broker pod and bounce it. Using `delete pod` rather than
# `rollout restart` so the rollout completes in one cycle (faster
# signal); the StatefulSet recreates it under the same name.
TARGET_POD="${TARGET_POD:-${STS}-0}"
# Remember which incarnation we are replacing. `delete --wait=false`
# returns immediately and the grace period keeps the old pod Running and
# Ready for a further ~10s, so waiting on `condition=ready` alone matches
# the *outgoing* pod and returns instantly — the test would then judge a
# restart that had not started yet.
OLD_UID="$(kubectl -n "$NAMESPACE" get pod "$TARGET_POD" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"

echo ">> bouncing pod $NAMESPACE/$TARGET_POD"
kubectl -n "$NAMESPACE" delete pod "$TARGET_POD" --wait=false --grace-period=10 \
  || fail "could not delete pod"

echo ">> waiting for $TARGET_POD to be replaced"
for _ in $(seq 1 150); do
  NEW_UID="$(kubectl -n "$NAMESPACE" get pod "$TARGET_POD" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  if [ -n "$NEW_UID" ] && [ "$NEW_UID" != "$OLD_UID" ]; then break; fi
  sleep 1
done
if [ -z "${NEW_UID:-}" ] || [ "${NEW_UID:-}" = "$OLD_UID" ]; then
  fail "pod was never replaced"
fi

# Wait for the StatefulSet to bring the pod back Ready before we
# pass judgement on the producer. controller-runtime's lease-renewal
# loop and the broker's TakeOver path both run on pod start; until
# both are settled, the producer might legitimately retry.
echo ">> waiting for $TARGET_POD to be Ready again"
kubectl -n "$NAMESPACE" wait --for=condition=ready --timeout=120s "pod/$TARGET_POD" \
  || fail "pod did not return Ready in time"

# Bouncing a broker moves its partitions and its consumer-group
# coordinators onto the survivors, and that reshuffle outlives the
# restarted pod's own readiness. Settle here rather than at the end of
# the script so every exit path below — including the failure ones —
# hands the next script in the suite a healthy cluster.
wait_cluster_ready

echo ">> waiting for producer to finish"
# Bounded wait — the perf test is sized for ~30s on a healthy cluster
# even with one pod restart in the middle.
for _ in $(seq 1 90); do
  if [ -f "$TMP/perf.exit" ]; then break; fi
  sleep 1
done
if ! [ -f "$TMP/perf.exit" ]; then
  tail -30 "$TMP/perf.err" >&2
  fail "producer did not finish within timeout"
fi

PERF_EXIT=$(cat "$TMP/perf.exit")
echo ">> producer exit code: $PERF_EXIT"

if [ "$PERF_EXIT" -ne 0 ]; then
  echo "FAIL: producer died after broker restart" >&2
  echo "--- producer stderr (last 30 lines) ---" >&2
  tail -30 "$TMP/perf.err" >&2
  if grep -qE 'OutOfOrderSequenceException|InvalidProducerEpoch|UnknownProducerIdException' "$TMP/perf.err"; then
    echo ">> diagnostic: producer hit an idempotence-fatal error — likely B2 snapshot was not loaded after restart" >&2
  fi
  exit 1
fi

# HWM cross-check: even if the producer exited 0, a silent dedupe
# bug could let duplicates land or in-flight records vanish. The
# offset must equal $RECORDS exactly.
echo ">> verifying high watermark == $RECORDS"
HWM=$("$KAFKA_BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" --time -1 | awk -F: '{print $3}')
if [ "$HWM" != "$RECORDS" ]; then
  echo "FAIL: HWM=$HWM, want $RECORDS (idempotence broken across restart)" >&2
  exit 1
fi

echo ">> PASS"
