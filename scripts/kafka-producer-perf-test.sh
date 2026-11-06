#!/usr/bin/env bash
# Test kafka-producer-perf-test.sh against kaas.
#
# Scenarios:
#   1. 10k records, 1KB payload, acks=1
#   2. Same with --producer-props compression.type=snappy

. "$(dirname "$0")/_common.sh"

"$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 3 --replication-factor 1
delete_on_exit "$TOPIC"

echo ">> Scenario 1: 10k records / 1KB / acks=1"
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
  --num-records 10000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers="$BOOTSTRAP" acks=1

echo ">> Scenario 2 (gh #13): produce 1k records under each compression codec"
# Apache producers can pick gzip / snappy / lz4 / zstd. Kaas stores
# the compressed RecordBatch verbatim — it never decompresses — so
# this test confirms each codec round-trips through Produce without
# the broker tripping on the codec-bit combination. A consumer would
# decompress on its end; this script only verifies the producer
# wire path (consumer-side coverage, where ALL four codecs decode
# back to the original bytes, lives in the broker integration tests).
for codec in gzip snappy lz4 zstd; do
  echo "  -- $codec"
  "$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
    --num-records 1000 --record-size 1024 --throughput -1 \
    --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 \
      "compression.type=$codec" 2>&1 | tail -1
done

echo ">> Scenario 3 (gh #14): oversized record (>1MB) must be rejected"
# Apache broker default max.message.bytes=1048588. Kaas mirrors that
# via KAAS_MAX_MESSAGE_BYTES (chart-default 1048588). Push one
# 2 MiB record with max.request.size lifted client-side so the
# rejection comes from the broker, not the client's own pre-flight.
#
# NOTE: kafka-producer-perf-test.sh exits 0 even when every record
# fails — send errors arrive via the producer callback, not the
# process exit code. So classify on the output text, not $?.
"$KAFKA_BIN/kafka-producer-perf-test.sh" --topic "$TOPIC" \
   --num-records 1 --record-size $((2 * 1024 * 1024)) --throughput -1 \
   --producer-props bootstrap.servers="$BOOTSTRAP" acks=1 \
      max.request.size=$((4 * 1024 * 1024)) \
      buffer.memory=$((8 * 1024 * 1024)) \
   >"$TMP/oversize.out" 2>&1 || true
if grep -qE "RecordTooLarge|RecordBatchTooLarge|RECORD_LIST_TOO_LARGE|RECORD_TOO_LARGE|MESSAGE_TOO_LARGE|too large|larger than the configured" "$TMP/oversize.out"; then
  echo "(expected) oversized record rejected by broker"
elif grep -qE "^1 records sent" "$TMP/oversize.out"; then
  echo "FAIL: 2 MiB record was accepted; broker is not enforcing max.message.bytes (#14)" >&2
  tail -5 "$TMP/oversize.out" >&2
  exit 1
else
  echo "FAIL: oversized-record outcome unclear — neither a broker rejection nor a clean accept" >&2
  tail -5 "$TMP/oversize.out" >&2
  exit 1
fi

echo ">> PASS"
