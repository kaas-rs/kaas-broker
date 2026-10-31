# KIP-32 — Record timestamps

**Status: partial** — see the [KIP index](../kip-index.md). CreateTime
timestamps round-trip byte-identically; **LogAppendTime and
timestamp→offset lookup are not implemented** (details below).

## What the KIP changes in Apache Kafka

Kafka 0.10 added a timestamp to every message, plus the
`message.timestamp.type` topic config choosing between **CreateTime**
(producer-supplied, the default) and **LogAppendTime** (broker overwrites
the timestamp at append). Timestamps feed time-based retention, compaction
lag, and offset-for-timestamp lookup.

## How kaas implements it

Timestamps are honoured **byte-opaquely**. RecordBatches flow through the
broker as uninterpreted bytes (the
[byte-opacity contract](../wire-protocol.md#the-byte-opacity-contract)),
so whatever timestamps and timestamp-type attribute bits the producer
wrote are stored and served back byte-identical. Concretely:

- The only timestamp the broker reads is the batch-level `MaxTimestamp`:
  `parse_batch_offsets` in `crates/kaas-storage/src/segment.rs` pulls it
  from bytes `[35..43]` of the v2 batch header — the records payload is
  never touched. Each segment tracks the highest value seen
  (`ActiveSegment::max_timestamp`).
- **LogAppendTime is not implemented.** kaas never rewrites timestamps.
  `message.timestamp.type` is advertised by `DescribeConfigs` as
  `CreateTime` and the config-admin path accepts that value as an
  accept-and-drop key — validated, never persisted
  (`crates/kaas-broker/src/topic_cr_writer.rs`) — while `LogAppendTime`
  is rejected with `INVALID_CONFIG`; the per-topic config file itself
  (`crates/kaas-storage/src/topicconfig.rs`, which carries retention,
  segment, flush, and compaction knobs) has no timestamp-type field.
  The Produce response always returns
  `log_append_time_ms: -1` — the CreateTime sentinel
  (`crates/kaas-broker/src/handlers/produce.rs`). A topic cannot ask the
  broker to stamp append time; every topic behaves as CreateTime.
- **Timestamp-based ListOffsets lookup is a gap.** `EARLIEST` (-2) and
  `LATEST` (-1) resolve via log-start / high-watermark
  (`crates/kaas-broker/src/handlers/list_offsets.rs`), but a concrete
  timestamp returns the `(-1, -1)` "no matching offset" sentinel on both
  engines (`crates/kaas-storage/src/disk.rs`,
  `crates/kaas-storage/src/memory.rs`) — the segment-level
  `max_timestamp` tracking is in place, the timestamp→offset index that
  would answer the query is a follow-up.
- `max.message.time.difference.ms` validation does not exist — it would
  require reading record payloads, which the opacity contract forbids.

## How it's verified

- `crates/kaas-storage/src/segment.rs`:
  `create_then_append_one_batch_updates_state` asserts `MaxTimestamp` is
  parsed out of the 43-byte header and tracked per segment.
- `crates/kaas-storage/src/memory.rs`: `offset_for_timestamp_sentinel`
  pins the honest "no match" answer for timestamp queries.
- `crates/kaas-storage/src/disk.rs` (`append_then_read_byte_identical`)
  and `bins/kaas/tests/smoke.rs` (`produce_fetch_metadata_roundtrip`) —
  records, timestamps included, come back byte-identical from the disk
  engine and over the wire.
- `scripts/kafka-get-offsets.sh` exercises `--time -2` / `--time -1`
  against a live broker; `scripts/kafka-console-producer.sh` /
  `scripts/kafka-console-consumer.sh` round-trip producer-stamped records.
