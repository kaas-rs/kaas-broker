//! Transaction-coordinator persistent state store.
//!
//! Tracks
//! `(producer_id, epoch, state, partitions, groups, ongoing_since_ms,
//! transaction_timeout_ms)` per `transactional.id`, sharded across
//! 50 JSON files under `<data_dir>/__cluster/txn_state/slot-<n>.json`.
//!
//! Mirrors Apache Kafka's `__transaction_state` internal topic:
//! partition = slot, log replay = JSON file read. Kaas skips the
//! log-replay step because the file *is* the materialised map the
//! Apache coordinator builds from compacted log records.
//!
//! **Read-fresh-on-every-call** semantics: each public method
//! re-reads the slot file from disk before mutating, then writes
//! back via atomic `tmp + fsync + rename`. NFS close-to-open
//! consistency means a fresh `File::open` sees the latest committed
//! state from any other broker that recently wrote — so on
//! coordinator failover the new owner continues from the same
//! (PID, epoch) state without log replay.
//!
//! **Known gaps**:
//! - pre-#108 single-file → slot-layout migration is not implemented —
//!   only the slot layout ships; ancient deployments upgrade through
//!   a stop-the-world cutover.
//! - `migrateLayout` (slot count re-shard) — `NUM_SLOTS` is pinned
//!   to `DEFAULT_NUM_SLOTS = 50` for the whole cluster.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::atomic_write::atomic_write_json;

/// Matches Apache Kafka's `transaction.state.log.num.partitions=50`
/// default. Pinning to a fixed cluster-wide constant decouples the
/// storage layout from broker scale operations.
///
/// **Do not change this value on a deployed cluster.** The on-disk
/// slot is computed as `fnv1a_32(txn_id) % NUM_SLOTS`; a different
/// constant moves every existing entry to a different slot file and
/// the next `init_producer_id` for an existing `txn_id` reads an
/// empty slot — silently breaking the gh #22 rejoin contract. Apache
/// enforces this by reading `transaction.state.log.num.partitions`
/// at first cluster start and ignoring later changes. Kaas relies
/// on the constant staying constant; a re-shard path
/// is the documented follow-up on gh #174 for
/// the day the value needs to change.
pub const DEFAULT_NUM_SLOTS: usize = 50;

// gh #174: compile-time guard. If a future edit changes the
// constant, the build fails — the on-disk layout is shared with
// existing deployments and silently re-slotting their entries on a
// rolling upgrade breaks the gh #22 fence-on-rejoin contract.
// Bump this assertion deliberately as part of the same change that
// ships the migration path.
const _: () = assert!(
    DEFAULT_NUM_SLOTS == 50,
    "NUM_SLOTS is shared with on-disk layout — see gh #174"
);

/// Transaction state machine. Mirrors Apache's `TransactionState`
/// (TransactionMetadata.scala). The on-disk JSON representation
/// uses stable human-readable strings (v0.1-compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TxnState {
    /// No transaction in progress (default).
    #[serde(rename = "")]
    #[default]
    Empty,
    /// At least one `AddPartitionsToTxn` or `AddOffsetsToTxn`
    /// succeeded.
    Ongoing,
    /// `EndTxn(commit)` accepted, transition in flight.
    PrepareCommit,
    /// `EndTxn(abort)` accepted, transition in flight.
    PrepareAbort,
    /// Commit finished. Idempotent commit retries return `Ok(())`.
    CompleteCommit,
    /// Abort finished. Idempotent abort retries return `Ok(())`.
    CompleteAbort,
}

impl TxnState {
    fn is_empty(&self) -> bool {
        matches!(self, TxnState::Empty)
    }
}

/// One `(topic, partitions)` tuple inside a [`TxnEntry`]. Apache's
/// wire/storage shape uses `TopicPartition` (a single `(topic, int32)`
/// pair) but groups by topic on the wire; we store the same grouped
/// form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Persistent record of one transactional producer. JSON shape is
/// pinned so a v0.1-written slot file
/// reads cleanly through this struct (and vice versa for the
/// migration window).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxnEntry {
    pub pid: i64,
    pub epoch: i16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<TxnTopic>,
    /// Wall-clock millis when the entry entered `Ongoing`. Zero
    /// in any other state. Reaper input.
    #[serde(default, skip_serializing_if = "i64_is_zero")]
    pub ongoing_since_ms: i64,
    /// Mirrors `InitProducerIdRequest.transaction_timeout_ms`
    /// (KIP-98).
    #[serde(default, skip_serializing_if = "i32_is_zero")]
    pub transaction_timeout_ms: i32,
    #[serde(default, skip_serializing_if = "TxnState::is_empty")]
    pub state: TxnState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
    /// Apache's `lastProducerId` (KIP-360): the PID this entry held
    /// before its latest `InitProducerId`. Differs from `pid` only
    /// after a rotation on epoch exhaustion, and it is only consulted
    /// while `state` is `Empty` — the state every `InitProducerId`
    /// lands in and nothing else does — because Apache overwrites it
    /// with the current PID on every later transition.
    #[serde(default = "no_producer_id", skip_serializing_if = "is_no_producer_id")]
    pub last_pid: i64,
    /// Apache's `lastProducerEpoch` (KIP-360): the epoch a producer
    /// held before its own claimed-identity bump, so a retry of that
    /// bump gets the same answer instead of `PRODUCER_FENCED`. `-1`
    /// after a new-instance init or a fence — that is what makes the
    /// previous session's claim stale.
    #[serde(
        default = "no_producer_epoch",
        skip_serializing_if = "is_no_producer_epoch"
    )]
    pub last_epoch: i16,
}

/// `RecordBatch.NO_PRODUCER_ID`.
pub const NO_PRODUCER_ID: i64 = -1;
/// `RecordBatch.NO_PRODUCER_EPOCH`.
pub const NO_PRODUCER_EPOCH: i16 = -1;

/// Apache's `TransactionMetadata.isEpochExhausted`. One short of
/// `i16::MAX` because fencing an `Ongoing` transaction bumps once more
/// without going through `InitProducerId`'s rotation check.
fn is_epoch_exhausted(epoch: i16) -> bool {
    epoch >= i16::MAX - 1
}

fn i64_is_zero(v: &i64) -> bool {
    *v == 0
}
fn i32_is_zero(v: &i32) -> bool {
    *v == 0
}
fn no_producer_id() -> i64 {
    NO_PRODUCER_ID
}
fn is_no_producer_id(v: &i64) -> bool {
    *v == NO_PRODUCER_ID
}
fn no_producer_epoch() -> i16 {
    NO_PRODUCER_EPOCH
}
fn is_no_producer_epoch(v: &i16) -> bool {
    *v == NO_PRODUCER_EPOCH
}

/// What [`TxnStateStore::init_producer_id`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitPidOutcome {
    /// Hand `(pid, epoch)` to the producer.
    Ready { pid: i64, epoch: i16 },
    /// The id had an `Ongoing` transaction, so it was fenced instead:
    /// the entry is now `PrepareAbort` at a bumped epoch with its
    /// dispatch set intact. The caller owes the ABORT markers (or
    /// leaves them to the reconcile) and must answer
    /// `CONCURRENT_TRANSACTIONS` — the bumped epoch is never handed to
    /// anyone, and the producer's retry bumps again once it completes.
    AbortingOngoing(PendingMarkerDispatch),
}

/// One transaction still owing markers, as reported by
/// [`TxnStateStore::pending_marker_dispatches`]. `epoch` is the
/// entry's *current* epoch — for a reaper-prepared abort that is the
/// already-bumped one, and it is what
/// [`TxnStateStore::complete_end_txn`] validates against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMarkerDispatch {
    pub txn_id: String,
    pub pid: i64,
    pub epoch: i16,
    /// `true` for COMMIT, `false` for ABORT.
    pub commit: bool,
    pub partitions: Vec<TxnTopic>,
}

/// Side-effect record [`TxnStateStore::abort_overdue_owned`] returns
/// per aborted txn — feeds metrics, the future cross-broker marker
/// writer (gh #114), and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnAbortRecord {
    pub txn_id: String,
    pub pid: i64,
    pub old_epoch: i16,
    pub new_epoch: i16,
    pub groups: Vec<String>,
}

/// Return value of [`TxnStateStore::prepare_end_txn`]. Carries the
/// partition + group lists the caller must dispatch COMMIT / ABORT
/// control batches for (gh #114 / gh #175).
///
/// The lists are *copies* — they stay on the persisted entry until
/// [`TxnStateStore::complete_end_txn`] clears them, so a dispatch
/// failure or a coordinator crash re-derives the identical set on the
/// next attempt (gh #225).
///
/// `transition_fired = false` is the nothing-to-do path — the txn was
/// already `CompleteCommit`/`CompleteAbort`, its markers are long
/// written, and no fresh side effects are required.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndTxnOutcome {
    pub partitions: Vec<TxnTopic>,
    pub groups: Vec<String>,
    pub transition_fired: bool,
}

/// Cross-coordinator signal that fires on every `EndTxn` (and
/// reaper-driven abort) transition. The txn coordinator tells the
/// group's offset store to either materialise (`commit = true`) or
/// discard (`commit = false`) the pending offsets that
/// `TxnOffsetCommit` staged earlier.
///
/// In Apache, this signal travels via `WriteTxnMarkers` to the
/// `__consumer_offsets[partitionFor(group_id)]` partition's leader.
/// Kaas stages it as a local hook — when txn coord and group
/// coord live on the same broker it fires directly; cross-broker
/// dispatch lands with gh #114.
pub trait TxnOffsetHook: Send + Sync + 'static {
    fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool);
}

/// Errors mappable to Kafka wire codes by the txn handlers. Keeping
/// the store transport-free lets each handler pick the v0-3 (per-
/// partition error code) or v4+ (top-level error code) shape
/// without leaking codec types into this crate.
#[derive(Debug, Error)]
pub enum TxnStateError {
    /// Wire code 42 — INVALID_REQUEST (what the handlers send; Apache has
    /// no dedicated code for an empty transactional id).
    #[error("txn state: empty transactional id")]
    EmptyTxnId,
    /// Wire code 49 — INVALID_PRODUCER_ID_MAPPING.
    #[error("txn state: unknown txn id or pid mismatch")]
    UnknownProducer,
    /// Wire code 90 — PRODUCER_FENCED, the txn-coordinator convention
    /// (KIP-588); Produce answers a stale epoch with 47
    /// INVALID_PRODUCER_EPOCH instead.
    #[error("txn state: producer epoch fenced")]
    EpochFenced,
    /// Wire code 51 — CONCURRENT_TRANSACTIONS.
    #[error("txn state: concurrent transition in progress")]
    Concurrent,
    /// Wire code 48 — INVALID_TXN_STATE (gh #198; 50 is INVALID_TRANSACTION_TIMEOUT).
    #[error("txn state: invalid state transition")]
    InvalidState,
    #[error("txn state: i/o: {0}")]
    Io(#[from] io::Error),
    #[error("txn state: decode: {0}")]
    Decode(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, TxnStateError>;

/// Per-cluster transactional-state store. See module docs.
pub struct TxnStateStore {
    dir: PathBuf,
    num_slots: usize,
    /// Single global mutex serialises slot reads + writes
    /// (coarse on purpose). The store sits off the hot
    /// path (txn surface fires at producer boot + per-txn commit;
    /// Produce/Fetch never touch it) so coarse locking is fine.
    mu: Mutex<()>,
    hook: RwLock<Option<Arc<dyn TxnOffsetHook>>>,
}

impl std::fmt::Debug for TxnStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnStateStore")
            .field("dir", &self.dir)
            .field("num_slots", &self.num_slots)
            .finish()
    }
}

impl TxnStateStore {
    /// Open the per-cluster transactional-state dir under
    /// `parent_dir/txn_state/`. `num_slots == 0` falls back to
    /// [`DEFAULT_NUM_SLOTS`]. Creates the directory if missing.
    pub fn open(parent_dir: &Path, num_slots: usize) -> io::Result<Self> {
        let num_slots = if num_slots == 0 {
            DEFAULT_NUM_SLOTS
        } else {
            num_slots
        };
        let dir = parent_dir.join("txn_state");
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            num_slots,
            mu: Mutex::new(()),
            hook: RwLock::new(None),
        })
    }

    pub fn num_slots(&self) -> usize {
        self.num_slots
    }

    /// Wire the cross-coordinator offset hook. Production sets this
    /// from the broker bootstrap so `EndTxn` / reaper aborts
    /// materialise or discard pending offsets staged by
    /// `TxnOffsetCommit`. `None` (the default) leaves the hook
    /// silent — pending offsets remain staged.
    pub fn set_offset_hook(&self, hook: Arc<dyn TxnOffsetHook>) {
        *self.hook.write() = Some(hook);
    }

    /// The gh #22 contract: first call for `txn_id` returns a fresh
    /// PID with `epoch = 0`; every subsequent call returns the
    /// **same** PID with `epoch += 1`.
    pub fn get_or_allocate<F>(&self, txn_id: &str, alloc: F) -> Result<(i64, i16)>
    where
        F: FnOnce() -> i64,
    {
        self.get_or_allocate_with_timeout(txn_id, 0, alloc)
    }

    /// As [`get_or_allocate`] but also records the producer's
    /// `transaction.timeout.ms`. `timeout_ms <= 0` leaves the
    /// existing entry's timeout untouched (cooperative for the
    /// non-transactional fast path).
    ///
    /// [`Self::init_producer_id`] with no claimed identity. An
    /// `Ongoing` or `Prepare*` entry answers
    /// [`TxnStateError::Concurrent`]; an `Ongoing` one is fenced on the
    /// way, and its ABORT markers are left to the reconcile.
    pub fn get_or_allocate_with_timeout<F>(
        &self,
        txn_id: &str,
        timeout_ms: i32,
        alloc: F,
    ) -> Result<(i64, i16)>
    where
        F: FnOnce() -> i64,
    {
        match self.init_producer_id(txn_id, timeout_ms, None, alloc)? {
            InitPidOutcome::Ready { pid, epoch } => Ok((pid, epoch)),
            InitPidOutcome::AbortingOngoing(_) => Err(TxnStateError::Concurrent),
        }
    }

    /// `InitProducerId` for a transactional id — Apache's
    /// `prepareInitProducerIdTransit` (gh #22, KIP-360, gh #201).
    ///
    /// `expected` is the v3+ request's claimed `(producer_id,
    /// producer_epoch)`, `None` for a new producer instance. The claim
    /// is checked before anything moves:
    ///
    /// - The PID must be the entry's current one — or its previous one
    ///   with an exhausted epoch, a retry of a rotation whose response
    ///   was lost. Anything else is [`TxnStateError::EpochFenced`].
    /// - The epoch must be the current one (bump it, remembering the
    ///   old one) or the remembered one (a retry of that bump: answer
    ///   the current epoch unchanged). Anything else is fenced.
    ///
    /// Before KIP-360 validation landed, every call bumped regardless,
    /// so a zombie re-calling `InitProducerId` was handed a fresh valid
    /// epoch and fenced the live session instead of being fenced
    /// itself.
    ///
    /// By state:
    ///
    /// ```text
    /// (none)                   → fresh PID, epoch 0 (any claim accepted:
    ///                            a producer recovering from
    ///                            UNKNOWN_PRODUCER_ID)
    /// Empty / Complete*        → bump, or rotate to a fresh PID once the
    ///                            epoch is exhausted; lands in Empty
    /// Ongoing                  → fence: PrepareAbort at epoch + 1, lists
    ///                            kept → AbortingOngoing
    /// Prepare*                 → Concurrent (markers still in flight)
    /// ```
    ///
    /// The `Ongoing` arm is the other half of gh #201. It used to bump
    /// and leave the entry `Ongoing` with the previous session's
    /// partition list, so the new session inherited the zombie's open
    /// transaction and its first commit committed the zombie's records
    /// too.
    pub fn init_producer_id<F>(
        &self,
        txn_id: &str,
        timeout_ms: i32,
        expected: Option<(i64, i16)>,
        alloc: F,
    ) -> Result<InitPidOutcome>
    where
        F: FnOnce() -> i64,
    {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let Some(mut entry) = state.get(txn_id).cloned() else {
            let entry = TxnEntry {
                pid: alloc(),
                epoch: 0,
                partitions: Vec::new(),
                ongoing_since_ms: 0,
                transaction_timeout_ms: timeout_ms.max(0),
                state: TxnState::Empty,
                groups: Vec::new(),
                last_pid: NO_PRODUCER_ID,
                last_epoch: NO_PRODUCER_EPOCH,
            };
            let (pid, epoch) = (entry.pid, entry.epoch);
            state.insert(txn_id.to_owned(), entry);
            self.persist_slot(slot, &state)?;
            return Ok(InitPidOutcome::Ready { pid, epoch });
        };

        if let Some((claimed_pid, claimed_epoch)) = expected {
            let retried_rotation = entry.state == TxnState::Empty
                && claimed_pid == entry.last_pid
                && is_epoch_exhausted(claimed_epoch);
            if claimed_pid != entry.pid && !retried_rotation {
                return Err(TxnStateError::EpochFenced);
            }
        }
        if timeout_ms > 0 {
            entry.transaction_timeout_ms = timeout_ms;
        }

        match entry.state {
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                return Err(TxnStateError::Concurrent);
            }
            TxnState::Ongoing => {
                // Saturating: an entry can only sit `Ongoing` at
                // `i16::MAX` if an older build handed that epoch out
                // (it rotated at MAX, not MAX - 1). The abort still has
                // to happen; the producer's retry then rotates.
                entry.epoch = entry.epoch.saturating_add(1);
                entry.last_epoch = NO_PRODUCER_EPOCH;
                entry.state = TxnState::PrepareAbort;
                entry.ongoing_since_ms = 0;
                let dispatch = PendingMarkerDispatch {
                    txn_id: txn_id.to_owned(),
                    pid: entry.pid,
                    epoch: entry.epoch,
                    commit: false,
                    partitions: entry.partitions.clone(),
                };
                state.insert(txn_id.to_owned(), entry);
                self.persist_slot(slot, &state)?;
                return Ok(InitPidOutcome::AbortingOngoing(dispatch));
            }
            TxnState::Empty | TxnState::CompleteCommit | TxnState::CompleteAbort => {}
        }

        let claimed_epoch = expected.map(|(_, e)| e);
        if is_epoch_exhausted(entry.epoch) && claimed_epoch.is_none_or(|e| e == entry.epoch) {
            // Rotate. Remembering the old epoch only for a claimed
            // bump is Apache's `recordLastEpoch`: a new instance has
            // no retry to recognise.
            entry.last_pid = entry.pid;
            entry.last_epoch = if expected.is_some() {
                entry.epoch
            } else {
                NO_PRODUCER_EPOCH
            };
            entry.pid = alloc();
            entry.epoch = 0;
        } else {
            entry.last_pid = entry.pid;
            match claimed_epoch {
                None => {
                    entry.epoch += 1;
                    entry.last_epoch = NO_PRODUCER_EPOCH;
                }
                Some(e) if e == entry.epoch => {
                    entry.last_epoch = entry.epoch;
                    entry.epoch += 1;
                }
                // A retry of a bump that already happened: same answer.
                Some(e) if e == entry.last_epoch => {}
                Some(_) => return Err(TxnStateError::EpochFenced),
            }
        }
        // Complete* lists are already empty; Empty is the one state
        // only InitProducerId produces, which `last_pid` relies on.
        entry.state = TxnState::Empty;
        let (pid, epoch) = (entry.pid, entry.epoch);
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        Ok(InitPidOutcome::Ready { pid, epoch })
    }

    /// Union `additions` into the entry's partition list. gh #23.
    /// Validation order matches Apache:
    /// 1. empty `txn_id` → [`TxnStateError::EmptyTxnId`]
    /// 2. no entry → [`TxnStateError::UnknownProducer`]
    /// 3. PID mismatch → [`TxnStateError::UnknownProducer`]
    /// 4. Epoch mismatch → [`TxnStateError::EpochFenced`]
    /// 5. `Prepare*` state → [`TxnStateError::Concurrent`]
    pub fn add_partitions(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        additions: &[TxnTopic],
        now_ms: i64,
    ) -> Result<()> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }
        match entry.state {
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                return Err(TxnStateError::Concurrent);
            }
            _ => {}
        }

        let merged = merge_partitions(&mut entry, additions);
        let was_not_ongoing = entry.state != TxnState::Ongoing;
        if was_not_ongoing {
            entry.state = TxnState::Ongoing;
            entry.ongoing_since_ms = now_ms;
        }
        if !merged && !was_not_ongoing {
            // Idempotent no-op — every (topic, partition) already
            // recorded AND no state change.
            return Ok(());
        }
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        Ok(())
    }

    /// Record that the producer will commit offsets to consumer
    /// group `group_id` as part of this transaction. gh #24.
    /// Idempotent — re-adding the same group is a no-op.
    pub fn add_offsets_to_txn(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        group_id: &str,
        now_ms: i64,
    ) -> Result<()> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        if group_id.is_empty() {
            return Err(TxnStateError::InvalidState);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }
        match entry.state {
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                return Err(TxnStateError::Concurrent);
            }
            _ => {}
        }

        let already_recorded = entry.groups.iter().any(|g| g == group_id);
        let needs_state_advance = entry.state != TxnState::Ongoing;
        if already_recorded && !needs_state_advance {
            return Ok(());
        }
        if !already_recorded {
            entry.groups.push(group_id.to_owned());
        }
        if needs_state_advance {
            entry.state = TxnState::Ongoing;
            entry.ongoing_since_ms = now_ms;
        }
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        Ok(())
    }

    /// Phase 1 of the `EndTxn` (API key 26) transition. gh #25 / #26,
    /// split out under gh #225.
    ///
    /// ```text
    /// Ongoing        → PrepareCommit   (commit = true)
    /// Ongoing        → PrepareAbort    (commit = false)
    /// PrepareCommit  + commit = true   → re-drive (same lists)
    /// PrepareAbort   + commit = false  → re-drive (same lists)
    /// PrepareCommit  + commit = false  → InvalidState
    /// PrepareAbort   + commit = true   → InvalidState
    /// CompleteCommit + commit = true   → Ok, transition_fired = false
    /// CompleteAbort  + commit = false  → Ok, transition_fired = false
    /// Complete* mismatched             → InvalidState
    /// Empty                            → InvalidState
    /// ```
    ///
    /// **The partition and group lists are deliberately NOT cleared
    /// here** — that is the whole point of the split. `Prepare*` plus a
    /// retained partition list *is* the durable record of "these
    /// markers still owe a write", so a dispatch failure, a coordinator
    /// crash, or a producer retry all re-derive the same dispatch set
    /// instead of losing it. Clearing happens in
    /// [`Self::complete_end_txn`], which the caller may only invoke once
    /// every marker is durable.
    ///
    /// This is the NFS-substrate rule-2 shape (see
    /// `docs/src/architecture/nfs-substrate.md`): the compound
    /// "transition + write N markers" op is not a single atomic
    /// primitive, so it is made idempotent and driven to completion by
    /// retry rather than fired once and hoped for.
    ///
    /// Re-driving on a matching `Prepare*` replaces the previous
    /// `Concurrent` error. A producer retrying `EndTxn` after a failed
    /// dispatch must be able to push it through; answering
    /// CONCURRENT_TRANSACTIONS forever is exactly the wedge gh #225
    /// describes.
    pub fn prepare_end_txn(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<EndTxnOutcome> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }

        let want_prepare = if commit {
            TxnState::PrepareCommit
        } else {
            TxnState::PrepareAbort
        };

        match entry.state {
            TxnState::Ongoing => {
                entry.state = want_prepare;
                // Lists stay put — see the doc comment. They are read
                // out for the caller but remain on the entry so a
                // re-drive sees the same set.
                let partitions = entry.partitions.clone();
                let groups = entry.groups.clone();
                state.insert(txn_id.to_owned(), entry);
                self.persist_slot(slot, &state)?;
                Ok(EndTxnOutcome {
                    partitions,
                    groups,
                    transition_fired: true,
                })
            }
            // Already preparing: re-drive with the retained lists so a
            // retry re-attempts dispatch for exactly the same targets.
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                if entry.state != want_prepare {
                    return Err(TxnStateError::InvalidState);
                }
                Ok(EndTxnOutcome {
                    partitions: entry.partitions.clone(),
                    groups: entry.groups.clone(),
                    transition_fired: true,
                })
            }
            TxnState::CompleteCommit => {
                if commit {
                    Ok(EndTxnOutcome::default())
                } else {
                    Err(TxnStateError::InvalidState)
                }
            }
            TxnState::CompleteAbort => {
                if commit {
                    Err(TxnStateError::InvalidState)
                } else {
                    Ok(EndTxnOutcome::default())
                }
            }
            TxnState::Empty => Err(TxnStateError::InvalidState),
        }
    }

    /// Phase 2 of `EndTxn`: `Prepare* → Complete*`, clearing the
    /// partition + group lists and firing the offset hook.
    ///
    /// **Only call this once every marker for the transaction is
    /// durable.** It is the step that discards the dispatch set, so
    /// calling it early re-creates gh #225: the txn reads as finished
    /// while some partition never received its marker, leaving
    /// `read_committed` consumers with a permanently pinned LSO and no
    /// way for any retry to notice.
    ///
    /// The offset hook fires here rather than in
    /// [`Self::prepare_end_txn`] so staged `TxnOffsetCommit` offsets
    /// only become visible once the markers backing them are durable.
    ///
    /// Idempotent on `Complete*` with a matching `commit`.
    pub fn complete_end_txn(&self, txn_id: &str, pid: i64, epoch: i16, commit: bool) -> Result<()> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }

        let (want_prepare, want_complete) = if commit {
            (TxnState::PrepareCommit, TxnState::CompleteCommit)
        } else {
            (TxnState::PrepareAbort, TxnState::CompleteAbort)
        };

        if entry.state == want_complete {
            return Ok(()); // idempotent
        }
        if entry.state != want_prepare {
            return Err(TxnStateError::InvalidState);
        }

        entry.state = want_complete;
        let groups = std::mem::take(&mut entry.groups);
        entry.partitions.clear();
        entry.ongoing_since_ms = 0;
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        let hook = self.hook.read().clone();
        if let Some(hook) = hook {
            for g in &groups {
                hook.on_end_txn(g, pid, commit);
            }
        }
        Ok(())
    }

    /// Single-phase `EndTxn` — [`Self::prepare_end_txn`] immediately
    /// followed by [`Self::complete_end_txn`].
    ///
    /// **Not for the production EndTxn path.** It completes the
    /// transaction without giving the caller a window to write markers,
    /// which is precisely the gh #225 defect. It exists for tests and
    /// for callers that have no markers to dispatch.
    pub fn end_txn(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<EndTxnOutcome> {
        let outcome = self.prepare_end_txn(txn_id, pid, epoch, commit)?;
        if outcome.transition_fired {
            self.complete_end_txn(txn_id, pid, epoch, commit)?;
        }
        Ok(outcome)
    }

    /// As [`abort_overdue_owned`] without the ownership gate.
    /// Tests / dev mode only — production multi-broker setups must
    /// pass a real `owns_txn` closure.
    pub fn abort_overdue(&self, now_ms: i64) -> Vec<TxnAbortRecord> {
        self.abort_overdue_owned(now_ms, None)
    }

    /// Every transaction sitting in `Prepare*` with partitions still
    /// owed a COMMIT / ABORT marker (gh #225).
    ///
    /// This is the reconcile half of NFS-substrate rule 2. The retry
    /// half — a producer re-sending `EndTxn` — only covers producers
    /// that come back. A producer that crashed, gave up, or was fenced
    /// leaves the transaction prepared forever, and every timed-out
    /// transaction the reaper prepares has no producer to retry at all.
    /// Something has to drive those to completion, or their partitions
    /// keep a pinned LSO indefinitely.
    ///
    /// Entries with an empty partition list are skipped: there is
    /// nothing to dispatch, and the caller's `complete_end_txn` closes
    /// them out on the next pass anyway.
    ///
    /// `owns_txn` gates the sweep exactly as
    /// [`Self::abort_overdue_owned`] does — a slot file has one legal
    /// writer (NFS substrate rule 3), so a multi-broker deployment must
    /// pass a real closure.
    /// The `Sync` bound (unlike [`Self::abort_overdue_owned`]'s) is
    /// what lets an async caller hold the closure across an await and
    /// still have a `Send` future — the marker reconcile drives this
    /// from a spawned task.
    pub fn pending_marker_dispatches(
        &self,
        owns_txn: Option<&(dyn Fn(&str) -> bool + Sync)>,
    ) -> Vec<PendingMarkerDispatch> {
        let _guard = self.mu.lock();
        let mut out = Vec::new();
        for slot in 0..self.num_slots {
            let state = match self.load_slot(slot) {
                Ok(s) => s,
                Err(_) => continue,
            };
            for (txn_id, entry) in state {
                let commit = match entry.state {
                    TxnState::PrepareCommit => true,
                    TxnState::PrepareAbort => false,
                    _ => continue,
                };
                if entry.partitions.is_empty() {
                    continue;
                }
                if let Some(owns) = owns_txn {
                    if !owns(&txn_id) {
                        continue;
                    }
                }
                out.push(PendingMarkerDispatch {
                    txn_id,
                    pid: entry.pid,
                    epoch: entry.epoch,
                    commit,
                    partitions: entry.partitions,
                });
            }
        }
        out
    }

    /// Walk every slot, abort `Ongoing` entries past their
    /// `ongoing_since_ms + transaction_timeout_ms` deadline. gh #28.
    /// Bumps the producer epoch on abort so the next `InitProducerId`
    /// from the stuck client fences out via the gh #22 path.
    ///
    /// Lands the entry in **`PrepareAbort`, not `CompleteAbort`**, with
    /// its partition and group lists intact (gh #225). A timed-out
    /// transaction owes its partitions an ABORT marker exactly like a
    /// client-driven abort does; completing it here would clear the
    /// dispatch set with nothing written, leaving `read_committed`
    /// consumers with a pinned LSO and no record that anything was
    /// owed. [`Self::pending_marker_dispatches`] is what picks these up.
    ///
    /// The offset hook therefore fires in
    /// [`Self::complete_end_txn`] rather than here, so staged offsets
    /// are discarded on the same edge that writes the markers.
    ///
    /// `owns_txn` gates the sweep: when `Some`, only entries this
    /// broker is the coordinator for are touched (gh #91). When
    /// `None` (tests / dev / single broker) every slot is in scope.
    pub fn abort_overdue_owned(
        &self,
        now_ms: i64,
        owns_txn: Option<&dyn Fn(&str) -> bool>,
    ) -> Vec<TxnAbortRecord> {
        let _guard = self.mu.lock();
        let mut aborted = Vec::new();
        for slot in 0..self.num_slots {
            let mut state = match self.load_slot(slot) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut changed = false;
            // Collect txn_ids first to avoid concurrent mutation
            // of the map while iterating.
            let candidate_ids: Vec<String> = state.keys().cloned().collect();
            for txn_id in candidate_ids {
                let entry = match state.get(&txn_id) {
                    Some(e) if e.state == TxnState::Ongoing => e.clone(),
                    _ => continue,
                };
                if let Some(owns) = owns_txn {
                    if !owns(&txn_id) {
                        continue;
                    }
                }
                if entry.ongoing_since_ms == 0 || entry.transaction_timeout_ms <= 0 {
                    continue;
                }
                let deadline = entry.ongoing_since_ms + i64::from(entry.transaction_timeout_ms);
                if deadline > now_ms {
                    continue;
                }

                let pid = entry.pid;
                let old_epoch = entry.epoch;
                let groups = entry.groups.clone();

                let mut updated = entry;
                // PrepareAbort, not CompleteAbort: the partition and
                // group lists stay so the marker reconcile can find
                // them (gh #225). `complete_end_txn` clears them and
                // fires the offset hook once the markers are durable.
                updated.state = TxnState::PrepareAbort;
                updated.ongoing_since_ms = 0;
                updated.epoch = if updated.epoch == i16::MAX {
                    0
                } else {
                    updated.epoch + 1
                };
                let new_epoch = updated.epoch;
                state.insert(txn_id.clone(), updated);
                changed = true;

                aborted.push(TxnAbortRecord {
                    txn_id,
                    pid,
                    old_epoch,
                    new_epoch,
                    groups,
                });
            }
            if changed {
                let _ = self.persist_slot(slot, &state);
            }
        }
        aborted
    }

    /// Copy of every txn entry across every slot. Tests only.
    pub fn snapshot(&self) -> HashMap<String, TxnEntry> {
        let _guard = self.mu.lock();
        let mut out = HashMap::new();
        for slot in 0..self.num_slots {
            if let Ok(state) = self.load_slot(slot) {
                out.extend(state);
            }
        }
        out
    }

    fn slot_for(&self, txn_id: &str) -> usize {
        // Same dance as `kaas-broker::group_hash::coordinator_slot` —
        // u32 → u64 → usize is safe on every target (usize ≥ 32 bits)
        // and dodges the workspace `clippy::as-conversions` lint.
        let h = u64::from(fnv1a_32(txn_id.as_bytes()));
        let n = u64::try_from(self.num_slots).unwrap_or(u64::MAX);
        usize::try_from(h % n).unwrap_or(0)
    }

    fn slot_path(&self, slot: usize) -> PathBuf {
        self.dir.join(format!("slot-{slot}.json"))
    }

    fn load_slot(&self, slot: usize) -> Result<HashMap<String, TxnEntry>> {
        let path = self.slot_path(slot);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(e.into()),
        };
        if data.is_empty() {
            return Ok(HashMap::new());
        }
        let state: HashMap<String, TxnEntry> = serde_json::from_slice(&data)?;
        Ok(state)
    }

    fn persist_slot(&self, slot: usize, state: &HashMap<String, TxnEntry>) -> Result<()> {
        let name = format!("slot-{slot}.json");
        atomic_write_json(&self.dir, &name, state)?;
        Ok(())
    }
}

/// Union `additions` into `entry.partitions` in place. Returns
/// `true` if anything new was added; `false` if every
/// `(topic, partition)` was already recorded.
fn merge_partitions(entry: &mut TxnEntry, additions: &[TxnTopic]) -> bool {
    let mut changed = false;
    for add in additions {
        if let Some(existing) = entry.partitions.iter_mut().find(|t| t.topic == add.topic) {
            for p in &add.partitions {
                if !existing.partitions.contains(p) {
                    existing.partitions.push(*p);
                    changed = true;
                }
            }
        } else {
            entry.partitions.push(TxnTopic {
                topic: add.topic.clone(),
                partitions: add.partitions.clone(),
            });
            changed = true;
        }
    }
    changed
}

/// FNV-1a 32-bit. Same algorithm as `crates/kaas-broker/src/group_hash.rs`;
/// inlined here so `kaas-coordinator` doesn't pull
/// in a fnv crate just for the slot hash.
fn fnv1a_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut h = OFFSET;
    for b in bytes {
        h ^= u32::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

#[cfg(test)]
#[allow(clippy::redundant_closure)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn store() -> (tempfile::TempDir, TxnStateStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = TxnStateStore::open(tmp.path(), DEFAULT_NUM_SLOTS).unwrap();
        (tmp, store)
    }

    fn pid_alloc() -> impl FnMut() -> i64 {
        let counter = AtomicI64::new(100);
        move || counter.fetch_add(1, Ordering::SeqCst)
    }

    #[test]
    fn empty_txn_id_rejected() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        assert!(matches!(
            s.get_or_allocate("", || a()),
            Err(TxnStateError::EmptyTxnId)
        ));
    }

    #[test]
    fn first_call_allocates_epoch_zero_rejoin_bumps() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid1, e1) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(e1, 0);
        let (pid2, e2) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(pid1, pid2);
        assert_eq!(e2, 1);
        let (pid3, e3) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(pid1, pid3);
        assert_eq!(e3, 2);
    }

    #[test]
    fn distinct_txn_ids_get_distinct_pids() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (p1, _) = s.get_or_allocate("tx-a", || a()).unwrap();
        let (p2, _) = s.get_or_allocate("tx-b", || a()).unwrap();
        assert_ne!(p1, p2);
    }

    #[test]
    fn add_partitions_unknown_producer() {
        let (_t, s) = store();
        let err = s
            .add_partitions(
                "tx-1",
                1,
                0,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                0,
            )
            .unwrap_err();
        assert!(matches!(err, TxnStateError::UnknownProducer));
    }

    #[test]
    fn add_partitions_happy_path_then_idempotent() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1],
            }],
            1_000,
        )
        .unwrap();
        // Idempotent re-add — same tuples, no error, no spurious write.
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            2_000,
        )
        .unwrap();
        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.state, TxnState::Ongoing);
        assert_eq!(
            entry.partitions,
            vec![TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1]
            }]
        );
        assert_eq!(entry.ongoing_since_ms, 1_000);
    }

    #[test]
    fn add_partitions_unions_across_calls() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            10,
        )
        .unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[
                TxnTopic {
                    topic: "t".into(),
                    partitions: vec![1],
                },
                TxnTopic {
                    topic: "u".into(),
                    partitions: vec![5],
                },
            ],
            20,
        )
        .unwrap();
        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(
            entry.partitions,
            vec![
                TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0, 1]
                },
                TxnTopic {
                    topic: "u".into(),
                    partitions: vec![5]
                },
            ]
        );
    }

    #[test]
    fn epoch_mismatch_fences() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, _) = s.get_or_allocate("tx-1", || a()).unwrap();
        let err = s
            .add_partitions(
                "tx-1",
                pid,
                7, // wrong epoch
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                0,
            )
            .unwrap_err();
        assert!(matches!(err, TxnStateError::EpochFenced));
    }

    /// gh #225: `prepare_end_txn` must leave the dispatch set on the
    /// persisted entry. It is the only durable record that these
    /// markers still owe a write — clearing it early is what made a
    /// failed dispatch unrecoverable.
    #[test]
    fn prepare_retains_the_dispatch_set_until_complete() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1],
            }],
            100,
        )
        .unwrap();

        let out = s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(out.transition_fired);
        assert_eq!(out.partitions.len(), 1);
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::PrepareCommit);
        assert_eq!(
            snap["tx-1"].partitions.len(),
            1,
            "dispatch set must survive prepare"
        );

        s.complete_end_txn("tx-1", pid, epoch, true).unwrap();
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::CompleteCommit);
        assert!(snap["tx-1"].partitions.is_empty());
    }

    /// A retry after a failed dispatch must re-derive the *same*
    /// targets, not an empty set.
    #[test]
    fn prepare_is_re_drivable_with_the_same_dispatch_set() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        let first = s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        let second = s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(second.transition_fired);
        assert_eq!(first.partitions, second.partitions);
    }

    /// Prepared-to-commit cannot be flipped to an abort (and vice
    /// versa) — that would write the opposite marker for a txn whose
    /// COMMIT may already be on disk at some partition.
    #[test]
    fn prepare_rejects_the_opposite_outcome() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(matches!(
            s.prepare_end_txn("tx-1", pid, epoch, false),
            Err(TxnStateError::InvalidState)
        ));
        assert!(matches!(
            s.complete_end_txn("tx-1", pid, epoch, false),
            Err(TxnStateError::InvalidState)
        ));
    }

    /// Staged offsets must not become visible until the markers
    /// backing them are durable, so the hook fires on complete.
    #[test]
    fn offset_hook_fires_on_complete_not_on_prepare() {
        struct CapturingHook(parking_lot::Mutex<Vec<(String, i64, bool)>>);
        impl TxnOffsetHook for CapturingHook {
            fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
                self.0
                    .lock()
                    .push((group_id.to_owned(), producer_id, commit));
            }
        }
        let (_t, s) = store();
        let hook = Arc::new(CapturingHook(parking_lot::Mutex::new(Vec::new())));
        s.set_offset_hook(hook.clone());
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g1", 100).unwrap();

        s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(
            hook.0.lock().is_empty(),
            "prepare must not publish staged offsets"
        );

        s.complete_end_txn("tx-1", pid, epoch, true).unwrap();
        assert_eq!(hook.0.lock().len(), 1);
        assert_eq!(hook.0.lock()[0], ("g1".to_owned(), pid, true));
    }

    #[test]
    fn complete_without_prepare_is_invalid() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        // Still Ongoing — completing without preparing would clear the
        // dispatch set with no marker written.
        assert!(matches!(
            s.complete_end_txn("tx-1", pid, epoch, true),
            Err(TxnStateError::InvalidState)
        ));
    }

    #[test]
    fn end_txn_happy_commit_clears_partitions_and_fires_hook() {
        struct CapturingHook(parking_lot::Mutex<Vec<(String, i64, bool)>>);
        impl TxnOffsetHook for CapturingHook {
            fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
                self.0
                    .lock()
                    .push((group_id.to_owned(), producer_id, commit));
            }
        }
        let (_t, s) = store();
        let hook = Arc::new(CapturingHook(parking_lot::Mutex::new(Vec::new())));
        s.set_offset_hook(hook.clone());
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g1", 110).unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g2", 120).unwrap();
        s.end_txn("tx-1", pid, epoch, true).unwrap();

        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.state, TxnState::CompleteCommit);
        assert!(entry.partitions.is_empty());
        assert!(entry.groups.is_empty());
        assert_eq!(entry.ongoing_since_ms, 0);

        let fired = hook.0.lock().clone();
        assert_eq!(
            fired,
            vec![("g1".to_owned(), pid, true), ("g2".to_owned(), pid, true),]
        );
    }

    #[test]
    fn end_txn_idempotent_retry_returns_ok() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            0,
        )
        .unwrap();
        s.end_txn("tx-1", pid, epoch, true).unwrap();
        s.end_txn("tx-1", pid, epoch, true).unwrap(); // idempotent
        assert!(matches!(
            s.end_txn("tx-1", pid, epoch, false),
            Err(TxnStateError::InvalidState)
        ));
    }

    #[test]
    fn end_txn_against_empty_is_invalid() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        // No AddPartitions / AddOffsets — state stays Empty.
        let err = s.end_txn("tx-1", pid, epoch, true).unwrap_err();
        assert!(matches!(err, TxnStateError::InvalidState));
    }

    #[test]
    fn reaper_aborts_overdue_bumps_epoch_fires_hook() {
        struct CapturingHook(parking_lot::Mutex<Vec<(String, i64, bool)>>);
        impl TxnOffsetHook for CapturingHook {
            fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
                self.0
                    .lock()
                    .push((group_id.to_owned(), producer_id, commit));
            }
        }
        let (_t, s) = store();
        let hook = Arc::new(CapturingHook(parking_lot::Mutex::new(Vec::new())));
        s.set_offset_hook(hook.clone());
        let mut a = pid_alloc();
        let (pid, epoch) = s
            .get_or_allocate_with_timeout("tx-1", 1_000, || a())
            .unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            10_000,
        )
        .unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g1", 10_000)
            .unwrap();

        // Before the deadline — no abort.
        let aborted = s.abort_overdue(10_500);
        assert!(aborted.is_empty());

        // Past the deadline — single abort, epoch bumped.
        let aborted = s.abort_overdue(20_000);
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].pid, pid);
        assert_eq!(aborted[0].old_epoch, epoch);
        assert_eq!(aborted[0].new_epoch, epoch + 1);
        assert_eq!(aborted[0].groups, vec!["g1".to_owned()]);

        // gh #225: the reaper leaves the txn in PrepareAbort with its
        // dispatch set intact — a timed-out txn owes ABORT markers just
        // like a client-driven one. Completing here would drop the
        // partition list with nothing written and pin the LSO.
        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.state, TxnState::PrepareAbort);
        assert_eq!(entry.epoch, epoch + 1);
        assert_eq!(
            entry.partitions,
            vec![TxnTopic {
                topic: "t".into(),
                partitions: vec![0]
            }],
            "dispatch set must survive for the marker reconcile"
        );
        assert!(
            hook.0.lock().is_empty(),
            "offset hook fires on complete, not on the reaper's prepare"
        );

        // The reconcile finds it, and completing discards the staged
        // offsets. Note the *bumped* epoch is what completes it.
        let pending = s.pending_marker_dispatches(None);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].txn_id, "tx-1");
        assert_eq!(pending[0].pid, pid);
        assert_eq!(pending[0].epoch, epoch + 1);
        assert!(!pending[0].commit, "timed-out txn aborts");

        s.complete_end_txn("tx-1", pid, epoch + 1, false).unwrap();
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::CompleteAbort);
        assert!(snap["tx-1"].partitions.is_empty());
        let fired = hook.0.lock().clone();
        assert_eq!(fired, vec![("g1".to_owned(), pid, false)]);

        // Nothing left owing once completed.
        assert!(s.pending_marker_dispatches(None).is_empty());
    }

    #[test]
    fn reaper_owns_gate_filters() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s
            .get_or_allocate_with_timeout("tx-mine", 1_000, || a())
            .unwrap();
        s.add_partitions(
            "tx-mine",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            5_000,
        )
        .unwrap();
        let (pid2, e2) = s
            .get_or_allocate_with_timeout("tx-theirs", 1_000, || a())
            .unwrap();
        s.add_partitions(
            "tx-theirs",
            pid2,
            e2,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            5_000,
        )
        .unwrap();

        let mine_only = |id: &str| id == "tx-mine";
        let aborted = s.abort_overdue_owned(10_000, Some(&mine_only));
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].txn_id, "tx-mine");
    }

    #[test]
    fn slot_hashing_distributes_consistently() {
        let (_t, s) = store();
        // FNV-1a is deterministic — same input → same slot every call.
        assert_eq!(s.slot_for("tx-1"), s.slot_for("tx-1"));
        // Different inputs almost always land in different slots
        // (50 slots, FNV spread is dense enough for a small sample).
        let slots: Vec<usize> = (0..20).map(|i| s.slot_for(&format!("tx-{i}"))).collect();
        let distinct: std::collections::HashSet<_> = slots.iter().copied().collect();
        assert!(
            distinct.len() > 10,
            "expected reasonable spread, got {distinct:?}"
        );
    }

    /// Write `entry` for `txn_id` straight into its slot file.
    fn seed(s: &TxnStateStore, txn_id: &str, pid: i64, epoch: i16, state: TxnState) {
        let slot = s.slot_for(txn_id);
        let mut slot_state = s.load_slot(slot).unwrap();
        slot_state.insert(
            txn_id.to_owned(),
            TxnEntry {
                pid,
                epoch,
                partitions: vec![],
                ongoing_since_ms: 0,
                transaction_timeout_ms: 5_000,
                state,
                groups: vec![],
                last_pid: NO_PRODUCER_ID,
                last_epoch: NO_PRODUCER_EPOCH,
            },
        );
        s.persist_slot(slot, &slot_state).unwrap();
    }

    fn one_partition() -> Vec<TxnTopic> {
        vec![TxnTopic {
            topic: "t".into(),
            partitions: vec![0],
        }]
    }

    fn ready(outcome: InitPidOutcome) -> (i64, i16) {
        match outcome {
            InitPidOutcome::Ready { pid, epoch } => Some((pid, epoch)),
            InitPidOutcome::AbortingOngoing(_) => None,
        }
        .expect("expected Ready, got AbortingOngoing")
    }

    #[test]
    fn epoch_overflow_rotates_to_fresh_pid() {
        let (_t, s) = store();
        // Seed the slot file by hand at epoch = MAX so the next
        // get_or_allocate triggers rotation.
        seed(&s, "tx-1", 42, i16::MAX, TxnState::Empty);
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_ne!(pid, 42);
        assert_eq!(epoch, 0);
    }

    /// Apache's `isEpochExhausted` is `>= MAX - 1`: the last epoch is
    /// kept back for fencing an `Ongoing` transaction, which bumps
    /// without passing through the rotation check.
    #[test]
    fn epoch_exhaustion_rotates_one_short_of_max() {
        let (_t, s) = store();
        seed(&s, "tx-1", 42, i16::MAX - 1, TxnState::CompleteCommit);
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_ne!(pid, 42);
        assert_eq!(epoch, 0);
    }

    /// gh #201: a producer re-initialising under its own identity
    /// (KIP-360) bumps once, and a retry of that request — its response
    /// lost — gets the same answer rather than a second bump or a fence.
    #[test]
    fn a_claimed_bump_is_retry_safe() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, e0) = s.get_or_allocate("tx-1", || a()).unwrap();

        let first = ready(
            s.init_producer_id("tx-1", 0, Some((pid, e0)), || a())
                .unwrap(),
        );
        assert_eq!(first, (pid, e0 + 1));
        let retry = ready(
            s.init_producer_id("tx-1", 0, Some((pid, e0)), || a())
                .unwrap(),
        );
        assert_eq!(retry, first, "a retried bump must not bump again");

        let next = ready(
            s.init_producer_id("tx-1", 0, Some((pid, e0 + 1)), || a())
                .unwrap(),
        );
        assert_eq!(next, (pid, e0 + 2));
    }

    /// gh #201, the headline: once a newer instance has initialised, the
    /// old session's claim is stale and must be fenced — not handed a
    /// fresh epoch that fences the live session in turn.
    #[test]
    fn a_stale_claim_is_fenced_not_bumped() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, old) = s.get_or_allocate("tx-1", || a()).unwrap();
        // New instance (no claim) takes over.
        let (_, live) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(live, old + 1);

        assert!(matches!(
            s.init_producer_id("tx-1", 0, Some((pid, old)), || a()),
            Err(TxnStateError::EpochFenced)
        ));
        // A claim that is ahead of the coordinator is just as stale.
        assert!(matches!(
            s.init_producer_id("tx-1", 0, Some((pid, live + 5)), || a()),
            Err(TxnStateError::EpochFenced)
        ));
        assert_eq!(
            s.snapshot()["tx-1"].epoch,
            live,
            "a fenced claim must not move the epoch"
        );
    }

    #[test]
    fn a_claim_for_another_pid_is_fenced() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert!(matches!(
            s.init_producer_id("tx-1", 0, Some((pid + 1, epoch)), || a()),
            Err(TxnStateError::EpochFenced)
        ));
    }

    /// No entry is Apache's "metadata just created": a producer that
    /// lost its coordinator state (UNKNOWN_PRODUCER_ID) still gets an
    /// identity, whatever it claims.
    #[test]
    fn an_unknown_txn_id_accepts_any_claim() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = ready(
            s.init_producer_id("tx-1", 0, Some((999, 7)), || a())
                .unwrap(),
        );
        assert_ne!(pid, 999);
        assert_eq!(epoch, 0);
    }

    /// gh #201's second half. A rejoin that finds an `Ongoing`
    /// transaction must abort it — before the fix it bumped and left the
    /// entry `Ongoing` with the old session's partitions, so the new
    /// session's first commit committed the zombie's records as well.
    #[test]
    fn a_rejoin_mid_ongoing_prepares_an_abort_at_a_bumped_epoch() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions("tx-1", pid, epoch, &one_partition(), 100)
            .unwrap();

        let abort = match s.init_producer_id("tx-1", 0, None, || a()).unwrap() {
            InitPidOutcome::AbortingOngoing(d) => Some(d),
            InitPidOutcome::Ready { .. } => None,
        }
        .expect("expected AbortingOngoing, got Ready");
        assert_eq!(abort.pid, pid);
        assert_eq!(abort.epoch, epoch + 1);
        assert!(!abort.commit);
        assert_eq!(abort.partitions, one_partition());

        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::PrepareAbort);
        assert_eq!(snap["tx-1"].ongoing_since_ms, 0);
        assert_eq!(
            snap["tx-1"].partitions,
            one_partition(),
            "dispatch set must survive for the markers (gh #225)"
        );
        // The marker reconcile can finish it if the caller doesn't.
        assert_eq!(s.pending_marker_dispatches(None).len(), 1);

        // The old session is fenced the moment the abort is prepared.
        assert!(matches!(
            s.add_partitions("tx-1", pid, epoch, &one_partition(), 200),
            Err(TxnStateError::EpochFenced)
        ));
        // Markers still in flight: retry later.
        assert!(matches!(
            s.init_producer_id("tx-1", 0, None, || a()),
            Err(TxnStateError::Concurrent)
        ));

        s.complete_end_txn("tx-1", pid, epoch + 1, false).unwrap();
        // The retry lands on a clean entry, bumped a second time, so
        // the fence epoch itself is never handed out.
        let (p, e) = ready(s.init_producer_id("tx-1", 0, None, || a()).unwrap());
        assert_eq!((p, e), (pid, epoch + 2));
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::Empty);
        assert!(snap["tx-1"].partitions.is_empty());
    }

    /// Apache resets `lastProducerEpoch` when it fences, so a claim
    /// from the session whose transaction was just aborted is stale on
    /// its retry.
    #[test]
    fn the_session_an_abort_fenced_cannot_reclaim_its_epoch() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions("tx-1", pid, epoch, &one_partition(), 100)
            .unwrap();
        assert!(matches!(
            s.init_producer_id("tx-1", 0, Some((pid, epoch)), || a()),
            Ok(InitPidOutcome::AbortingOngoing(_))
        ));
        s.complete_end_txn("tx-1", pid, epoch + 1, false).unwrap();
        assert!(matches!(
            s.init_producer_id("tx-1", 0, Some((pid, epoch)), || a()),
            Err(TxnStateError::EpochFenced)
        ));
    }

    #[test]
    fn a_rejoin_during_prepare_is_concurrent() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions("tx-1", pid, epoch, &one_partition(), 100)
            .unwrap();
        s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(matches!(
            s.init_producer_id("tx-1", 0, None, || a()),
            Err(TxnStateError::Concurrent)
        ));
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::PrepareCommit);
        assert_eq!(snap["tx-1"].epoch, epoch, "commit markers keep their epoch");
    }

    /// KIP-360 rotation: a producer that exhausted its epoch and never
    /// saw the rotated identity retries with its old one, and must get
    /// the rotated identity again rather than a fence or a second PID.
    #[test]
    fn a_retried_rotation_gets_the_same_answer() {
        let (_t, s) = store();
        seed(&s, "tx-1", 42, i16::MAX - 1, TxnState::CompleteCommit);
        let mut a = pid_alloc();
        let rotated = ready(
            s.init_producer_id("tx-1", 0, Some((42, i16::MAX - 1)), || a())
                .unwrap(),
        );
        assert_ne!(rotated.0, 42);
        assert_eq!(rotated.1, 0);
        let retry = ready(
            s.init_producer_id("tx-1", 0, Some((42, i16::MAX - 1)), || a())
                .unwrap(),
        );
        assert_eq!(retry, rotated);
    }

    /// The old PID is honoured only until the rotated session moves
    /// on. Apache overwrites `lastProducerId` on every later
    /// transition; without that, the old session's claim would abort
    /// the new session's live transaction.
    #[test]
    fn the_pre_rotation_pid_is_stale_once_the_new_session_moves() {
        let (_t, s) = store();
        seed(&s, "tx-1", 42, i16::MAX - 1, TxnState::CompleteCommit);
        let mut a = pid_alloc();
        let (pid, epoch) = ready(s.init_producer_id("tx-1", 0, None, || a()).unwrap());
        s.add_partitions("tx-1", pid, epoch, &one_partition(), 100)
            .unwrap();
        assert!(matches!(
            s.init_producer_id("tx-1", 0, Some((42, i16::MAX - 1)), || a()),
            Err(TxnStateError::EpochFenced)
        ));
        assert_eq!(s.snapshot()["tx-1"].state, TxnState::Ongoing);
    }

    /// Slot files written before gh #201 have no `lastPid` /
    /// `lastEpoch`; they must read as "none", and an entry without
    /// either must not grow the fields on disk.
    #[test]
    fn last_identity_fields_default_to_none() {
        let entry: TxnEntry = serde_json::from_str(r#"{"pid":7,"epoch":3}"#).unwrap();
        assert_eq!(entry.last_pid, NO_PRODUCER_ID);
        assert_eq!(entry.last_epoch, NO_PRODUCER_EPOCH);
        assert_eq!(
            serde_json::to_string(&entry).unwrap(),
            r#"{"pid":7,"epoch":3}"#
        );
    }

    #[test]
    fn persistence_round_trip_across_open() {
        let tmp = tempfile::tempdir().unwrap();
        let mut a = pid_alloc();
        let pid;
        {
            let s = TxnStateStore::open(tmp.path(), DEFAULT_NUM_SLOTS).unwrap();
            let (p, _e) = s.get_or_allocate("tx-1", || a()).unwrap();
            pid = p;
            s.add_partitions(
                "tx-1",
                p,
                0,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0, 1],
                }],
                100,
            )
            .unwrap();
        }
        // Reopen: same on-disk state surfaces.
        let s2 = TxnStateStore::open(tmp.path(), DEFAULT_NUM_SLOTS).unwrap();
        let snap = s2.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.pid, pid);
        assert_eq!(entry.epoch, 0);
        assert_eq!(entry.state, TxnState::Ongoing);
        assert_eq!(
            entry.partitions,
            vec![TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1]
            }]
        );
    }

    #[test]
    fn add_partitions_concurrent_transition_rejected() {
        let (_t, s) = store();
        // Seed Prepare* state directly to test the rejection arm.
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        {
            let slot = s.slot_for("tx-1");
            let mut state = s.load_slot(slot).unwrap();
            let entry = state.get_mut("tx-1").unwrap();
            entry.state = TxnState::PrepareCommit;
            s.persist_slot(slot, &state).unwrap();
        }
        let err = s
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                0,
            )
            .unwrap_err();
        assert!(matches!(err, TxnStateError::Concurrent));
    }
}
