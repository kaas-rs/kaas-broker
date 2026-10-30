//! Broker-side bidi `ControllerHeartbeat` client.
//!
//! One
//! client per broker process; long-lived; reconnects on disconnect
//! with exponential backoff capped at 5 s.
//!
//! Two seams the rest of the binary consumes:
//!
//! - [`HeartbeatClient::send`] pushes a `BrokerStatus` upstream.
//!   The Phase-5 plan calls for the broker to invoke this every
//!   ~1 s with its current ownership view + `active_groups`.
//! - [`HeartbeatClient::last_received`] returns the wall-clock
//!   tokio [`Instant`] of the most recent message from the
//!   controller. The struct satisfies
//!   [`crate::coordinator::HeartbeatSource`], so wiring it onto the
//!   `Coordinator` lights up the gh #62 self-fence (produce on a
//!   stale heartbeat surfaces `NOT_LEADER_FOR_PARTITION`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::coordinator::HeartbeatSource;
use crate::heartbeatpb::controller_heartbeat_client::ControllerHeartbeatClient;
use crate::heartbeatpb::{BrokerStatus, ControllerCommand};

/// Error returned by [`HeartbeatClient::send`].
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("heartbeat: not connected")]
    Disconnected,
    #[error("heartbeat: outbox full")]
    OutboxFull,
}

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(5);
const SEND_BUFFER: usize = 32;

/// Callback invoked per `ControllerCommand`. Runs on the
/// heartbeat-client's recv task; must not block on heavy work.
pub type CommandHandler = Arc<dyn Fn(&ControllerCommand) + Send + Sync + 'static>;

/// Builder + resolver for the heartbeat client's dial target. In
/// production the broker passes a closure that reads
/// [`crate::coordinator::Coordinator`]'s current snapshot to pick
/// the controller-broker's heartbeat endpoint; `None` from the
/// resolver triggers a backoff retry (boot-time, no controller
/// known yet).
pub type TargetResolver = Arc<dyn Fn() -> Option<String> + Send + Sync + 'static>;

/// Broker-side heartbeat client.
pub struct HeartbeatClient {
    broker_id: String,
    target: Mutex<Option<String>>,
    target_fn: Mutex<Option<TargetResolver>>,
    on_command: Mutex<Option<CommandHandler>>,
    /// Outbox tx — `Send` writes through this; the run-loop's
    /// recv side drains it onto the active stream. `None` between
    /// reconnect cycles.
    outbox: Mutex<Option<mpsc::Sender<BrokerStatus>>>,
    /// Wall-clock (tokio `Instant`) ms of the last received command
    /// — encoded as `i64` for atomic storage. `0` ↔ "never". We
    /// stash the duration since a process-relative reference point
    /// computed at construction so a relative `Instant` survives the
    /// `Atomic` round trip.
    last_received_ms: AtomicI64,
    reference_instant: Instant,
}

impl std::fmt::Debug for HeartbeatClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeartbeatClient")
            .field("broker_id", &self.broker_id)
            .field("connected", &self.outbox.lock().is_some())
            .finish()
    }
}

impl HeartbeatClient {
    pub fn new(broker_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            broker_id: broker_id.into(),
            target: Mutex::new(None),
            target_fn: Mutex::new(None),
            on_command: Mutex::new(None),
            outbox: Mutex::new(None),
            last_received_ms: AtomicI64::new(0),
            reference_instant: Instant::now(),
        })
    }

    /// Set the static dial target — typically `"host:port"`. Tests
    /// use this; production calls [`Self::with_target_fn`] instead
    /// so the client follows controller failover.
    pub fn set_target(&self, target: impl Into<String>) {
        *self.target.lock() = Some(target.into());
    }

    /// Install a dynamic resolver invoked at the start of every
    /// reconnect cycle. A `None` return triggers a backoff retry
    /// (no dial) — useful at boot when the controller-watcher
    /// hasn't seen the Lease holder yet.
    pub fn with_target_fn(self: &Arc<Self>, f: TargetResolver) -> Arc<Self> {
        *self.target_fn.lock() = Some(f);
        self.clone()
    }

    /// Register the command handler. Replaces any prior handler.
    pub fn on_command(&self, h: CommandHandler) {
        *self.on_command.lock() = Some(h);
    }

    fn resolve_target(&self) -> Option<String> {
        if let Some(f) = self.target_fn.lock().as_ref() {
            return f();
        }
        self.target.lock().clone()
    }

    /// `BrokerStatus` push. Returns `Err(SendError::Disconnected)`
    /// when no stream is connected (the caller's tick should
    /// swallow it — a reconnect is already in progress) or
    /// `Err(SendError::OutboxFull)` when the channel is full
    /// (slow controller).
    pub fn send(&self, mut status: BrokerStatus) -> Result<(), SendError> {
        if status.broker_id.is_empty() {
            status.broker_id = self.broker_id.clone();
        }
        let outbox = self.outbox.lock().clone();
        match outbox {
            None => Err(SendError::Disconnected),
            Some(tx) => tx.try_send(status).map_err(|_| SendError::OutboxFull),
        }
    }

    fn record_received(&self) {
        let ms = self
            .reference_instant
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(i64::MAX);
        self.last_received_ms.store(ms.max(1), Ordering::Relaxed);
    }

    /// Long-running task: maintain a long-lived bidi stream with
    /// exponential backoff on disconnect. Returns when `cancel`
    /// fires.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut backoff = RECONNECT_INITIAL_BACKOFF;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let target = match self.resolve_target() {
                Some(t) if !t.is_empty() => t,
                _ => {
                    debug!("heartbeat: no controller target yet");
                    if Self::sleep_or_cancel(backoff, &cancel).await {
                        return;
                    }
                    backoff = backoff_next(backoff);
                    continue;
                }
            };
            match self.run_once(target, cancel.clone()).await {
                Ok(()) => return, // cancelled cleanly
                Err(err) => {
                    debug!(%err, "heartbeat: stream closed; will reconnect");
                    if Self::sleep_or_cancel(backoff, &cancel).await {
                        return;
                    }
                    backoff = backoff_next(backoff);
                }
            }
        }
    }

    async fn sleep_or_cancel(d: Duration, cancel: &CancellationToken) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(d) => false,
            _ = cancel.cancelled() => true,
        }
    }

    async fn run_once(
        self: &Arc<Self>,
        target: String,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        // tonic 0.12: prefix with http://. The endpoint helper is
        // happy with either form when the channel is built via
        // Endpoint::from_shared.
        let endpoint = if target.contains("://") {
            target.clone()
        } else {
            format!("http://{target}")
        };
        // Bounded connect + h2 keepalive: a controller pod replaced
        // mid-stream leaves a silently-dead TCP connection (no FIN /
        // RST reaches us), and without keepalive the recv side can
        // hang forever — observed live as a broker stranded out of
        // the alive set until manually bounced.
        let channel = tonic::transport::Channel::from_shared(endpoint)?
            .connect_timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(5))
            .keep_alive_timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true)
            .connect()
            .await?;
        let mut client = ControllerHeartbeatClient::new(channel);

        // Build the outbox channel. The initial `BrokerStatus`
        // carrying broker_id is queued before we open the stream
        // so the controller has a valid first message.
        let (tx, rx) = mpsc::channel::<BrokerStatus>(SEND_BUFFER);
        let initial = BrokerStatus {
            broker_id: self.broker_id.clone(),
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            last_seen_assignment_version: 0,
            partitions: Vec::new(),
            active_groups: Vec::new(),
            // gh #208: the controller registers this broker's health
            // from the FIRST message, so carry it here rather than
            // wait for the pump's first tick.
            healthy: kaas_observability::main_alive(),
            draining: kaas_observability::is_draining(),
        };
        let _ = tx.try_send(initial);
        *self.outbox.lock() = Some(tx);

        // Drive the outbound stream from the receiver.
        let outbound = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response = client.stream(outbound).await?;
        let mut inbound = response.into_inner();

        let me = self.clone();
        let result = tokio::select! {
            _ = cancel.cancelled() => Ok(()),
            r = me.recv_loop(&mut inbound) => r,
        };
        *self.outbox.lock() = None;
        result
    }

    async fn recv_loop(
        &self,
        inbound: &mut tonic::Streaming<ControllerCommand>,
    ) -> anyhow::Result<()> {
        // The controller PINGs every 1 s — sustained silence means
        // the stream is dead even if the transport hasn't noticed.
        // Belt-and-braces with the channel's h2 keepalive. 30 s (not
        // lower): a controller briefly starved by takeover I/O must
        // not have its whole broker set torn down and rebalanced,
        // which is itself what feeds the next starvation.
        const READ_TIMEOUT: Duration = Duration::from_secs(30);
        loop {
            let msg = match tokio::time::timeout(READ_TIMEOUT, inbound.message()).await {
                Ok(next) => next?,
                Err(_elapsed) => {
                    kaas_observability::metrics::global()
                        .heartbeat_misses
                        .add(1, &[]);
                    return Err(anyhow::anyhow!(
                        "heartbeat: no controller traffic for {READ_TIMEOUT:?}; reconnecting"
                    ));
                }
            };
            let Some(msg) = msg else { break };
            self.record_received();
            if let Some(h) = self.on_command.lock().as_ref() {
                h(&msg);
            }
        }
        // Stream closed by the peer — either the controller went
        // away or the network dropped. Alerting reads this via
        // `heartbeat.misses`; a healthy client only bumps it on
        // failover cycles.
        kaas_observability::metrics::global()
            .heartbeat_misses
            .add(1, &[]);
        Err(anyhow::anyhow!("heartbeat: server closed stream"))
    }
}

impl HeartbeatSource for HeartbeatClient {
    fn last_received(&self) -> Option<Instant> {
        let ms = self.last_received_ms.load(Ordering::Relaxed);
        if ms == 0 {
            None
        } else {
            let ms_u64 = u64::try_from(ms).unwrap_or(0);
            Some(self.reference_instant + Duration::from_millis(ms_u64))
        }
    }
}

fn backoff_next(current: Duration) -> Duration {
    let next = current * 2;
    if next > RECONNECT_MAX_BACKOFF {
        RECONNECT_MAX_BACKOFF
    } else {
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_client_has_received_nothing() {
        let c = HeartbeatClient::new("kaas-0");
        assert_eq!(c.last_received(), None);
    }

    #[test]
    fn send_without_stream_returns_err() {
        let c = HeartbeatClient::new("kaas-0");
        let r = c.send(BrokerStatus {
            broker_id: String::new(),
            timestamp_ms: 0,
            last_seen_assignment_version: 0,
            partitions: Vec::new(),
            active_groups: Vec::new(),
            healthy: false,
            draining: false,
        });
        assert!(r.is_err());
    }

    #[test]
    fn target_resolver_overrides_static_target() {
        let c = HeartbeatClient::new("kaas-0");
        c.set_target("static:9094");
        c.with_target_fn(Arc::new(|| Some("dynamic:9095".to_owned())));
        assert_eq!(c.resolve_target().as_deref(), Some("dynamic:9095"));
    }

    #[test]
    fn record_received_advances_last_received() {
        let c = HeartbeatClient::new("kaas-0");
        assert_eq!(c.last_received(), None);
        c.record_received();
        assert!(c.last_received().is_some());
    }
}
