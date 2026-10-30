//! Cluster assignment — the `assignment.json` schema.
//!
//! `<data_dir>/__cluster/assignment.json` is the authoritative cluster
//! state: which broker leads which partition (and serves which
//! consumer group) at which epoch. The controller broker is the only
//! writer; every other broker is a reader and observes changes via
//! [`Coordinator`].
//!
//! Serde shape is pinned byte-for-byte (`camelCase` field
//! names, `RFC3339Nano` timestamp encoding) so files written by any
//! release — v0.1 included — decode cleanly under any other.
//!
//! [`Coordinator`]: crate::coordinator::Coordinator

use serde::{Deserialize, Serialize};

/// Health of one broker, as recorded by the controller.
///
/// The hash routing in [`crate::group_hash::pick_coordinator`] treats
/// only [`BrokerHealth::Alive`] as "available to coordinate";
/// `Draining` brokers fall through to the deterministic alternate
/// alive broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BrokerHealth {
    Alive,
    Draining,
    Dead,
}

/// Role a broker plays for a partition. Kaas is single-writer-per-
/// partition so the only role today is `Leader`; the field is kept
/// for forward compatibility with a v2 replicated extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionRole {
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerAssignment {
    pub id: String,
    pub health: BrokerHealth,
    /// RFC3339Nano timestamp. Stored as a string so
    /// cross-release timestamp byte equality is exact — `chrono` / `time` add
    /// trailing-zero variance.
    pub last_seen: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionAssignment {
    pub topic: String,
    pub partition: i32,
    pub broker: String,
    pub epoch: u32,
    pub role: PartitionRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupAssignment {
    pub group_id: String,
    pub broker: String,
    pub epoch: u32,
}

/// Cluster-wide authoritative assignment, persisted under
/// `__cluster/assignment.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub controller_epoch: i64,
    pub assignment_version: i64,
    /// RFC3339Nano. See [`BrokerAssignment::last_seen`].
    pub generated_at: String,
    pub controller: String,
    pub brokers: Vec<BrokerAssignment>,
    pub partitions: Vec<PartitionAssignment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consumer_groups: Vec<ConsumerGroupAssignment>,
}

impl Assignment {
    /// File name relative to the `__cluster/` directory. The shared
    /// directory layout puts it at
    /// `<data_dir>/__cluster/assignment.json`.
    pub const FILE_NAME: &'static str = "assignment.json";

    /// `(brokers, alive)` views: the full ordered set of brokers
    /// and a map from broker id → alive (only `Alive` health counts).
    /// `Draining` is treated as down so fresh traffic doesn't route
    /// to a broker that's winding down.
    pub fn broker_sets(&self) -> (Vec<String>, std::collections::HashMap<String, bool>) {
        let mut brokers = Vec::with_capacity(self.brokers.len());
        let mut alive = std::collections::HashMap::with_capacity(self.brokers.len());
        for b in &self.brokers {
            brokers.push(b.id.clone());
            alive.insert(b.id.clone(), matches!(b.health, BrokerHealth::Alive));
        }
        (brokers, alive)
    }
}

/// `(prev, next)` handler signature fired by
/// [`Coordinator::on_assignment_change`]. Pre-change `prev` is `None`
/// on the very first apply.
///
/// [`Coordinator::on_assignment_change`]:
///     crate::coordinator::Coordinator::on_assignment_change
pub type AssignmentChangeHandler =
    std::sync::Arc<dyn Fn(Option<&Assignment>, &Assignment) + Send + Sync + 'static>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip_matches_go_shape() {
        let a = Assignment {
            controller_epoch: 1,
            assignment_version: 7,
            generated_at: "2025-01-02T03:04:05.123456789Z".to_owned(),
            controller: "kaas-0".to_owned(),
            brokers: vec![BrokerAssignment {
                id: "kaas-0".to_owned(),
                health: BrokerHealth::Alive,
                last_seen: "2025-01-02T03:04:05.123456789Z".to_owned(),
            }],
            partitions: vec![PartitionAssignment {
                topic: "t1".to_owned(),
                partition: 0,
                broker: "kaas-0".to_owned(),
                epoch: 1,
                role: PartitionRole::Leader,
            }],
            consumer_groups: vec![ConsumerGroupAssignment {
                group_id: "g1".to_owned(),
                broker: "kaas-0".to_owned(),
                epoch: 1,
            }],
        };
        let json = serde_json::to_string(&a).unwrap();
        // Spot-check the camelCase + lowercase enum encodings.
        assert!(json.contains("\"controllerEpoch\":1"));
        assert!(json.contains("\"assignmentVersion\":7"));
        assert!(json.contains("\"health\":\"alive\""));
        assert!(json.contains("\"role\":\"leader\""));
        let back: Assignment = serde_json::from_str(&json).unwrap();
        assert_eq!(back, a);
    }

    #[test]
    fn consumer_groups_omitted_when_empty() {
        let a = Assignment {
            controller_epoch: 1,
            assignment_version: 1,
            generated_at: "x".to_owned(),
            controller: "kaas-0".to_owned(),
            brokers: vec![],
            partitions: vec![],
            consumer_groups: vec![],
        };
        let json = serde_json::to_string(&a).unwrap();
        assert!(
            !json.contains("consumerGroups"),
            "empty consumerGroups should be omitted, got: {json}"
        );
    }

    #[test]
    fn broker_sets_treats_draining_as_not_alive() {
        let a = Assignment {
            controller_epoch: 1,
            assignment_version: 1,
            generated_at: "x".to_owned(),
            controller: "kaas-0".to_owned(),
            brokers: vec![
                BrokerAssignment {
                    id: "kaas-0".to_owned(),
                    health: BrokerHealth::Alive,
                    last_seen: "x".to_owned(),
                },
                BrokerAssignment {
                    id: "kaas-1".to_owned(),
                    health: BrokerHealth::Draining,
                    last_seen: "x".to_owned(),
                },
                BrokerAssignment {
                    id: "kaas-2".to_owned(),
                    health: BrokerHealth::Dead,
                    last_seen: "x".to_owned(),
                },
            ],
            partitions: vec![],
            consumer_groups: vec![],
        };
        let (brokers, alive) = a.broker_sets();
        assert_eq!(brokers, vec!["kaas-0", "kaas-1", "kaas-2"]);
        assert!(alive["kaas-0"]);
        assert!(!alive["kaas-1"]);
        assert!(!alive["kaas-2"]);
    }

    /// gh #249 end-to-end: the tri-state exists so that losing a
    /// broker doesn't rehash every coordinator. This is the payoff
    /// the `group_hash` divisor rule promises, exercised through the
    /// real `broker_sets` → `pick_coordinator` chain — a path that
    /// never ran against a non-`Alive` entry before, because nothing
    /// produced one.
    #[test]
    fn a_dead_broker_moves_only_its_own_groups() {
        use crate::group_hash::pick_group_coordinator;

        let entry = |id: &str, health| BrokerAssignment {
            id: id.to_owned(),
            health,
            last_seen: "x".to_owned(),
        };
        let assignment = |third_health| Assignment {
            controller_epoch: 1,
            assignment_version: 1,
            generated_at: "x".to_owned(),
            controller: "kaas-0".to_owned(),
            brokers: vec![
                entry("kaas-0", BrokerHealth::Alive),
                entry("kaas-1", BrokerHealth::Alive),
                entry("kaas-2", third_health),
            ],
            partitions: vec![],
            consumer_groups: vec![],
        };

        let (b_ok, alive_ok) = assignment(BrokerHealth::Alive).broker_sets();
        let (b_dead, alive_dead) = assignment(BrokerHealth::Dead).broker_sets();

        let mut moved = 0;
        for i in 0..200 {
            let g = format!("group-{i}");
            let before = pick_group_coordinator(&g, &b_ok, &alive_ok).unwrap();
            let after = pick_group_coordinator(&g, &b_dead, &alive_dead).unwrap();
            if before != after {
                moved += 1;
                // Only groups that lived on the dead broker move, and
                // they never move back onto it.
                assert_eq!(before, "kaas-2");
                assert_ne!(after, "kaas-2");
            }
        }
        // ~1/3 of groups hashed to kaas-2; the other ~2/3 must not
        // have budged. Dropping the dead broker from the list instead
        // would have rehashed ~2/3 of everything.
        assert!(
            (40..=90).contains(&moved),
            "expected only kaas-2's share to move, got {moved}/200"
        );
    }
}
