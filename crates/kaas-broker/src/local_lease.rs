//! Single-broker stand-in for the cluster lease manager.
//!
//! Pins the epoch at 0.
//! Phase 5 swaps this for the real `Coordinator` that reads
//! `assignment.json` off the shared NFS volume.

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalLeaseManager;

impl LocalLeaseManager {
    pub fn current_epoch(&self) -> u32 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_is_zero() {
        assert_eq!(LocalLeaseManager.current_epoch(), 0);
    }
}
