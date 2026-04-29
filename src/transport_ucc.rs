use std::sync::Arc;

use crate::cluster::Membership;
use crate::error::{BcError, Result};

#[allow(non_camel_case_types, non_snake_case, non_upper_case_globals, dead_code)]
pub mod sys {
    pub use ucc_sys::*;
}

/// Handle to an initialized UCC library + context + team that spans the
/// gossip cluster, exposed as a single allgatherv-friendly collective.
///
/// Construction is async because the underlying `ucc_team_create_post`
/// call needs an out-of-band exchange of UCC team addresses across the
/// cluster; we drive that exchange through the existing gossip layer
/// instead of pulling in MPI.
pub struct UccCollectives {
    rank: u32,
    world: u32,
}

impl UccCollectives {
    pub async fn new(_rank: u32, _world: u32, _membership: Arc<Membership>) -> Result<Self> {
        Err(BcError::Other(
            "transport_ucc: UccCollectives::new not yet implemented".into(),
        ))
    }

    pub fn rank(&self) -> u32 {
        self.rank
    }

    pub fn world(&self) -> u32 {
        self.world
    }

    pub fn allgatherv(
        &self,
        _send: &[u8],
        _recv: &mut [u8],
        _counts: &[usize],
        _displs: &[usize],
    ) -> Result<()> {
        Err(BcError::Other(
            "transport_ucc: allgatherv not yet implemented".into(),
        ))
    }
}
