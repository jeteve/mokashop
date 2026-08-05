use std::sync::{Arc, atomic::AtomicU64};

use openraft::{LogId, RaftTypeConfig, StoredMembership};
use tokio::sync::RwLock;

// Example there: https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L79

// What defines the state at the application level.
// This what raft snapshots between nodes.
// AppData can be accessed in a Multiple Reader, Single writer mode.
//
struct RaftData<C: RaftTypeConfig, AppData> {
    last_applied_log: LogId<C::NodeId>,
    last_membership: StoredMembership<C::NodeId, C::Node>,
    app_data: AppData,
}

// A snapshot of the RaftData
#[derive(Debug)]
pub struct Snapshot<C: RaftTypeConfig> {
    pub meta: openraft::SnapshotMeta<C::NodeId, C::Node>,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

// See example there:
// https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L79
pub struct StateMachine<C: RaftTypeConfig, AppData> {
    raft_data: RwLock<RaftData<C, AppData>>, // Tokyo RwLock
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<Snapshot<C>>>,
}
