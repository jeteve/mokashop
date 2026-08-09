use std::{
    io::Cursor,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use openraft::{
    AppData, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use tokio::sync::RwLock;

// Example there: https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L79

// What defines the state at the application level.
// This what raft snapshots between nodes.
// AppData can be accessed in a Multiple Reader, Single writer mode.
//
struct RaftData<C: RaftTypeConfig, AppData> {
    pub last_applied_log: Option<LogId<C::NodeId>>,
    pub last_membership: StoredMembership<C::NodeId, C::Node>,
    pub app_data: AppData,
}

// A snapshot of the RaftData
#[derive(Debug)]
pub struct StoredSnapshot<C: RaftTypeConfig> {
    pub meta: openraft::SnapshotMeta<C::NodeId, C::Node>,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

// See example there:
// https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L79
pub struct StateMachine<C: RaftTypeConfig, AppData> {
    raft_data: RwLock<RaftData<C, AppData>>, // Tokyo RwLock
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot<C>>>,
}

pub struct StateMachineArc<C: RaftTypeConfig, AppData> {
    inner: Arc<StateMachine<C, AppData>>,
    snapshot_data_hander: fn(Vec<u8>) -> C::SnapshotData,
}

impl<C: RaftTypeConfig, AppData: Send + Sync + 'static + serde::Serialize> RaftSnapshotBuilder<C>
    for StateMachineArc<C, AppData>
where
    C::NodeId: Copy,
{
    #[doc = " Build snapshot"]
    #[doc = ""]
    #[doc = " A snapshot has to contain state of all applied log, including membership. Usually it is just"]
    #[doc = " a serialized state machine."]
    #[doc = ""]
    #[doc = " Building snapshot can be done by:"]
    #[doc = " - Performing log compaction, e.g. merge log entries that operates on the same key, like a"]
    #[doc = "   LSM-tree does,"]
    #[doc = " - or by fetching a snapshot from the state machine."]
    async fn build_snapshot(
        &mut self,
    ) -> Result<openraft::storage::Snapshot<C>, StorageError<C::NodeId>> {
        let raft_data = self.inner.raft_data.read().await;

        let app_data = serde_json::to_vec::<AppData>(&raft_data.app_data)
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = raft_data.last_applied_log;
        let last_membership = raft_data.last_membership.clone();

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.inner.current_snapshot.write().await;
        // We have the raft data copy. Can drop the lock.
        drop(raft_data);

        let snapshot_idx = self.inner.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: app_data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(openraft::Snapshot {
            meta,
            snapshot: Box::new((self.snapshot_data_hander)(app_data)),
            // This cannot be concrete, as this depends on the C::SnapShotData generic type.
            //snapshot: Box::new(Cursor::new(app_data)),
        })
    }
}
