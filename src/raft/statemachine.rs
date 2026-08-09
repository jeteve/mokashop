use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use openraft::{
    LogId, OptionalSend, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, storage::RaftStateMachine,
};
use serde::Serialize;
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

#[derive(Clone)]
pub struct StateMachineArc<C: RaftTypeConfig, AppData> {
    inner: Arc<StateMachine<C, AppData>>,
    snapshot_data_handle: fn(Vec<u8>) -> C::SnapshotData,
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
            snapshot: Box::new((self.snapshot_data_handle)(app_data)),
            // This cannot be concrete, as this depends on the C::SnapShotData generic type.
            //snapshot: Box::new(Cursor::new(app_data)),
        })
    }
}

impl<C: RaftTypeConfig, AppData: Send + Sync + 'static> RaftStateMachine<C>
    for StateMachineArc<C, AppData>
where
    AppData: Serialize,
    C::NodeId: Copy,
{
    #[doc = " Snapshot builder type."]
    type SnapshotBuilder = Self;

    #[doc = " Returns the last applied log id which is recorded in state machine, and the last applied"]
    #[doc = " membership config."]
    #[doc = ""]
    #[doc = " ### Correctness requirements"]
    #[doc = ""]
    #[doc = " It is all right to return a membership with greater log id than the"]
    #[doc = " last-applied-log-id."]
    #[doc = " Because upon startup, the last membership will be loaded by scanning logs from the"]
    #[doc = " `last-applied-log-id`."]
    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<C::NodeId>>,
            StoredMembership<C::NodeId, C::Node>,
        ),
        StorageError<C::NodeId>,
    > {
        todo!()
    }

    #[doc = " Apply the given payload of entries to the state machine."]
    #[doc = ""]
    #[doc = " The Raft protocol guarantees that only logs which have been _committed_, that is, logs which"]
    #[doc = " have been replicated to a quorum of the cluster, will be applied to the state machine."]
    #[doc = ""]
    #[doc = " This is where the business logic of interacting with your application\'s state machine"]
    #[doc = " should live. This is 100% application specific. Perhaps this is where an application"]
    #[doc = " specific transaction is being started, or perhaps committed. This may be where a key/value"]
    #[doc = " is being stored."]
    #[doc = ""]
    #[doc = " For every entry to apply, an implementation should:"]
    #[doc = " - Store the log id as last applied log id."]
    #[doc = " - Deal with the business logic log."]
    #[doc = " - Store membership config if `RaftEntry::get_membership()` returns `Some`."]
    #[doc = ""]
    #[doc = " Note that for a membership log, the implementation need to do nothing about it, except"]
    #[doc = " storing it."]
    #[doc = ""]
    #[doc = " An implementation may choose to persist either the state machine or the snapshot:"]
    #[doc = ""]
    #[doc = " - An implementation with persistent state machine: persists the state on disk before"]
    #[doc = "   returning from `apply()`. So that a snapshot does not need to be persistent."]
    #[doc = ""]
    #[doc = " - An implementation with persistent snapshot: `apply()` does not have to persist state on"]
    #[doc = "   disk. But every snapshot has to be persistent. And when starting up the application, the"]
    #[doc = "   state machine should be rebuilt from the last snapshot."]
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<C::R>, StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        todo!()
    }

    #[doc = " Get the snapshot builder for the state machine."]
    #[doc = ""]
    #[doc = " Usually it returns a snapshot view of the state machine(i.e., subsequent changes to the"]
    #[doc = " state machine won\'t affect the return snapshot view), or just a copy of the entire state"]
    #[doc = " machine."]
    #[doc = ""]
    #[doc = " The method is intentionally async to give the implementation a chance to use"]
    #[doc = " asynchronous sync primitives to serialize access to the common internal object, if"]
    #[doc = " needed."]
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        todo!()
    }

    #[doc = " Create a new blank snapshot, returning a writable handle to the snapshot object."]
    #[doc = ""]
    #[doc = " Openraft will use this handle to receive snapshot data."]
    #[doc = ""]
    #[doc = " See the [storage chapter of the guide][sto] for details on log compaction / snapshotting."]
    #[doc = ""]
    #[doc = " [sto]: crate::docs::getting_started#3-implement-raftlogstorage-and-raftstatemachine"]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<C::SnapshotData>, StorageError<C::NodeId>> {
        todo!()
    }

    #[doc = " Install a snapshot which has finished streaming from the leader."]
    #[doc = ""]
    #[doc = " Before this method returns:"]
    #[doc = " - The state machine should be replaced with the new contents of the snapshot,"]
    #[doc = " - the input snapshot should be saved, i.e., [`Self::get_current_snapshot`] should return it."]
    #[doc = " - and all other snapshots should be deleted at this point."]
    #[doc = ""]
    #[doc = " ### snapshot"]
    #[doc = ""]
    #[doc = " A snapshot created from an earlier call to `begin_receiving_snapshot` which provided the"]
    #[doc = " snapshot."]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<C::NodeId, C::Node>,
        snapshot: Box<C::SnapshotData>,
    ) -> Result<(), StorageError<C::NodeId>> {
        todo!()
    }

    #[doc = " Get a readable handle to the current snapshot."]
    #[doc = ""]
    #[doc = " ### implementation algorithm"]
    #[doc = ""]
    #[doc = " Implementing this method should be straightforward. Check the configured snapshot"]
    #[doc = " directory for any snapshot files. A proper implementation will only ever have one"]
    #[doc = " active snapshot, though another may exist while it is being created. As such, it is"]
    #[doc = " recommended to use a file naming pattern which will allow for easily distinguishing between"]
    #[doc = " the current live snapshot, and any new snapshot which is being created."]
    #[doc = ""]
    #[doc = " A proper snapshot implementation will store last-applied-log-id and the"]
    #[doc = " last-applied-membership config as part of the snapshot, which should be decoded for"]
    #[doc = " creating this method\'s response data."]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::Snapshot<C>>, StorageError<C::NodeId>> {
        todo!()
    }
}
