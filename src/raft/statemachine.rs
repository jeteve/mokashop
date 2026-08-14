use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use openraft::{
    LogId, OptionalSend, RaftLogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, storage::RaftStateMachine,
};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};

// Example there: https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L79

// What defines the state at the application level.
// This what raft snapshots between nodes.
// AppData can be accessed in a Multiple Reader, Single writer mode.
//

pub struct RaftData<C: RaftTypeConfig, AppData> {
    pub last_applied_log: Option<LogId<C::NodeId>>,
    pub last_membership: StoredMembership<C::NodeId, C::Node>,
    pub app_data: AppData,
    pub apply_entry: fn(&mut Self, C::Entry) -> C::R,
}

#[cfg(test)]
mod tests_raft_data {
    use std::io::Cursor;

    use openraft::{EntryPayload, LeaderId};
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    pub enum Cmd {
        Set(String),
        Get,
    }

    #[derive(Deserialize, Serialize, PartialEq, Eq, Debug)]
    pub enum Resp {
        None,
        Ok(String),
        Error(String),
    }

    #[derive(Serialize, Deserialize)]
    pub struct MyAppData {
        pub s: String,
    }

    openraft::declare_raft_types!(
        pub MyTypeConf:
            D = Cmd,
            R = Resp,
    );

    // Concrete implementation of Entry Payload application
    pub fn apply_entry(
        d: &mut RaftData<MyTypeConf, MyAppData>,
        e: <MyTypeConf as openraft::RaftTypeConfig>::Entry,
    ) -> Resp {
        let p = e.payload;
        match p {
            EntryPayload::Blank => Resp::None,
            EntryPayload::Normal(ref cmd) => match cmd {
                Cmd::Set(s) => {
                    d.app_data.s = s.clone();
                    Resp::Ok("SET!".into())
                }
                Cmd::Get => Resp::Ok(d.app_data.s.clone()),
            },
            EntryPayload::Membership(m) => {
                d.last_membership = openraft::StoredMembership::new(Some(e.log_id), m.clone());
                Resp::None
            }
        }
    }

    #[test]
    fn test_raftdata() {
        use super::*;

        let mut d = RaftData::<MyTypeConf, MyAppData> {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            app_data: MyAppData { s: "Bla".into() },
            apply_entry,
        };

        // For the Entry trait methods. (new_blank for instance..)
        use openraft::entry::RaftEntry;
        let leader_id = LeaderId::new(1, 1);
        let log_id = LogId::<<MyTypeConf as RaftTypeConfig>::NodeId>::new(leader_id, 1);
        let e = <MyTypeConf as RaftTypeConfig>::Entry::new_blank(log_id);
        assert_eq!((d.apply_entry)(&mut d, e), Resp::None);

        type MyEntry = <MyTypeConf as RaftTypeConfig>::Entry;

        let payload = EntryPayload::Normal(Cmd::Set("sausage".into()));
        let e = MyEntry { log_id, payload };

        assert_eq!((d.apply_entry)(&mut d, e), Resp::Ok("SET!".into()));

        let payload = EntryPayload::Normal(Cmd::Get);
        let e = MyEntry { log_id, payload };
        assert_eq!((d.apply_entry)(&mut d, e), Resp::Ok("sausage".into()));
    }
}

// A snapshot of the RaftData
// Meant to fit in memory.
#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct StoredSnapshot<C: RaftTypeConfig> {
    pub meta: openraft::SnapshotMeta<C::NodeId, C::Node>,
    /// The data of the state machine at the time of this snapshot.
    #[serde_as(as = "Base64")]
    pub data: Vec<u8>,
}

// See example there:
// https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L79
pub struct StateMachine<C: RaftTypeConfig, AppData> {
    raft_data: RwLock<RaftData<C, AppData>>, // Tokyo RwLock
    snapshot_idx: AtomicU64,
    // TODO: Persist this on disk, given
    // we choose to implement a Snapshot based state.
    // The stored snapshot can be backed by persistent storage, like a file.
    //current_snapshot: RwLock<Option<StoredSnapshot<C>>>,
    current_snapshot: RwLock<Option<tokio::fs::File>>,
}

pub async fn stored_snapshot_file<C: RaftTypeConfig>(
    snapshot: &StoredSnapshot<C>,
    path: impl AsRef<std::path::Path>,
) -> Result<File, StorageError<C::NodeId>> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        // TODO: Parametrise this file path.
        .open(path)
        .await
        .map_err(|e| StorageIOError::write_snapshot(Some(snapshot.meta.signature()), &e))?;
    // Truncate. This is in case a previous file with the same name exists. Usually the case!
    file.set_len(0)
        .await
        .map_err(|e| StorageIOError::write_snapshot(Some(snapshot.meta.signature()), &e))?;

    file.write_all(
        &serde_json::to_vec(&snapshot)
            .map_err(|e| StorageIOError::write_snapshot(Some(snapshot.meta.signature()), &e))?,
    )
    .await
    .map_err(|e| StorageIOError::write_snapshot(Some(snapshot.meta.signature()), &e))?;

    file.sync_all()
        .await
        .map_err(|e| StorageIOError::write_snapshot(Some(snapshot.meta.signature()), &e))?;
    Ok(file)
}

impl<C: RaftTypeConfig, AppData> StateMachine<C, AppData> {
    pub fn new(raft_data: RaftData<C, AppData>) -> Self {
        Self {
            raft_data: RwLock::new(raft_data),
            snapshot_idx: AtomicU64::default(),
            current_snapshot: RwLock::new(None),
        }
    }
}

pub struct StateMachineArc<C: RaftTypeConfig, AppData> {
    pub inner: Arc<StateMachine<C, AppData>>,
    pub snapshot_path: PathBuf,

    // TODO: Change this so it can return an Result<..,  StorageError<C::NodeId>>
    // Vec<u8> is emited from the serialisation of the inner state machine.
    pub snapshot_data_handle: fn(Vec<u8>) -> C::SnapshotData, // SnapshotData is an async IO handle.
    // Used to receive a SnapshotData. Returns writeable async IO.
    pub snapshot_data_blank: fn() -> C::SnapshotData,
    // Used to turn Snapshotdata handle back into bytes to install.
    pub snapshot_data: fn(C::SnapshotData) -> Vec<u8>,
}

// Specific clone implementation.
impl<C: RaftTypeConfig, AppData> Clone for StateMachineArc<C, AppData> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            snapshot_data_handle: self.snapshot_data_handle,
            snapshot_data_blank: self.snapshot_data_blank,
            snapshot_data: self.snapshot_data,
            snapshot_path: self.snapshot_path.clone(),
        }
    }
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
        // We have the raft data copy. Can drop the lock on the state machine ASAP.
        drop(raft_data);

        // Drop the File if it was there. This is to avoid opening the File
        // on the same Path twice.
        *current_snapshot = None;

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

        let snapshot = StoredSnapshot::<C> {
            meta: meta.clone(),
            data: app_data.clone(),
        };

        // Store the file
        // Note that drops the old file too.
        *current_snapshot = Some(stored_snapshot_file(&snapshot, &self.snapshot_path).await?);

        Ok(openraft::Snapshot {
            meta,
            snapshot: Box::new((self.snapshot_data_handle)(app_data)),
            // This cannot be concrete, as this depends on the C::SnapShotData generic type.
            //snapshot: Box::new(Cursor::new(app_data)),
        })
    }
}

#[cfg(test)]
mod test_state_machine_snapshot {
    use std::io::Cursor;

    use super::tests_raft_data::*;
    use super::*;

    pub fn snapshot_data_handle(v: Vec<u8>) -> <MyTypeConf as RaftTypeConfig>::SnapshotData {
        Cursor::new(v)
    }

    pub fn snapshot_data_blank() -> <MyTypeConf as RaftTypeConfig>::SnapshotData {
        snapshot_data_handle(Vec::new())
    }
    pub fn snapshot_data(d: <MyTypeConf as RaftTypeConfig>::SnapshotData) -> Vec<u8> {
        d.into_inner()
    }

    #[tokio::test]
    async fn test_state_machine() {
        // First build the Raft Data.
        let d = RaftData::<MyTypeConf, MyAppData> {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            app_data: MyAppData { s: "Bla".into() },
            apply_entry,
        };

        // Then build the new state machine:
        let sm = StateMachine::new(d);

        // Then the Arc around it.

        let mut asm = StateMachineArc {
            inner: Arc::new(sm),
            snapshot_path: PathBuf::from("/tmp/foo.json"),
            snapshot_data_handle,
            snapshot_data_blank,
            snapshot_data,
        };

        // Check we can build the snapshot.
        assert!(asm.inner.current_snapshot.read().await.is_none());
        let r = asm.build_snapshot().await;
        assert!(r.is_ok());
        assert!(asm.inner.current_snapshot.read().await.is_some());
    }
}

// Example there:
// https://github.com/databendlabs/openraft/blob/v0.9.21/examples/raft-kv-memstore/src/store/mod.rs#L136
impl<C: RaftTypeConfig, AppData: Send + Sync + 'static + for<'a> Deserialize<'a>>
    RaftStateMachine<C> for StateMachineArc<C, AppData>
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
        let raft_data = self.inner.raft_data.read().await;
        Ok((
            raft_data.last_applied_log,
            raft_data.last_membership.clone(),
        ))
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
        let mut res = Vec::new(); //No `with_capacity`; do not know `len` of iterator

        // Write all entries in one go
        let mut sm = self.inner.raft_data.write().await;

        for e in entries {
            let log_id = *e.get_log_id();
            // Turn entry into a response.
            // Note we dont do any IO here as this is a snapshot based persistent implementation
            // so there cannot be any StorageError.
            res.push((sm.apply_entry)(&mut sm, e));
            sm.last_applied_log = Some(log_id);
        }

        Ok(res)
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
        self.clone()
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
        // This could be something that writes to the disk for instance.
        Ok(Box::new((self.snapshot_data_blank)()))
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
        // Build a new snapshot.
        let new_snapshot = StoredSnapshot::<C> {
            meta: meta.clone(),
            data: (self.snapshot_data)(*snapshot),
        };

        // Update the state machine.
        let updated_state_machine_data = serde_json::from_slice(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;

        let updated_state_machine = RaftData::<C, AppData> {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            app_data: updated_state_machine_data,
            apply_entry: self.inner.raft_data.read().await.apply_entry,
        };
        let mut state_machine = self.inner.raft_data.write().await;
        // No need to save to disk or anything. Only in memory please.
        *state_machine = updated_state_machine;

        // Lock the current snapshot before releasing the lock on the state machine, to avoid a race
        // condition on the written snapshot
        let mut current_snapshot = self.inner.current_snapshot.write().await;
        drop(state_machine);

        // Update current snapshot.
        // TODO: Save that to disk.
        *current_snapshot = Some(stored_snapshot_file(&new_snapshot, &self.snapshot_path).await?);

        Ok(())
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
        match &mut *self.inner.current_snapshot.write().await {
            Some(file) => {
                // Get the bytes and deserialise into A StoredSnapshot
                let mut buf = vec![];
                file.read_to_end(&mut buf)
                    .await
                    .map_err(|e| StorageIOError::read_snapshot(None, &e))?;

                let stored_snapshot: StoredSnapshot<C> = serde_json::from_slice(&buf)
                    .map_err(|e| StorageIOError::read_snapshot(None, &e))?;

                // The data persists in memory, so we can give a handle onto it
                let data = stored_snapshot.data.clone();
                Ok(Some(openraft::Snapshot {
                    meta: stored_snapshot.meta.clone(),
                    snapshot: Box::new((self.snapshot_data_handle)(data)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod test_state_machine_full {

    use std::sync::Arc;

    use openraft::RaftTypeConfig;
    use openraft::StorageError;
    use openraft::StoredMembership;
    use redb::Database;

    use crate::raft::statemachine::StateMachineArc;

    use super::super::logstore::*;
    use super::test_state_machine_snapshot::*;
    use super::tests_raft_data::*;
    use super::*;

    // Alias is needed to have the good shaped signature.
    // For the StoreBuilder trait implementation.
    // Currying the MyAppData in :)
    // Note we would like a bounding
    // C: RaftTypeConfig , but that's not possible.
    type Sma<C> = StateMachineArc<C, MyAppData>;
    struct StoresBuilder;
    impl openraft::testing::StoreBuilder<MyTypeConf, LogStore<MyTypeConf>, Sma<MyTypeConf>>
        for StoresBuilder
    {
        #[doc = " Build a [`RaftLogStorage`] and [`RaftStateMachine`] implementation"]
        async fn build(
            &self,
        ) -> Result<
            ((), LogStore<MyTypeConf>, Sma<MyTypeConf>),
            StorageError<<MyTypeConf as RaftTypeConfig>::NodeId>,
        > {
            // Build a logstore.
            let file = tempfile::NamedTempFile::new().unwrap();
            let db = Database::create(file.path()).unwrap();
            let ls = LogStore::<MyTypeConf>::new(Arc::new(db));

            // Build a state machine.
            // First build the Raft Data.
            let d = RaftData::<MyTypeConf, MyAppData> {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                app_data: MyAppData { s: "Bla".into() },
                apply_entry,
            };

            // Then build the new state machine:
            let sm = StateMachine::new(d);
            // Then the Arc, which is the one that implements the SM.
            let asm = StateMachineArc {
                inner: Arc::new(sm),
                snapshot_path: PathBuf::from("/tmp/foo.json"),
                snapshot_data_handle,
                snapshot_data_blank,
                snapshot_data,
            };

            Ok(((), ls.unwrap(), asm))
        }
    }

    #[test]
    fn test_core() {
        assert_eq!(openraft::testing::Suite::test_all(StoresBuilder), Ok(()));
    }
}
