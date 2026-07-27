use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogId, LogState, RaftTypeConfig, Vote};
use openraft::{OptionalSend, RaftLogReader, StorageError, StorageIOError};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Deserialize;
use std::error::Error;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::raft::OurTypeConfig;

// See example there:
// https://github.com/databendlabs/openraft/blob/main/examples/rocksstore/src/log_store.rs

// See https://docs.rs/redb/latest/redb/

// A logID -> bytes.
const LOGS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("logs");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

#[derive(Clone)]
struct LogStore {
    db: Arc<Database>, // To be able to flush in another thread and clone.
}

impl LogStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

fn to_storeerr(e: impl Error + 'static) -> StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(&e),
    }
}

impl RaftLogReader<OurTypeConfig> for LogStore
where
    <OurTypeConfig as RaftTypeConfig>::Entry: for<'de> serde::Deserialize<'de>,
{
    #[doc = " Get a series of log entries from storage."]
    #[doc = ""]
    #[doc = " The start value is inclusive in the search and the stop value is non-inclusive: `[start,"]
    #[doc = " stop)`."]
    #[doc = ""]
    #[doc = " Entry that is not found is allowed."]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<
        Vec<<OurTypeConfig as RaftTypeConfig>::Entry>,
        StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>,
    > {
        let read_txn = self.db.begin_read().map_err(to_storeerr)?;
        let logs_table = read_txn.open_table(LOGS_TABLE).map_err(to_storeerr)?;

        let found_range = logs_table.range(range).map_err(to_storeerr)?;
        found_range
            .map(|e| {
                e.map(|e| e.1.value().to_vec())
                    .map_err(|err| Box::new(to_storeerr(err)))
                    .and_then(|v| {
                        serde_json::from_slice::<<OurTypeConfig as RaftTypeConfig>::Entry>(&v)
                            .map_err(|err| Box::new(to_storeerr(err)))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| *e)
    }
}

fn read_meta<D: for<'de> serde::Deserialize<'de>>(
    meta_table: &redb::ReadOnlyTable<&str, &[u8]>,
    meta: &str,
) -> Result<Option<D>, StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>> {
    meta_table
        .get(meta)
        .map_err(to_storeerr)?
        .map(|g| serde_json::from_slice::<D>(g.value()).map_err(to_storeerr))
        .map_or(Ok(None), |v| v.map(Some))
}

fn log_id_meta(
    meta_table: &redb::ReadOnlyTable<&str, &[u8]>,
    meta: &str,
) -> Result<
    Option<LogId<<OurTypeConfig as RaftTypeConfig>::NodeId>>,
    StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>,
> {
    read_meta::<LogId<<OurTypeConfig as RaftTypeConfig>::NodeId>>(meta_table, meta)
}

impl RaftLogStorage<OurTypeConfig> for LogStore {
    #[doc = " Log reader type."]
    #[doc = ""]
    #[doc = " Log reader is used by multiple replication tasks, which read logs and send them to remote"]
    #[doc = " nodes."]
    type LogReader = Self;

    #[doc = " Returns the last deleted log id and the last log id."]
    #[doc = ""]
    #[doc = " The impl should **not** consider the applied log id in state machine."]
    #[doc = " The returned `last_log_id` could be the log id of the last present log entry, or the"]
    #[doc = " `last_purged_log_id` if there is no entry at all."]
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<OurTypeConfig>, StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>>
    {
        let read_txn = self.db.begin_read().map_err(to_storeerr)?;
        let meta_table: redb::ReadOnlyTable<&str, &[u8]> =
            read_txn.open_table(META_TABLE).map_err(to_storeerr)?;

        let last_purged_log_id = log_id_meta(&meta_table, "last_purged_log_id")?;

        let logs = read_txn.open_table(LOGS_TABLE).map_err(to_storeerr)?;
        let last_entry = logs.last().map_err(to_storeerr)?;

        let last_log_id = last_entry
            .map(|g| {
                serde_json::from_slice::<LogId<<OurTypeConfig as RaftTypeConfig>::NodeId>>(
                    g.1.value(),
                )
            })
            .map_or(Ok(None), |v| v.map(Some))
            .map_err(to_storeerr)?;

        Ok(LogState::<OurTypeConfig> {
            last_purged_log_id,
            last_log_id: last_log_id.or(last_purged_log_id),
        })
    }

    #[doc = " Get the log reader."]
    #[doc = ""]
    #[doc = " The method is intentionally async to give the implementation a chance to use asynchronous"]
    #[doc = " primitives to serialize access to the common internal object, if needed."]
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[doc = " Save vote to storage."]
    #[doc = ""]
    #[doc = " ### To ensure correctness:"]
    #[doc = ""]
    #[doc = " The vote must be persisted on disk before returning."]
    async fn save_vote(
        &mut self,
        vote: &Vote<<OurTypeConfig as RaftTypeConfig>::NodeId>,
    ) -> Result<(), StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>> {
        let write_tx = self.db.begin_write().map_err(to_storeerr)?;
        {
            let mut meta_table = write_tx.open_table(META_TABLE).map_err(to_storeerr)?;
            let json_bytes = serde_json::to_vec(vote).map_err(to_storeerr)?;
            meta_table
                .insert("vote", json_bytes.as_slice())
                .map_err(to_storeerr)?;
        }
        write_tx.commit().map_err(to_storeerr)?;

        Ok(())
    }

    #[doc = " Return the last saved vote by [`Self::save_vote`]."]
    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<Vote<<OurTypeConfig as RaftTypeConfig>::NodeId>>,
        StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>,
    > {
        let read_txn = self.db.begin_read().map_err(to_storeerr)?;
        let meta_table: redb::ReadOnlyTable<&str, &[u8]> =
            read_txn.open_table(META_TABLE).map_err(to_storeerr)?;
        read_meta::<Vote<<OurTypeConfig as RaftTypeConfig>::NodeId>>(&meta_table, "vote")
    }

    #[doc = " Append log entries and call the `callback` once logs are persisted on disk."]
    #[doc = ""]
    #[doc = " It should returns immediately after saving the input log entries in memory, and calls the"]
    #[doc = " `callback` when the entries are persisted on disk, i.e., avoid blocking."]
    #[doc = ""]
    #[doc = " This method is still async because preparing the IO is usually async."]
    #[doc = ""]
    #[doc = " ### To ensure correctness:"]
    #[doc = ""]
    #[doc = " - When this method returns, the entries must be readable, i.e., a `LogReader` can read these"]
    #[doc = "   entries."]
    #[doc = ""]
    #[doc = " - When the `callback` is called, the entries must be persisted on disk."]
    #[doc = ""]
    #[doc = "   NOTE that: the `callback` can be called either before or after this method returns."]
    #[doc = ""]
    #[doc = " - There must not be a **hole** in logs. Because Raft only examine the last log id to ensure"]
    #[doc = "   correctness."]
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<OurTypeConfig>,
    ) -> Result<(), StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>>
    where
        I: IntoIterator<Item = <OurTypeConfig as RaftTypeConfig>::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let write_tx = self.db.begin_write().map_err(to_storeerr)?;

        {
            let mut logs = write_tx.open_table(LOGS_TABLE).map_err(to_storeerr)?;
            for e in entries {
                let bytes = serde_json::to_vec(&e).map_err(to_storeerr)?;
                logs.insert(e.log_id.index, bytes.as_slice())
                    .map_err(to_storeerr)?;
            }
        }

        write_tx.commit().map_err(to_storeerr)?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    #[doc = " Truncate logs since `log_id`, inclusive"]
    #[doc = ""]
    #[doc = " ### To ensure correctness:"]
    #[doc = ""]
    #[doc = " - It must not leave a **hole** in logs."]
    async fn truncate(
        &mut self,
        log_id: LogId<<OurTypeConfig as RaftTypeConfig>::NodeId>,
    ) -> Result<(), StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>> {
        let write_tx = self.db.begin_write().map_err(to_storeerr)?;

        {
            let mut logs = write_tx.open_table(LOGS_TABLE).map_err(to_storeerr)?;
            logs.retain_in((log_id.index).., |_, _| false)
                .map_err(to_storeerr)?;
        }

        write_tx.commit().map_err(to_storeerr)?;
        Ok(())
    }

    #[doc = " Purge logs upto `log_id`, inclusive"]
    #[doc = ""]
    #[doc = " ### To ensure correctness:"]
    #[doc = ""]
    #[doc = " - It must not leave a **hole** in logs."]
    async fn purge(
        &mut self,
        log_id: LogId<<OurTypeConfig as RaftTypeConfig>::NodeId>,
    ) -> Result<(), StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId>> {
        // Dont forget to set last purge ID.

        let write_tx = self.db.begin_write().map_err(to_storeerr)?;

        {
            let mut logs = write_tx.open_table(LOGS_TABLE).map_err(to_storeerr)?;
            logs.retain_in(..=(log_id.index), |_, _| false)
                .map_err(to_storeerr)?;
            let mut meta = write_tx.open_table(META_TABLE).map_err(to_storeerr)?;
            let bytes = serde_json::to_vec(&log_id).map_err(to_storeerr)?;
            meta.insert("last_purged_log_id", bytes.as_slice())
                .map_err(to_storeerr)?;
        }

        write_tx.commit().map_err(to_storeerr)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::raft::logstore::LogStore;
    use openraft::{Vote, storage::RaftLogStorage};
    use redb::Database;

    const NODE_ID: u64 = 0;

    #[tokio::test]
    async fn test_my_storage() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let db = Database::create(file.path()).unwrap();
        let mut store = LogStore::new(Arc::new(db));
        // See https://docs.rs/openraft/latest/src/openraft/testing/suite.rs.html

        store.save_vote(&Vote::new(100, NODE_ID)).await.unwrap();

        let got = store.read_vote().await.unwrap();

        assert_eq!(Some(Vote::new(100, NODE_ID)), got,);
    }
}
