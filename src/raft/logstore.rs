use openraft::RaftTypeConfig;
use openraft::storage::RaftLogStorage;
use openraft::{OptionalSend, RaftLogReader, StorageError, StorageIOError};
use redb::{Database, ReadableDatabase, TableDefinition};
use std::error::Error;
use std::fmt::Debug;
use std::ops::RangeBounds;

use crate::raft::OurTypeConfig;

// See example there:
// https://github.com/databendlabs/openraft/blob/main/examples/rocksstore/src/log_store.rs

// See https://docs.rs/redb/latest/redb/

// A logID -> bytes.
const LOGS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("logs");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

struct LogStore {
    db: Database, // To be able to flush in another thread.
}

impl LogStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }
}

fn read_logs_error(
    e: impl Error + 'static,
) -> StorageError<<OurTypeConfig as RaftTypeConfig>::NodeId> {
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
        let read_txn = self.db.begin_read().map_err(read_logs_error)?;
        let logs_table = read_txn.open_table(LOGS_TABLE).map_err(read_logs_error)?;

        let found_range = logs_table.range(range).map_err(read_logs_error)?;
        found_range
            .map(|e| {
                e.map(|e| e.1.value().to_vec())
                    .map_err(|err| Box::new(read_logs_error(err)))
                    .and_then(|v| {
                        serde_json::from_slice::<<OurTypeConfig as RaftTypeConfig>::Entry>(&v)
                            .map_err(|err| Box::new(read_logs_error(err)))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| *e)
    }
}

//impl RaftLogStorage<OurTypeConfig> for LogStore {}
