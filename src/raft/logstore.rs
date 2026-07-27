use openraft::{OptionalSend, RaftLogReader, RaftTypeConfig, StorageError, StorageIOError};
use redb::{Database, ReadableDatabase, TableDefinition};
use std::error::Error;
use std::fmt::Debug;
use std::{marker::PhantomData, ops::RangeBounds};

// See example there:
// https://github.com/databendlabs/openraft/blob/main/examples/rocksstore/src/log_store.rs

// See https://docs.rs/redb/latest/redb/

// A logID -> bytes.
const LOGS_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("logs");
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");

struct LogStore<C>
where
    C: RaftTypeConfig,
{
    db: Database, // To be able to flush in another thread.
    _p: PhantomData<C>,
}

impl<C> LogStore<C>
where
    C: RaftTypeConfig,
{
    pub fn new(db: Database) -> Self {
        Self {
            db,
            _p: Default::default(),
        }
    }
}

fn read_logs_error<C: RaftTypeConfig>(e: impl Error + 'static) -> StorageError<C::NodeId> {
    StorageError::IO {
        source: StorageIOError::read_logs(&e),
    }
}

impl<C> RaftLogReader<C> for LogStore<C>
where
    C: RaftTypeConfig,
    C::Entry: for<'de> serde::Deserialize<'de>,
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
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let read_txn = self.db.begin_read().map_err(read_logs_error::<C>)?;
        let logs_table = read_txn
            .open_table(LOGS_TABLE)
            .map_err(read_logs_error::<C>)?;

        let found_range = logs_table.range(range).map_err(read_logs_error::<C>)?;
        found_range
            .map(|e| {
                e.map(|e| e.1.value().to_vec())
                    .map_err(|err| Box::new(read_logs_error::<C>(err)))
                    .and_then(|v| {
                        serde_json::from_slice::<C::Entry>(&v)
                            .map_err(|err| Box::new(read_logs_error::<C>(err)))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| *e)
    }
}
