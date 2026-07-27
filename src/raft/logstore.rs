use openraft::{OptionalSend, RaftLogReader, RaftTypeConfig, StorageError};
use redb::{Database, ReadableDatabase, TableDefinition};
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

impl<C> RaftLogReader<C> for LogStore<C>
where
    C: RaftTypeConfig,
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
        let mut res = Vec::new();

        let read_txn = self.db.begin_read()?

        Ok(res)
    }
}
