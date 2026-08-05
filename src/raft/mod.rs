use serde::{Deserialize, Serialize};
use std::io::Cursor;

// Commands flowing about one shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardCommand {
    // Internal raft level state changing command
    Put(crate::Qid, String), // Put a query identified by Qid
    Del(crate::Qid),         // Delete a query identified by Qid
}

// Result of command on a shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardResult {
    // A response to a state changing command is either nothing
    // (things went well) , or an error (things went not well)
    None,
    Error(String),
}

openraft::declare_raft_types!(
    pub ShardConfig:
        D = ShardCommand,
        R = ShardResult,
);

pub mod logstore;
