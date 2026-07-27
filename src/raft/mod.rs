use serde::{Deserialize, Serialize};
use std::io::Cursor;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AppCommand {
    // Internal raft level state changing command
    Put(crate::Qid, String), // Put a query identified by Qid
    Del(crate::Qid),         // Delete a query identified by Qid
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AppResult {
    // A response to a state changing command is either nothing
    // (things went well) , or an error (things went not well)
    None,
    Error(String),
}

openraft::declare_raft_types!(
    pub OurTypeConfig:
        D = AppCommand,
        R = AppResult,
);

pub mod logstore;
