use bytes::BytesMut;

use crate::cmd::{Command, FULLRESYNC};
use crate::{rdb, DataType, Error, ReplState};

#[derive(Debug)]
pub struct PSync(ReplState);

impl TryFrom<&[DataType]> for PSync {
    type Error = Error;

    /// PSYNC replicationid offset
    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let [repl_id, repl_offset] = args else {
            return Err(Error::WrongNumArgs("psync"));
        };

        let (DataType::SimpleString(repl_id) | DataType::BulkString(repl_id)) = repl_id.clone()
        else {
            return Err(Error::err("PSYNC replication ID must be a string"));
        };

        let (DataType::SimpleString(repl_offset) | DataType::BulkString(repl_offset)) =
            repl_offset.clone()
        else {
            return Err(Error::VAL_NOT_INT);
        };

        ReplState::try_from((repl_id, repl_offset)).map(Self)
    }
}

impl From<PSync> for Command {
    #[inline]
    fn from(PSync(state): PSync) -> Self {
        Self::PSync(state)
    }
}

#[derive(Debug)]
pub struct FullResync(ReplState);

impl TryFrom<&DataType> for FullResync {
    type Error = Error;

    /// FULLRESYNC replicationid offset
    fn try_from(cmd: &DataType) -> Result<Self, Self::Error> {
        use {rdb::String::*, DataType::*};

        let (SimpleString(Str(args)) | BulkString(Str(args))) = cmd else {
            return Err(Error::err("FULLRESYNC must be a string"));
        };

        let args = args
            .strip_prefix(FULLRESYNC.as_ref())
            .unwrap_or_default()
            .split(|b| b.is_ascii_whitespace())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>();

        // NOTE: FULLRESYNC actually returns UnknownCommand instead of more appropriate errors

        let [repl_id, repl_offset] = args[..] else {
            return Err(Error::unkown_cmd(cmd, &[]));
        };

        let repl_id = BytesMut::from(repl_id).freeze();

        ReplState::try_from((repl_id, repl_offset))
            .map(Self)
            .map_err(|_| Error::unkown_cmd(cmd, &[]))
    }
}

impl From<FullResync> for Command {
    #[inline]
    fn from(FullResync(state): FullResync) -> Self {
        Self::FullResync(state)
    }
}
