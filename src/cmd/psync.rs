use crate::{Command, DataType, Error, ReplState};

const CMD: &str = "psync";

#[derive(Debug)]
pub struct PSync(ReplState);

impl TryFrom<&[DataType]> for PSync {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let [repl_id, repl_offset] = args else {
            return Err(Error::WrongNumArgs(CMD));
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
