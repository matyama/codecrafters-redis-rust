use crate::{rdb, Command, DataType, Error};

#[derive(Debug)]
#[repr(transparent)]
pub struct Ping(Option<rdb::String>);

impl TryFrom<&[DataType]> for Ping {
    type Error = Error;

    #[inline]
    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        match args.first() {
            None => Ok(Self(None)),
            Some(DataType::BulkString(msg)) => Ok(Self(Some(msg.clone()))),
            Some(arg) => Err(Error::err(format!(
                "ping argument must be a string, got {arg:?}"
            ))),
        }
    }
}

impl From<Ping> for Command {
    #[inline]
    fn from(Ping(msg): Ping) -> Self {
        Self::Ping(msg)
    }
}
