use std::time::Duration;

use crate::{Command, DataType, Error};

const CMD: &str = "wait";

#[derive(Debug)]
pub struct Wait(usize, Duration);

impl TryFrom<&[DataType]> for Wait {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let [num_replicas, timeout] = args else {
            return Err(Error::WrongNumArgs(CMD));
        };

        let num_replicas = match usize::try_from(num_replicas) {
            Ok(n) => n,
            Err(Error::NegInt(_)) => 0,
            Err(e) => return Err(e),
        };

        let timeout = u64::try_from(timeout)
            .map(Duration::from_millis)
            .map_err(|e| match e {
                Error::NotInt(_) => Error::NotInt("timeout"),
                Error::NegInt(_) => Error::NegInt("timeout"),
                e => e,
            })?;

        Ok(Self(num_replicas, timeout))
    }
}

impl From<Wait> for Command {
    #[inline]
    fn from(Wait(num_replicas, timeout): Wait) -> Self {
        Self::Wait(num_replicas, timeout)
    }
}
