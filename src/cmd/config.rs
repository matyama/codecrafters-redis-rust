use std::sync::Arc;

use bytes::Bytes;

use crate::data::{DataExt as _, DataType};
use crate::{Command, Error};

#[derive(Debug)]
pub struct ConfigGet(Arc<[Bytes]>);

impl TryFrom<&[DataType]> for ConfigGet {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let Some((arg, args)) = args.split_first() else {
            return Err(Error::WrongNumArgs("config"));
        };

        if !arg.matches(b"get") {
            return Err(Error::unkown_subcmd(arg, "CONFIG HELP"));
        };

        let params: Arc<[_]> = args
            .iter()
            .filter_map(|arg| match arg {
                DataType::BulkString(s) | DataType::SimpleString(s) => s.bytes(),
                _ => None,
            })
            .collect();

        if params.is_empty() {
            return Err(Error::WrongNumArgs("config|get"));
        }

        Ok(Self(params))
    }
}

impl From<ConfigGet> for Command {
    #[inline]
    fn from(ConfigGet(params): ConfigGet) -> Self {
        Self::Config(params)
    }
}
