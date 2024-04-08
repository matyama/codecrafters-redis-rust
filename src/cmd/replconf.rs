use std::{collections::VecDeque, u16};

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{DataExt as _, DataType};

#[derive(Debug)]
pub enum Conf {
    ListeningPort(u16),
    Capabilities(Bytes),
}

impl TryFrom<VecDeque<DataType>> for Conf {
    type Error = anyhow::Error;

    fn try_from(mut args: VecDeque<DataType>) -> Result<Self> {
        while let Some(key) = args.pop_front() {
            let key @ (DataType::BulkString(_) | DataType::SimpleString(_)) = key else {
                continue;
            };

            match key.to_lowercase().as_slice() {
                b"listening-port" => {
                    let Some(port) = args.pop_front() else {
                        bail!("protocol violation: REPLCONF {key:?} _ is missing an argument");
                    };

                    match port.parse_int()? {
                        DataType::Integer(port) if 0 <= port && port < (u16::MAX as i64) => {
                            return Ok(Self::ListeningPort(port as u16));
                        }
                        other => bail!("protocol violation: REPLCONF {key:?} {other:?}"),
                    }
                }

                b"capa" | b"capabilities" => match args.pop_front() {
                    Some(DataType::BulkString(capa) | DataType::SimpleString(capa)) => {
                        return Ok(Self::Capabilities(capa));
                    }
                    other => bail!("protocol violation: REPLCONF {key:?} {other:?}"),
                },

                _ => continue,
            }
        }

        bail!("protocol violation: REPLCONF with unknown/unsupported arguments");
    }
}
