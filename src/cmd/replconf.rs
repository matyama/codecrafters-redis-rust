use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;

use crate::{DataExt as _, DataType};

#[derive(Clone, Debug)]
pub enum Conf {
    ListeningPort(u16),
    Capabilities(Arc<[Bytes]>),
    GetAck(Bytes),
    Ack(isize),
}

impl TryFrom<&[DataType]> for Conf {
    type Error = anyhow::Error;

    fn try_from(args: &[DataType]) -> Result<Self> {
        let mut capabilities = Vec::with_capacity(2);

        let mut args = args.iter().cloned();

        while let Some(key) = args.next() {
            let key @ (DataType::BulkString(_) | DataType::SimpleString(_)) = key else {
                continue;
            };

            match key.to_lowercase().as_slice() {
                b"listening-port" => {
                    let Some(port) = args.next() else {
                        bail!("protocol violation: REPLCONF {key:?} _ is missing an argument");
                    };

                    match port.parse_int()? {
                        DataType::Integer(port) if 0 <= port && port < (u16::MAX as i64) => {
                            return Ok(Self::ListeningPort(port as u16));
                        }
                        other => bail!("protocol violation: REPLCONF {key:?} {other:?}"),
                    }
                }

                b"getack" => match args.next() {
                    Some(DataType::BulkString(dummy) | DataType::SimpleString(dummy)) => {
                        return Ok(Self::GetAck(dummy));
                    }
                    other => {
                        bail!("protocol violation: REPLCONF GETACK {other:?}");
                    }
                },

                b"ack" => {
                    let Some(offset) = args.next() else {
                        bail!("protocol violation: REPLCONF ACK _ is missing an offset argument");
                    };

                    let offset = offset
                        .parse_int()
                        .context("protocol violation: REPLCONF ACK _ with an invalid offset")?;

                    let DataType::Integer(offset) = offset else {
                        unreachable!("if parse_int succeeds, then only with integers");
                    };

                    return Ok(Self::Ack(offset as isize));
                }

                b"capa" | b"capabilities" => match args.next() {
                    Some(DataType::BulkString(capa) | DataType::SimpleString(capa)) => {
                        capabilities.push(capa);
                    }
                    other => bail!("protocol violation: REPLCONF {key:?} {other:?}"),
                },

                _ => continue,
            }
        }

        // NOTE: in case of listening-port we return immediately
        ensure!(
            !capabilities.is_empty(),
            "protocol violation: REPLCONF with unknown/unsupported arguments"
        );

        Ok(Self::Capabilities(capabilities.into()))
    }
}
