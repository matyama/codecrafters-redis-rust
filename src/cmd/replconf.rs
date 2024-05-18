use std::sync::Arc;

use bytes::Bytes;

use crate::data::{DataExt as _, DataType};
use crate::{rdb, Command, Error};

pub(crate) const GETACK: Bytes = Bytes::from_static(b"GETACK");
pub(crate) const ACK: Bytes = Bytes::from_static(b"ACK");

#[derive(Clone, Debug)]
pub enum Conf {
    ListeningPort(u16),
    Capabilities(Arc<[Bytes]>),
    GetAck(Bytes),
    Ack(isize),
}

impl TryFrom<&[DataType]> for Conf {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        use {rdb::String::*, DataType::*};

        let mut capabilities = Vec::with_capacity(2);

        let mut args = args.iter().cloned();

        while let Some(key) = args.next() {
            let (BulkString(key) | SimpleString(key)) = key else {
                continue;
            };

            if key.matches(GETACK) {
                return args
                    .next()
                    .and_then(|arg| match arg {
                        BulkString(dummy) | SimpleString(dummy) => dummy.bytes(),
                        _ => None,
                    })
                    .map(Self::GetAck)
                    .ok_or(Error::Syntax);
            }

            if key.matches(ACK) {
                let Some(offset) = args.next() else {
                    return Err(Error::Syntax);
                };

                let Integer(offset) = offset.parse_int()? else {
                    unreachable!("if parse_int succeeds, then only with integers");
                };

                return Ok(Self::Ack(offset as isize));
            }

            let Str(key) = key else {
                continue;
            };

            // NOTE: configuration options seems to always be lowercase, thus matched exactly
            match key.as_ref() {
                b"listening-port" => {
                    let Some(port) = args.next() else {
                        return Err(Error::Syntax);
                    };

                    return match u64::try_from(port) {
                        Ok(port) if port <= u16::MAX as u64 => Ok(Self::ListeningPort(port as u16)),
                        Err(Error::NegInt(_)) => Ok(Self::ListeningPort(0)),
                        _ => Err(Error::VAL_NOT_INT),
                    };
                }

                b"capa" | b"capabilities" => match args.next() {
                    Some(BulkString(capa) | SimpleString(capa)) => {
                        if let Some(capa) = capa.bytes() {
                            capabilities.push(capa);
                        }
                    }
                    Some(_) => continue,
                    None => return Err(Error::Syntax),
                },

                _ => continue,
            }
        }

        // NOTE: in case of listening-port we return immediately
        if capabilities.is_empty() {
            Err(Error::Syntax)
        } else {
            Ok(Self::Capabilities(capabilities.into()))
        }
    }
}

impl From<Conf> for Command {
    #[inline]
    fn from(conf: Conf) -> Self {
        Self::Replconf(conf)
    }
}
