use std::fmt::Write;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::store::{Key, Value};
use crate::{DataType, Instance, Protocol, ReplState, OK, PONG, PROTOCOL};

pub mod info;
pub mod replconf;
pub mod set;

const NULL: DataType = match PROTOCOL {
    Protocol::RESP2 => DataType::NullBulkString,
    Protocol::RESP3 => DataType::Null,
};

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Info(Vec<Bytes>),
    Get(Key),
    Set(Key, Value, set::Options),
    Replconf(replconf::Conf),
    PSync(ReplState),
    FullResync(ReplState),
}

impl Command {
    pub async fn exec(self, instance: Arc<Instance>) -> DataType {
        match self {
            Self::Ping(msg) => msg.map_or(DataType::SimpleString(PONG), DataType::BulkString),

            Self::Echo(msg) => DataType::BulkString(msg),

            Self::Info(sections) => {
                let num_sections = sections.len();
                let info = info::Info::new(&instance, sections);
                let mut data = BytesMut::with_capacity(1024 * num_sections);
                match write!(data, "{}", info) {
                    Ok(_) => DataType::BulkString(data.freeze()),
                    Err(e) => {
                        let error = format!("failed to serialize {info:?}: {e:?}");
                        DataType::SimpleError(error.into())
                    }
                }
            }

            Self::Get(key) => instance
                .store()
                .get(key)
                .await
                .map_or(NULL, |Value(value)| DataType::BulkString(value)),

            Self::Set(key, value, ops) => {
                let (ops, get) = ops.into();
                match instance.store().set(key, value, ops).await {
                    Ok(Some(Value(data))) if get => DataType::BulkString(data),
                    Ok(_) => DataType::SimpleString(OK),
                    Err(_) => NULL,
                }
            }

            // TODO: handle ListeningPort | Capabilities
            Self::Replconf(_) => DataType::SimpleString(OK),

            Self::PSync(
                state @ ReplState {
                    repl_id: None,
                    repl_offset: -1,
                },
            ) => {
                let mut data = BytesMut::with_capacity(64);
                match write!(data, "FULLRESYNC {state}") {
                    Ok(_) => DataType::SimpleString(data.freeze()),
                    Err(e) => {
                        let error = format!("failed to write response to PSYNC ? -1: {e:?}");
                        DataType::SimpleError(error.into())
                    }
                }
            }

            Self::PSync(state) => unimplemented!("handle PSYNC {state:?}"),
            Self::FullResync(state) => unimplemented!("handle FULLRESYNC {state:?}"),
        }
    }
}
