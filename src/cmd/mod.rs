use std::sync::Arc;

use bytes::Bytes;

use crate::store::{Key, Store, Value};
use crate::{DataType, Protocol, PROTOCOL};

pub mod set;

pub(crate) const PONG: Bytes = Bytes::from_static(b"PONG");
pub(crate) const OK: Bytes = Bytes::from_static(b"OK");

const NULL: DataType = match PROTOCOL {
    Protocol::RESP2 => DataType::NullBulkString,
    Protocol::RESP3 => DataType::Null,
};

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(Key),
    Set(Key, Value, set::Options),
}

impl Command {
    pub async fn exec(self, store: Arc<Store>) -> DataType {
        match self {
            Self::Ping(msg) => msg.map_or(DataType::SimpleString(PONG), DataType::BulkString),
            Self::Echo(msg) => DataType::BulkString(msg),
            Self::Get(key) => store
                .get(key)
                .await
                .map_or(NULL, |Value(value)| DataType::BulkString(value)),
            Self::Set(key, value, ops) => {
                let (ops, get) = ops.into();
                match store.set(key, value, ops).await {
                    Some(Value(data)) if get => DataType::BulkString(data),
                    Some(_) => DataType::SimpleString(OK),
                    None => NULL,
                }
            }
        }
    }
}
