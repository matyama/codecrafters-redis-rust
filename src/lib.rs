use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
pub use reader::DataReader;
use tokio::sync::RwLock;
pub use writer::DataWriter;

pub(crate) mod reader;
pub(crate) mod writer;

pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
pub(crate) const NULL: &[u8] = b"_\r\n";
pub(crate) const PONG: Bytes = Bytes::from_static(b"PONG");
pub(crate) const OK: Bytes = Bytes::from_static(b"OK");

pub trait DataExt {
    // NOTE: this could probably benefit from small vec optimization
    fn cmd(&self) -> Vec<u8>;
}

impl DataExt for Bytes {
    #[inline]
    fn cmd(&self) -> Vec<u8> {
        self.to_ascii_uppercase()
    }
}

#[derive(Debug)]
pub enum DataType {
    Null,
    Boolean(bool),
    SimpleString(Bytes),
    BulkString(Bytes),
    Array(VecDeque<DataType>),
}

impl DataExt for DataType {
    #[inline]
    fn cmd(&self) -> Vec<u8> {
        match self {
            Self::SimpleString(s) => s.cmd(),
            Self::BulkString(s) => s.cmd(),
            other => format!("{other:?}").into(),
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(Key),
    Set(Key, Value),
}

impl Command {
    pub async fn exec(self, store: Arc<Store>) -> DataType {
        match self {
            Self::Ping(msg) => msg.map_or(DataType::SimpleString(PONG), DataType::BulkString),
            Self::Echo(msg) => DataType::BulkString(msg),
            Self::Get(key) => store
                .get(&key)
                .await
                .map_or(DataType::Null, |Value(value)| DataType::BulkString(value)),
            Self::Set(key, value) => {
                let _value = store.set(key, value).await;
                DataType::SimpleString(OK)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Key(Bytes);

impl From<Bytes> for Key {
    #[inline]
    fn from(key: Bytes) -> Self {
        Self(key)
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Value(Bytes);

impl From<Bytes> for Value {
    #[inline]
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

#[derive(Debug, Default)]
pub struct Store(RwLock<HashMap<Key, Value>>);

impl Store {
    pub async fn get(self: Arc<Self>, key: &Key) -> Option<Value> {
        let store = self.0.read().await;
        let value = store.get(key).cloned();
        drop(store);
        value
    }

    pub async fn set(self: Arc<Self>, key: Key, value: Value) -> Option<Value> {
        let mut store = self.0.write().await;
        let value = store.insert(key, value);
        drop(store);
        value
    }
}
