use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;

pub use reader::DataReader;
use tokio::sync::RwLock;
pub use writer::DataWriter;

pub(crate) mod reader;
pub(crate) mod writer;

pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
pub(crate) const NULL: &[u8] = b"_\r\n";
pub(crate) const PONG: &[u8] = b"PONG";
pub(crate) const OK: &[u8] = b"OK";

pub trait DataExt {
    fn cmd(self) -> Data;
}

impl<'a> DataExt for Cow<'a, [u8]> {
    fn cmd(self) -> Data {
        match self {
            Cow::Borrowed(data) => Data(Cow::Owned(data.to_ascii_uppercase())),
            Cow::Owned(mut data) => {
                data.make_ascii_uppercase();
                Data(Cow::Owned(data))
            }
        }
    }
}

// TODO: consolidate Vec vs Box<[u8]> vs Cow<'_, [u8]>
// TODO: perhaps these could be Bytes (for cheap clone and write)
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Data(Cow<'static, [u8]>);

impl DataExt for Data {
    #[inline]
    fn cmd(self) -> Self {
        self.0.cmd()
    }
}

impl Deref for Data {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Data {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<&'static [u8]> for Data {
    fn from(data: &'static [u8]) -> Self {
        Self(Cow::Borrowed(data))
    }
}

impl From<Box<[u8]>> for Data {
    #[inline]
    fn from(data: Box<[u8]>) -> Self {
        Self(Vec::from(data).into())
    }
}

impl From<Vec<u8>> for Data {
    #[inline]
    fn from(data: Vec<u8>) -> Self {
        Self(data.into())
    }
}

impl From<String> for Data {
    #[inline]
    fn from(value: String) -> Self {
        Self(Cow::Owned(format!("{value:?}").as_bytes().into()))
    }
}

#[derive(Debug)]
pub enum DataType {
    Null,
    Boolean(bool),
    SimpleString(Data),
    BulkString(Data),
    Array(VecDeque<DataType>),
}

impl DataExt for DataType {
    #[inline]
    fn cmd(self) -> Data {
        match self {
            Self::SimpleString(s) => s.cmd(),
            Self::BulkString(s) => s.cmd(),
            other => Data::from(format!("{other:?}")),
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Ping(Option<Data>),
    Echo(Data),
    Get(Key),
    Set(Key, Value),
}

impl Command {
    pub async fn exec(self, store: Arc<Store>) -> DataType {
        match self {
            Self::Ping(Some(msg)) => DataType::BulkString(msg),
            Self::Ping(None) => DataType::SimpleString(PONG.into()),
            Self::Echo(msg) => DataType::BulkString(msg),
            Self::Get(key) => store
                .get(&key)
                .await
                .map_or(DataType::Null, |Value(value)| {
                    DataType::BulkString(value.into())
                }),
            Self::Set(key, value) => {
                let _value = store.set(key, value).await;
                DataType::SimpleString(OK.into())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Key(Box<[u8]>);

impl From<Data> for Key {
    #[inline]
    fn from(Data(data): Data) -> Self {
        Self(data.into())
    }
}

// TODO: perhaps these could be Bytes (for cheap clone and write)
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Value(Box<[u8]>);

impl From<Data> for Value {
    #[inline]
    fn from(Data(data): Data) -> Self {
        Self(data.into())
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
