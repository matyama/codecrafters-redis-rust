use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{bail, Context as _};
use tokio::sync::Mutex;

use crate::{rdb, DataType};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl StreamId {
    const MIN: StreamId = StreamId { ms: 0, seq: 1 };

    #[inline]
    pub fn new<T, S>(ms: T, seq: S) -> Option<Self>
    where
        T: Into<u64>,
        S: Into<u64>,
    {
        match seq.into() {
            0 => None,
            seq => Some(Self { ms: ms.into(), seq }),
        }
    }
}

impl Default for StreamId {
    #[inline]
    fn default() -> Self {
        Self::MIN
    }
}

impl std::fmt::Display for StreamId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl TryFrom<rdb::String> for StreamId {
    type Error = anyhow::Error;

    fn try_from(s: rdb::String) -> Result<Self, Self::Error> {
        use rdb::String::*;

        let Str(s) = s else {
            bail!("cannot parse stream id from {s:?}");
        };

        let sep = s
            .iter()
            .position(|&b| b == b'-')
            .context("stream id must contain '-'")?;

        let ms: u64 = std::str::from_utf8(&s.slice(..sep))?.parse()?;
        let seq: u64 = std::str::from_utf8(&s.slice(sep + 1..))?.parse()?;

        Self::new(ms, seq).context("zero sequence number")
    }
}

// XXX: it's a bit sad that this allocates
impl From<StreamId> for rdb::String {
    #[inline]
    fn from(id: StreamId) -> Self {
        Self::from(id.to_string())
    }
}

// NOTE: fields would better be an `IndexMap` to preserve RDB read/write order
#[derive(Clone, Debug)]
pub struct Entry {
    pub(crate) id: StreamId,
    pub(crate) fields: Arc<HashMap<rdb::String, rdb::Value>>,
}

impl TryFrom<&[DataType]> for Entry {
    type Error = anyhow::Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let num_fields = (args.len() - 1) / 2;
        let mut args = args.iter().cloned();

        // TODO: support <* | id>

        let id = match args.next() {
            Some(DataType::BulkString(id) | DataType::SimpleString(id)) => {
                id.try_into().context("stream id")?
            }
            Some(arg) => bail!("XADD only accepts bulk strings as id, got {arg:?}"),
            None => bail!("XADD requires an <* | id> argument, got none"),
        };

        // NOTE: keys can only be `rdb::String`s which are immutable (just ref counted)
        #[allow(clippy::mutable_key_type)]
        let mut fields = HashMap::with_capacity(num_fields);

        while let Some(key) = args.next() {
            let (DataType::BulkString(key) | DataType::SimpleString(key)) = key else {
                bail!("field key must be a string, got {key:?}");
            };

            let Some(val) = args.next() else {
                bail!("encountered field {key:?} without a pairing value");
            };

            // TODO: relax this restriction
            let (DataType::BulkString(val) | DataType::SimpleString(val)) = val else {
                bail!("field value must be a string, got {val:?}");
            };

            let val = rdb::Value::String(val);

            fields.insert(key, val);
        }

        Ok(Self {
            id,
            fields: fields.into(),
        })
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct PendingEntry {
    pub(crate) id: StreamId,
    pub(crate) delivery_time: SystemTime,
    pub(crate) delivery_count: usize,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CData {
    pub(crate) name: rdb::String,
    pub(crate) seen_time: SystemTime,
    pub(crate) pending: Vec<StreamId>,
}

impl CData {
    #[inline]
    pub fn new(
        name: impl Into<rdb::String>,
        seen_time: SystemTime,
        pending: Vec<StreamId>,
    ) -> Self {
        Self {
            name: name.into(),
            seen_time,
            pending,
        }
    }
}

/// Consumer group metadata
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CGroup {
    pub(crate) name: rdb::String,
    pub(crate) last_entry: StreamId,
    pub(crate) pending: Vec<PendingEntry>,
    pub(crate) consumers: Vec<CData>,
}

impl CGroup {
    #[inline]
    pub fn new(
        name: impl Into<rdb::String>,
        last_entry: StreamId,
        pending: Vec<PendingEntry>,
        consumers: Vec<CData>,
    ) -> Self {
        Self {
            name: name.into(),
            last_entry,
            pending,
            consumers,
        }
    }
}

// TODO: Redis uses rax (radix trees) to store all the lists
//  - the idea is that appending and accessing is cheap at the expense of deletes
//  - also to deletes use tombstones (probably to support different views from cgroups)
#[allow(dead_code)]
#[derive(Debug)]
pub struct StreamInner {
    pub(crate) entries: Vec<Entry>,
    pub(crate) length: usize,
    pub(crate) last_entry: StreamId,
    pub(crate) cgroups: Vec<CGroup>,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Stream(Arc<Mutex<StreamInner>>);

impl Stream {
    #[inline]
    pub fn new(
        entries: Vec<Entry>,
        length: usize,
        last_entry: StreamId,
        cgroups: Vec<CGroup>,
    ) -> Self {
        Self(Arc::new(Mutex::new(StreamInner {
            entries,
            length,
            last_entry,
            cgroups,
        })))
    }
}

impl Deref for Stream {
    type Target = Mutex<StreamInner>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
