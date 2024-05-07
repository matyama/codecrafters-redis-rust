use std::ops::Deref;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fmt::Write};

use anyhow::{bail, Context as _};
use tokio::sync::Mutex;

use crate::data::DataExt;
use crate::{rdb, DataType};

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Id {
    pub(crate) ms: Option<u64>,
    pub(crate) seq: Option<u64>,
}

impl Id {
    pub const ZERO: Id = Id {
        ms: Some(0),
        seq: Some(0),
    };

    #[inline]
    pub fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    pub(crate) fn next(self, last: Option<StreamId>) -> StreamId {
        let last = last.unwrap_or(StreamId::ZERO);
        match self.ms {
            Some(ms) => {
                let seq = match self.seq {
                    Some(seq) => seq,
                    None if ms == last.ms => last.seq.wrapping_add(1),
                    None => 0,
                };
                StreamId { ms, seq }
            }

            None => {
                let ms = timestamp_ms();
                if ms > last.ms {
                    StreamId { ms, seq: 0 }
                } else {
                    last.wrapping_inc()
                }
            }
        }
    }
}

impl std::fmt::Display for Id {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.ms, self.seq) {
            (Some(ms), Some(seq)) => write!(f, "{ms}-{seq}"),
            (Some(ms), None) => write!(f, "{ms}-*"),
            (None, Some(_)) => Err(std::fmt::Error),
            (None, None) => f.write_char('*'),
        }
    }
}

impl From<StreamId> for Id {
    #[inline]
    fn from(StreamId { ms, seq }: StreamId) -> Self {
        Self {
            ms: Some(ms),
            seq: Some(seq),
        }
    }
}

// XXX: it's a bit sad that this allocates
impl From<Id> for rdb::String {
    #[inline]
    fn from(id: Id) -> Self {
        Self::from(id.to_string())
    }
}

impl TryFrom<rdb::String> for Id {
    type Error = anyhow::Error;

    fn try_from(s: rdb::String) -> Result<Self, Self::Error> {
        use rdb::String::*;

        let Str(s) = s else {
            bail!("cannot parse stream id from {s:?}");
        };

        let sep = match s.iter().position(|&b| b == b'-') {
            Some(sep) => sep,
            None if s.matches("*") => return Ok(Self::default()),
            None => bail!("ID must either be '*' or contain '-'"),
        };

        let ms = match std::str::from_utf8(&s.slice(..sep))? {
            "*" => None,
            ms => Some(ms.parse()?),
        };

        let seq = match std::str::from_utf8(&s.slice(sep + 1..))? {
            "*" => None,
            seq => Some(seq.parse()?),
        };

        Ok(Self { ms, seq })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };
    pub const MIN: StreamId = StreamId { ms: 0, seq: 1 };

    #[inline]
    pub fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    /// Increment this ID, wrapping around to 0-0 if any overflow would occur
    fn wrapping_inc(self) -> Self {
        if let Some(seq) = self.seq.checked_add(1) {
            Self { ms: self.ms, seq }
        } else {
            Self {
                ms: self.ms.wrapping_add(1),
                seq: 0,
            }
        }
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

        let ms = std::str::from_utf8(&s.slice(..sep))?.parse()?;
        let seq = std::str::from_utf8(&s.slice(sep + 1..))?.parse()?;

        Ok(Self { ms, seq })
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
pub struct Entry<Id = StreamId> {
    pub(crate) id: Id,
    pub(crate) fields: Arc<HashMap<rdb::String, rdb::Value>>,
}

pub type EntryArg = Entry<Id>;

impl Entry<Id> {
    #[inline]
    pub(crate) fn next(self, last: Option<StreamId>) -> Entry {
        Entry {
            id: self.id.next(last),
            fields: self.fields,
        }
    }
}

impl TryFrom<&[DataType]> for Entry<Id> {
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
