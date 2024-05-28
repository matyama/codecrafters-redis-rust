use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{collections::HashMap, fmt::Write};

use anyhow::anyhow;
use bytes::Bytes;
use tokio::sync::Mutex;

use crate::{rdb, DataType, Error};

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ParseIdError {
    #[error("Invalid stream ID specified as stream command argument")]
    Invalid,
    #[error("parsed incomplete stream ID: {0}")]
    Incomplete(AutoId),
    #[error("timestamp/sequence out of range: {0}")]
    OutOfRange(i64),
    #[error(transparent)]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Seq {
    Some(u64),
    None,
    #[default]
    Auto,
}

// NOTE: None ms (i.e., *) should imply Seq::Auto
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AutoId {
    /// ms | *
    pub(crate) ms: Option<u64>,
    /// seq | <none> | *
    pub(crate) seq: Seq,
}

impl AutoId {
    #[inline]
    pub const fn at(ms: u64) -> Self {
        Self {
            ms: Some(ms),
            seq: Seq::None,
        }
    }

    #[inline]
    pub fn is_zero(&self) -> bool {
        matches!(
            self,
            Self {
                ms: Some(0),
                seq: Seq::Some(0) | Seq::None
            }
        )
    }

    pub(crate) fn next(self, last: Option<StreamId>) -> StreamId {
        let last = last.unwrap_or(StreamId::ZERO);
        match self.ms {
            Some(ms) => {
                let seq = match self.seq {
                    Seq::Some(seq) => seq,
                    Seq::Auto if ms == last.ms => last.seq.wrapping_add(1),
                    Seq::Auto | Seq::None => 0, // NOTE: no sequence implies implicit 0 (XADD)
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

impl std::fmt::Display for AutoId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.ms, self.seq) {
            (Some(ms), Seq::Some(seq)) => write!(f, "{ms}-{seq}"),
            (Some(ms), Seq::None) => write!(f, "{ms}"),
            (Some(ms), Seq::Auto) => write!(f, "{ms}-*"),
            (None, Seq::Auto) => f.write_char('*'),
            (None, _) => Err(std::fmt::Error),
        }
    }
}

impl From<StreamId> for AutoId {
    #[inline]
    fn from(StreamId { ms, seq }: StreamId) -> Self {
        Self {
            ms: Some(ms),
            seq: Seq::Some(seq),
        }
    }
}

impl TryFrom<&[u8]> for AutoId {
    type Error = ParseIdError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if matches!(bytes, b"*") {
            return Ok(Self::default());
        }

        let Some(sep) = bytes.iter().position(|&b| b == b'-') else {
            let ms = std::str::from_utf8(bytes)?.parse()?;
            return Ok(Self::at(ms));
        };

        let (bytes_ms, bytes_seq) = bytes.split_at(sep);

        if bytes_ms.is_empty() {
            // bytes ~ b'-12345', so parse neg ms from bytes_seq
            let ms = std::str::from_utf8(bytes_seq)?.parse()?;
            return Err(ParseIdError::OutOfRange(ms));
        }

        let ms = std::str::from_utf8(bytes_ms)?.parse()?;

        let seq = match &bytes_seq[1..] {
            b"*" => Seq::Auto,
            seq => Seq::Some(std::str::from_utf8(seq)?.parse()?),
        };

        Ok(Self { ms: Some(ms), seq })
    }
}

impl TryFrom<rdb::String> for AutoId {
    type Error = ParseIdError;

    fn try_from(s: rdb::String) -> Result<Self, Self::Error> {
        use rdb::String::*;
        match s {
            Str(s) => s.as_ref().try_into(),
            Int8(ms) if ms >= 0 => Ok(Self::at(ms as u64)),
            Int16(ms) if ms >= 0 => Ok(Self::at(ms as u64)),
            Int32(ms) if ms >= 0 => Ok(Self::at(ms as u64)),
            Int8(ms) => Err(ParseIdError::OutOfRange(ms as i64)),
            Int16(ms) => Err(ParseIdError::OutOfRange(ms as i64)),
            Int32(ms) => Err(ParseIdError::OutOfRange(ms as i64)),
        }
    }
}

impl TryFrom<DataType> for AutoId {
    type Error = ParseIdError;

    #[inline]
    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        match data {
            DataType::SimpleString(s) | DataType::BulkString(s) => s.try_into(),
            _ => Err(ParseIdError::Invalid),
        }
    }
}

impl TryFrom<&DataType> for AutoId {
    type Error = ParseIdError;

    #[inline]
    fn try_from(data: &DataType) -> Result<Self, Self::Error> {
        data.clone().try_into()
    }
}

/// Fully resolved stream ID with optional sequence part
///
/// Note that this `Id` is essentially equivalent to a [`StreamId`] (`seq: None` here implicitly
/// implies an ID with `seq: 0`), but it's useful to preserve exact command inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Id {
    pub(crate) ms: u64,
    pub(crate) seq: Option<u64>,
}

impl Id {
    pub const ZERO: Id = Id {
        ms: 0,
        seq: Some(0),
    };

    #[inline]
    pub fn or(self, seq: u64) -> StreamId {
        StreamId {
            ms: self.ms,
            seq: self.seq.unwrap_or(seq),
        }
    }

    #[inline]
    pub fn or_first(self) -> StreamId {
        self.or(0)
    }

    #[inline]
    pub fn or_last(self) -> StreamId {
        self.or(u64::MAX)
    }
}

impl std::fmt::Display for Id {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.seq {
            Some(seq) => write!(f, "{}-{}", self.ms, seq),
            None => write!(f, "{}", self.ms),
        }
    }
}

impl TryFrom<AutoId> for Id {
    type Error = AutoId;

    #[inline]
    fn try_from(id: AutoId) -> Result<Self, Self::Error> {
        match id {
            AutoId {
                ms: Some(ms),
                seq: Seq::Some(seq),
            } => Ok(Self { ms, seq: Some(seq) }),
            AutoId {
                ms: Some(ms),
                seq: Seq::None,
            } => Ok(Self { ms, seq: None }),
            id => Err(id),
        }
    }
}

impl TryFrom<&[u8]> for Id {
    type Error = ParseIdError;

    #[inline]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        AutoId::try_from(bytes)?
            .try_into()
            .map_err(ParseIdError::Incomplete)
    }
}

impl TryFrom<rdb::String> for Id {
    type Error = ParseIdError;

    #[inline]
    fn try_from(s: rdb::String) -> Result<Self, Self::Error> {
        AutoId::try_from(s)?
            .try_into()
            .map_err(ParseIdError::Incomplete)
    }
}

impl TryFrom<&rdb::String> for Id {
    type Error = ParseIdError;

    #[inline]
    fn try_from(s: &rdb::String) -> Result<Self, Self::Error> {
        s.clone().try_into()
    }
}

impl TryFrom<DataType> for Id {
    // TODO: custom Error
    type Error = anyhow::Error;

    #[inline]
    fn try_from(data: DataType) -> Result<Self, Self::Error> {
        AutoId::try_from(data)?
            .try_into()
            .map_err(|_| anyhow!("cannot convert from an auto-generated ID"))
    }
}

impl TryFrom<&DataType> for Id {
    // TODO: custom Error
    type Error = anyhow::Error;

    #[inline]
    fn try_from(data: &DataType) -> Result<Self, Self::Error> {
        data.clone().try_into()
    }
}

impl From<StreamId> for Id {
    #[inline]
    fn from(StreamId { ms, seq }: StreamId) -> Self {
        Self { ms, seq: Some(seq) }
    }
}

/// Fully resolved stream ID
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StreamId {
    pub(crate) ms: u64,
    pub(crate) seq: u64,
}

impl StreamId {
    pub const ZERO: StreamId = StreamId { ms: 0, seq: 0 };

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
    type Error = ParseIdError;

    #[inline]
    fn try_from(s: rdb::String) -> Result<Self, Self::Error> {
        Id::try_from(s).and_then(|Id { ms, seq }| match seq {
            Some(seq) => Ok(Self { ms, seq }),
            None => Err(ParseIdError::Incomplete(AutoId::at(ms))),
        })
    }
}

impl TryFrom<&rdb::String> for StreamId {
    type Error = ParseIdError;

    #[inline]
    fn try_from(s: &rdb::String) -> Result<Self, Self::Error> {
        s.clone().try_into()
    }
}

impl From<Id> for Bytes {
    #[inline]
    fn from(id: Id) -> Self {
        id.into()
    }
}

impl From<&Id> for Bytes {
    #[inline]
    fn from(id: &Id) -> Self {
        Self::from(id.to_string())
    }
}

impl From<StreamId> for Bytes {
    #[inline]
    fn from(id: StreamId) -> Self {
        Self::from(id.to_string())
    }
}

// XXX: it's a bit sad that this allocates
impl From<StreamId> for rdb::String {
    #[inline]
    fn from(id: StreamId) -> Self {
        Self::from(id.to_string())
    }
}

// XXX: it's a bit sad that this allocates
impl From<AutoId> for rdb::String {
    #[inline]
    fn from(id: AutoId) -> Self {
        Self::from(id.to_string())
    }
}

// NOTE: fields would better be an `IndexMap` to preserve RDB read/write order
#[derive(Clone, Debug)]
pub struct Entry<Id = StreamId> {
    pub(crate) id: Id,
    pub(crate) fields: Arc<HashMap<rdb::String, rdb::Value>>,
}

pub type EntryArg = Entry<AutoId>;

impl Entry<AutoId> {
    pub(crate) const DEFAULT_CMD: &'static str = "xadd";

    #[inline]
    pub(crate) fn next(self, last: Option<StreamId>) -> Entry {
        Entry {
            id: self.id.next(last),
            fields: self.fields,
        }
    }
}

impl TryFrom<&[DataType]> for Entry<AutoId> {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let num_fields = (args.len() - 1) / 2;

        let Some((arg, args)) = args.split_first() else {
            return Err(Error::WrongNumArgs(Self::DEFAULT_CMD));
        };

        let id = AutoId::try_from(arg).map_err(|_| Error::err(ParseIdError::Invalid))?;

        // NOTE: keys can only be `rdb::String`s which are immutable (just ref counted)
        #[allow(clippy::mutable_key_type)]
        let mut fields = HashMap::with_capacity(num_fields);

        let mut args = args.iter().cloned();

        while let Some(key) = args.next() {
            let (DataType::BulkString(key) | DataType::SimpleString(key)) = key else {
                return Err(Error::err("field key must be a string"));
            };

            let Some(val) = args.next() else {
                return Err(Error::WrongNumArgs(Self::DEFAULT_CMD));
            };

            let (DataType::BulkString(val) | DataType::SimpleString(val)) = val else {
                return Err(Error::err("field value be a string"));
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

impl From<Entry<StreamId>> for DataType {
    #[inline]
    fn from(Entry { id, fields }: Entry<StreamId>) -> Self {
        DataType::array([
            DataType::string(id),
            DataType::array(
                fields
                    .iter()
                    .flat_map(|(k, v)| [DataType::string(k.clone()), DataType::string(v.clone())]),
            ),
        ])
    }
}

#[cfg(test)]
impl<Id: PartialEq> PartialEq for Entry<Id> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.fields == other.fields
    }
}

#[cfg(test)]
impl<Id: Eq> Eq for Entry<Id> {}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingEntry {
    pub(crate) id: StreamId,
    pub(crate) delivery_time: SystemTime,
    pub(crate) delivery_count: usize,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct StreamInner {
    pub(crate) entries: BTreeMap<StreamId, Entry>,
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
        entries: BTreeMap<StreamId, Entry>,
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

/// Note that this impl is blocking and as such should be used carefully (e.g., in tests)
#[cfg(test)]
impl PartialEq for Stream {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.blocking_lock().eq(&other.blocking_lock())
    }
}

#[cfg(test)]
impl Eq for Stream {}

#[cfg(test)]
mod tests {
    use super::*;
    use rdb::String::*;

    #[test]
    fn parse_id() {
        let id = rdb::String::from(b"123-45".as_ref());

        assert_eq!(
            AutoId {
                ms: Some(123),
                seq: Seq::Some(45)
            },
            AutoId::try_from(id).expect("valid ID")
        );

        for partial in [
            rdb::String::from(b"123".as_ref()),
            Int8(123),
            Int16(123),
            Int32(123),
        ] {
            assert_eq!(
                AutoId {
                    ms: Some(123),
                    seq: Seq::None,
                },
                AutoId::try_from(partial).expect("valid partial ID")
            );
        }

        // partial IDs with auto-generation
        let id = rdb::String::from(b"123-*".as_ref());
        assert_eq!(
            AutoId {
                ms: Some(123),
                seq: Seq::Auto,
            },
            AutoId::try_from(id).expect("valid auto-gen ID")
        );

        let id = rdb::String::from(b"*".as_ref());
        assert_eq!(
            AutoId::default(),
            AutoId::try_from(id).expect("valid ID gen")
        );

        for not_id in [
            rdb::String::from(b"invalid".as_ref()),
            rdb::String::from(b"*-1".as_ref()),
            Int8(-123),
            Int16(-123),
            Int32(-123),
        ] {
            StreamId::try_from(not_id).expect_err("not a valid ID");
        }
    }

    #[test]
    fn parse_stream_id() {
        use crate::stream::{ParseIdError::*, Seq};

        let id = rdb::String::from(b"123-45".as_ref());
        assert_eq!(
            StreamId { ms: 123, seq: 45 },
            StreamId::try_from(id).expect("valid ID")
        );

        let id = rdb::String::from(b"123-*".as_ref());
        assert_eq!(
            Incomplete(AutoId {
                ms: Some(123),
                seq: Seq::Auto,
            }),
            StreamId::try_from(id).expect_err("not a valid ID"),
        );

        let id = rdb::String::from(b"*".as_ref());
        assert_eq!(
            Incomplete(AutoId {
                ms: None,
                seq: Seq::Auto,
            }),
            StreamId::try_from(id).expect_err("not a valid ID"),
        );

        for incomplete in [
            rdb::String::from(b"123".as_ref()),
            Int8(123),
            Int16(123),
            Int32(123),
        ] {
            assert_eq!(
                Incomplete(AutoId {
                    ms: Some(123),
                    seq: Seq::None,
                }),
                StreamId::try_from(incomplete).expect_err("not a valid ID"),
            );
        }

        for id in [Int8(-123), Int16(-123), Int32(-123)] {
            match StreamId::try_from(id).expect_err("not a valid ID") {
                OutOfRange(-123) => {}
                err => panic!("unexpected error: {err:?}"),
            }
        }

        for id in [b"invalid".as_ref(), b"*-".as_ref()] {
            match StreamId::try_from(rdb::String::from(id)).expect_err("not a valid ID") {
                ParseIntError(_) => {}
                err => panic!("unexpected error: {err:?}"),
            }
        }
    }
}
