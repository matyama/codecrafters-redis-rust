use std::sync::{Arc, OnceLock};

use anyhow::{bail, Context as _, Result};
use bytes::Bytes;

use crate::cmd::{self, Command};
use crate::rdb;
use crate::writer::{DataSerializer, Serializer as _};
use crate::ANY;

// NOTE: immutable with cheap Clone impl
#[derive(Debug, Clone)]
pub enum DataType {
    Null,
    NullBulkString,
    Boolean(bool),
    Integer(i64),
    SimpleString(rdb::String),
    SimpleError(rdb::String),
    BulkString(rdb::String),
    Array(Arc<[DataType]>),
    Map(Arc<[(DataType, DataType)]>),
}

impl DataType {
    pub(crate) fn err(e: impl Into<rdb::String>) -> Self {
        Self::SimpleError(e.into())
    }

    #[inline]
    pub(crate) fn str(s: impl Into<rdb::String>) -> Self {
        Self::SimpleString(s.into())
    }

    #[inline]
    pub(crate) fn string(s: impl Into<rdb::String>) -> Self {
        Self::BulkString(s.into())
    }

    #[inline]
    pub(crate) fn array<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self::Array(items.into_iter().collect())
    }

    #[inline]
    pub(crate) fn map<I>(items: I) -> Self
    where
        I: IntoIterator<Item = (Self, Self)>,
    {
        Self::Map(items.into_iter().collect())
    }

    #[inline]
    pub(crate) fn cmd<T, I>(args: I) -> Self
    where
        T: Into<rdb::String>,
        I: IntoIterator<Item = T>,
    {
        Self::array(args.into_iter().map(Self::string))
    }

    /// Returns a static reference to the 'REPLCONF GETACK *' command and its serialized size
    pub(crate) fn replconf_getack() -> &'static (Self, usize) {
        static REPLCONF_GETACK: OnceLock<(DataType, usize)> = OnceLock::new();
        REPLCONF_GETACK.get_or_init(|| {
            let data = Self::from(Command::Replconf(cmd::replconf::Conf::GetAck(ANY)));
            let size = DataSerializer::serialized_size(&data)
                .expect("'REPLCONF GETACK *' should be serializable");
            debug_assert!(size > 0, "'{data:?}' got serialized to 0B");
            (data, size)
        })
    }

    pub(crate) fn parse_int(self) -> Result<Self> {
        use rdb::String::*;
        match self {
            Self::Null => bail!("null cannot be converted to an integer"),
            Self::Boolean(b) => Ok(Self::Integer(b.into())),
            i @ Self::Integer(_) => Ok(i),
            Self::SimpleString(s) | Self::BulkString(s) => match s {
                Str(s) => {
                    let s = std::str::from_utf8(&s)
                        .with_context(|| format!("{s:?} is not a UTF-8 string"))?;
                    s.parse()
                        .map(Self::Integer)
                        .with_context(|| format!("'{s}' does not represent an integer"))
                }
                Int8(i) => Ok(Self::Integer(i as i64)),
                Int16(i) => Ok(Self::Integer(i as i64)),
                Int32(i) => Ok(Self::Integer(i as i64)),
            },
            Self::NullBulkString => bail!("null bulk string cannot be converted to an integer"),
            Self::SimpleError(_) => bail!("simple error cannot be converted to an integer"),
            Self::Array(_) => bail!("array cannot be converted to an integer"),
            Self::Map(_) => bail!("map cannot be converted to an integer"),
        }
    }
}

const CASE_SHIFT: u8 = b'a'.abs_diff(b'A');

// TODO: extend to multi-target matcher, possibly with different comparators
pub(crate) fn compare<F, D, T>(cmp: F, data: D, target: T) -> bool
where
    F: Fn(u8, u8) -> bool,
    D: AsRef<[u8]>,
    T: AsRef<[u8]>,
{
    let data = data.as_ref();
    let target = target.as_ref();

    if data.len() != target.len() {
        return false;
    }

    data.iter().zip(target.iter()).all(|(&x, &y)| cmp(x, y))
}

fn ignore_case_eq(x: u8, y: u8) -> bool {
    x == y || (x.is_ascii_alphabetic() && y.is_ascii_alphabetic() && x.abs_diff(y) == CASE_SHIFT)
}

/// Returns `true` iff `data` match given target ignoring casing of ASCII alphabetic characters
#[inline]
pub(crate) fn matches<D, T>(data: D, target: T) -> bool
where
    D: AsRef<[u8]>,
    T: AsRef<[u8]>,
{
    compare(ignore_case_eq, data, target)
}

pub trait DataExt {
    // TODO: deprecate in favor of matches
    // NOTE: this could probably benefit from small vec optimization
    fn to_uppercase(&self) -> Vec<u8>;

    fn matches(&self, target: impl AsRef<[u8]>) -> bool;
}

impl DataExt for Bytes {
    #[inline]
    fn to_uppercase(&self) -> Vec<u8> {
        self.to_ascii_uppercase()
    }

    #[inline]
    fn matches(&self, target: impl AsRef<[u8]>) -> bool {
        matches(self, target)
    }
}

impl DataExt for rdb::String {
    fn to_uppercase(&self) -> Vec<u8> {
        use rdb::String::*;
        let mut s = match self {
            Str(s) => return s.to_uppercase(),
            Int8(i) => i.to_string(),
            Int16(i) => i.to_string(),
            Int32(i) => i.to_string(),
        };
        s.make_ascii_uppercase();
        s.into()
    }

    /// Returns `true` iff this string matches `other` assuming it's in uppercase
    #[inline]
    fn matches(&self, target: impl AsRef<[u8]>) -> bool {
        match self {
            Self::Str(s) => matches(s, target),
            _ => false,
        }
    }
}

impl DataExt for DataType {
    fn to_uppercase(&self) -> Vec<u8> {
        match self {
            Self::NullBulkString => vec![],
            Self::SimpleString(s) => s.to_uppercase(),
            Self::BulkString(s) => s.to_uppercase(),
            other => {
                let mut other = format!("{other:?}");
                other.make_ascii_uppercase();
                other.into()
            }
        }
    }

    #[inline]
    fn matches(&self, target: impl AsRef<[u8]>) -> bool {
        match self {
            Self::BulkString(s) | Self::SimpleString(s) => s.matches(target),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{CONFIG, ECHO, PING};

    use super::*;

    #[test]
    fn matching_string() {
        // positive example
        let ping = Bytes::from_static(b"PiNg");
        assert!(ping.matches(PING), "{ping:?} ~ {PING:?}");

        // PING but each letter shifted by 32 (i.e., `|'a' - 'A'|`)
        let data = Bytes::from_static(b"0).'");
        assert!(!data.matches(PING), "{ping:?} ~ {PING:?}");

        // different commands, same length and casing
        assert!(!ECHO.matches(PING), "{ECHO:?} ~ {PING:?}");

        // different commands, different length, same casing
        assert!(!CONFIG.matches(PING), "{CONFIG:?} ~ {PING:?}");
    }
}
