use std::str::FromStr;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::cmd::{self, Command};
use crate::writer::{DataSerializer, Serializer as _};
use crate::{rdb, Error, ANY};

const NEG: &[u8] = b"-";

// NOTE: immutable with cheap Clone impl
#[derive(Debug, Clone)]
pub enum DataType {
    Null,
    NullBulkString,
    NullBulkError,
    Boolean(bool),
    Integer(i64),
    SimpleString(rdb::String),
    SimpleError(rdb::String),
    BulkString(rdb::String),
    BulkError(rdb::String),
    Array(Arc<[DataType]>),
    Map(Arc<[(DataType, DataType)]>),
}

impl DataType {
    #[inline]
    pub(crate) fn is_replicable(&self) -> bool {
        !self.is_err() && !self.is_null()
    }

    #[inline]
    pub(crate) fn is_err(&self) -> bool {
        matches!(self, Self::SimpleError(_) | Self::BulkError(_))
    }

    #[inline]
    pub(crate) fn is_null(&self) -> bool {
        matches!(
            self,
            Self::Null | Self::NullBulkString | Self::NullBulkError
        )
    }

    #[inline]
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
    pub(crate) fn error(s: impl Into<rdb::String>) -> Self {
        Self::BulkError(s.into())
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

    pub(crate) fn parse_int(self) -> Result<Self, Error> {
        use rdb::String::*;
        match self {
            Self::Boolean(b) => Ok(Self::Integer(b.into())),
            i @ Self::Integer(_) => Ok(i),
            Self::SimpleString(s) | Self::BulkString(s) => match s {
                Str(s) => s.parse().map(Self::Integer),
                Int8(i) => Ok(Self::Integer(i as i64)),
                Int16(i) => Ok(Self::Integer(i as i64)),
                Int32(i) => Ok(Self::Integer(i as i64)),
            },
            _ => Err(Error::VAL_NOT_INT),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null | Self::NullBulkString | Self::NullBulkError => Ok(()),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Integer(i) => write!(f, "{i}"),
            Self::SimpleString(s) | Self::BulkString(s) => write!(f, "{s}"),
            Self::SimpleError(s) | Self::BulkError(s) => write!(f, "{s}"),
            Self::Array(items) => write!(f, "{}", Args(items)),
            Self::Map(items) => {
                let len = items.len();
                for (i, (key, val)) in items.iter().enumerate() {
                    write!(f, "'{key}:{val}'{}", if i < len { " " } else { "" })?;
                }
                Ok(())
            }
        }
    }
}

macro_rules! impl_try_from_data {
    ( $($int:ty),+ ) => {
        $(
            impl TryFrom<DataType> for $int {
                type Error = Error;

                fn try_from(data: DataType) -> Result<Self, Self::Error> {
                    use {rdb::String::*, DataType::*};
                    match data {
                        Integer(i) if i > 0 => Ok(i as $int),
                        Integer(_) => Err(Error::VAL_NEG_INT),
                        SimpleString(Int8(i)) | BulkString(Int8(i)) if i >= 0 => Ok(i as $int),
                        SimpleString(Int8(_)) | BulkString(Int8(_)) => Err(Error::VAL_NEG_INT),
                        SimpleString(Int16(i)) | BulkString(Int16(i)) if i >= 0 => Ok(i as $int),
                        SimpleString(Int16(_)) | BulkString(Int16(_)) => Err(Error::VAL_NEG_INT),
                        SimpleString(Int32(i)) | BulkString(Int32(i)) if i >= 0 => Ok(i as $int),
                        SimpleString(Int32(_)) | BulkString(Int32(_)) => Err(Error::VAL_NEG_INT),
                        SimpleString(Str(s)) | BulkString(Str(s))
                            if s.starts_with(NEG) && s != NEG => {
                                Err(Error::VAL_NEG_INT)
                            }
                        SimpleString(Str(s)) | BulkString(Str(s)) => s.parse(),
                        _ => Err(Error::VAL_NOT_INT),
                    }
                }
            }
        )+
    };
}

impl_try_from_data!(u64, usize);

#[derive(Debug)]
#[repr(transparent)]
pub struct Args<'a>(pub(crate) &'a [DataType]);

impl std::ops::Deref for Args<'_> {
    type Target = [DataType];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl std::fmt::Display for Args<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.len();
        for (i, item) in self.iter().map(DataType::to_string).enumerate() {
            write!(f, "'{item}'{}", if i < len { " " } else { "" })?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Keys(Arc<[rdb::String]>);

impl std::ops::Deref for Keys {
    type Target = [rdb::String];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&[DataType]> for Keys {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        args.iter()
            .cloned()
            .map(|arg| match arg {
                DataType::BulkString(arg) | DataType::SimpleString(arg) => Ok(arg),
                // TODO: custom error variant
                arg => Err(Error::Err(format!("keys must be strings, got {arg:?}"))),
            })
            .collect::<Result<_, Self::Error>>()
            .map(Self)
    }
}

pub(crate) trait ParseInt {
    fn parse<T: FromStr>(self) -> Result<T, Error>;
}

impl ParseInt for &[u8] {
    #[inline]
    fn parse<T: FromStr>(self) -> Result<T, Error> {
        std::str::from_utf8(self)
            .map_err(|_| Error::VAL_NOT_INT)
            .and_then(|s| s.parse::<T>().map_err(|_| Error::VAL_NOT_INT))
    }
}

impl ParseInt for Bytes {
    #[inline]
    fn parse<T: FromStr>(self) -> Result<T, Error> {
        self.as_ref().parse()
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

    fn contains(&self, target: u8) -> bool;
}

impl<'a> DataExt for &'a [u8] {
    #[inline]
    fn to_uppercase(&self) -> Vec<u8> {
        self.to_ascii_uppercase()
    }

    #[inline]
    fn matches(&self, target: impl AsRef<[u8]>) -> bool {
        matches(self, target)
    }

    #[inline]
    fn contains(&self, target: u8) -> bool {
        self.as_ref().contains(&target)
    }
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

    #[inline]
    fn contains(&self, target: u8) -> bool {
        self.as_ref().contains(&target)
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

    #[inline]
    fn contains(&self, target: u8) -> bool {
        match self {
            Self::Str(s) => s.contains(target),
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

    #[inline]
    fn contains(&self, target: u8) -> bool {
        match self {
            Self::BulkString(s) | Self::SimpleString(s) => s.contains(target),
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

    #[test]
    fn string_contains() {
        let id = Bytes::from_static(b"123-4");
        assert!(id.contains(b'-'), "'-' in {id:?}");

        let key = Bytes::from_static(b"some_key");
        assert!(!key.contains(b'-'), "'-' not in {key:?}");
    }
}
