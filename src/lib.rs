use std::collections::VecDeque;

use anyhow::{bail, Context, Result};
use bytes::Bytes;

pub use cmd::Command;
pub use reader::DataReader;
pub use store::Store;
pub use writer::DataWriter;

pub(crate) mod cmd;
pub(crate) mod reader;
pub(crate) mod store;
pub(crate) mod writer;

pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
pub(crate) const NULL: &[u8] = b"_\r\n";

// NOTE: this is based on the codecrafters examples
pub const PROTOCOL: Protocol = Protocol::RESP2;

pub enum Protocol {
    RESP2,
    RESP3,
}

pub trait DataExt {
    // NOTE: this could probably benefit from small vec optimization
    fn to_uppercase(&self) -> Vec<u8>;
}

impl DataExt for Bytes {
    #[inline]
    fn to_uppercase(&self) -> Vec<u8> {
        self.to_ascii_uppercase()
    }
}

#[derive(Debug)]
pub enum DataType {
    Null,
    NullBulkString,
    Boolean(bool),
    Integer(i64),
    SimpleString(Bytes),
    SimpleError(Bytes),
    BulkString(Bytes),
    Array(VecDeque<DataType>),
}

impl DataType {
    pub(crate) fn parse_int(self) -> Result<Self> {
        match self {
            Self::Null => bail!("null cannot be converted to an integer"),
            Self::Boolean(b) => Ok(Self::Integer(b.into())),
            i @ Self::Integer(_) => Ok(i),
            Self::SimpleString(s) | Self::BulkString(s) => {
                let s = std::str::from_utf8(&s)
                    .with_context(|| format!("{s:?} is not a UTF-8 string"))?;
                s.parse()
                    .map(Self::Integer)
                    .with_context(|| format!("'{s}' does not represent an integer"))
            }
            Self::NullBulkString => bail!("null bulk string cannot be converted to an integer"),
            Self::SimpleError(_) => bail!("simple error cannot be converted to an integer"),
            Self::Array(_) => bail!("array cannot be converted to an integer"),
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
}
