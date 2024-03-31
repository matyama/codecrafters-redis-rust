use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::Deref;

pub use reader::DataReader;
pub use writer::DataWriter;

pub(crate) mod reader;
pub(crate) mod writer;

pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]

pub(crate) const PONG: SimpleString = SimpleString(Cow::Borrowed(b"PONG"));

pub trait DataExt {
    fn cmd(self) -> SimpleString;
}

impl<'a> DataExt for Cow<'a, [u8]> {
    fn cmd(self) -> SimpleString {
        match self {
            Cow::Borrowed(data) => SimpleString(Cow::Owned(data.to_ascii_lowercase())),
            Cow::Owned(mut data) => {
                data.make_ascii_lowercase();
                SimpleString(Cow::Owned(data))
            }
        }
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct SimpleString(Cow<'static, [u8]>);

impl DataExt for SimpleString {
    #[inline]
    fn cmd(self) -> SimpleString {
        self.0.cmd()
    }
}

impl Deref for SimpleString {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct BulkString(Cow<'static, [u8]>);

impl DataExt for BulkString {
    #[inline]
    fn cmd(self) -> SimpleString {
        self.0.cmd()
    }
}

impl AsRef<[u8]> for BulkString {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for BulkString {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for BulkString {
    #[inline]
    fn from(data: Vec<u8>) -> Self {
        Self(data.into())
    }
}

#[derive(Debug)]
pub enum DataType {
    Boolean(bool),
    SimpleString(SimpleString),
    BulkString(BulkString),
    Array(VecDeque<DataType>),
}

impl DataExt for DataType {
    #[inline]
    fn cmd(self) -> SimpleString {
        match self {
            Self::SimpleString(s) => s.cmd(),
            Self::BulkString(s) => s.cmd(),
            other => SimpleString(Cow::Owned(format!("{other:?}").as_bytes().into())),
        }
    }
}

impl From<Command> for DataType {
    #[inline]
    fn from(cmd: Command) -> Self {
        match cmd {
            Command::Ping(Some(msg)) => msg,
            Command::Ping(None) => DataType::SimpleString(PONG),
            Command::Echo(msg) => msg,
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Ping(Option<DataType>),
    Echo(DataType),
}
