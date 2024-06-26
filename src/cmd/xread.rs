use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::data::{DataExt, DataType, Keys};
use crate::{stream, Command, Error};

const CMD: &str = "xread";

const BLOCK: Bytes = Bytes::from_static(b"BLOCK");
const COUNT: Bytes = Bytes::from_static(b"COUNT");

const FUTURE: Bytes = Bytes::from_static(b"$");

pub(crate) const STREAMS: Bytes = Bytes::from_static(b"STREAMS");

#[derive(Clone, Copy, Debug)]
pub enum Id {
    Future,
    Explicit(stream::Id),
}

impl Id {
    #[inline]
    pub(crate) fn unwrap_or(self, id: impl Into<stream::Id>) -> stream::Id {
        match self {
            Self::Future => id.into(),
            Self::Explicit(id) => id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Ids(Arc<[Id]>);

impl Ids {
    #[inline]
    pub fn iter_bytes(&self) -> impl Iterator<Item = Bytes> + '_ {
        self.iter().map(|id| match id {
            Id::Future => FUTURE,
            Id::Explicit(id) => id.into(),
        })
    }
}

impl std::ops::Deref for Ids {
    type Target = [Id];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<&[DataType]> for Ids {
    type Error = Error;

    fn try_from(ids: &[DataType]) -> Result<Self, Self::Error> {
        ids.iter()
            .map(|id| {
                if id.matches(FUTURE) {
                    Ok(Id::Future)
                } else {
                    id.try_into().map(Id::Explicit).map_err(|_| {
                        Error::err("Invalid stream ID specified as stream command argument")
                    })
                }
            })
            .collect::<Result<_, Self::Error>>()
            .map(Self)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Options {
    pub(crate) count: Option<usize>,
    pub(crate) block: Option<Duration>,
}

impl Options {
    #[inline]
    pub fn len(&self) -> usize {
        2 * (self.count.is_some() as usize + self.block.is_some() as usize)
    }

    pub fn into_bytes(self) -> impl Iterator<Item = Bytes> {
        OptionsBytesIter(
            self.count.map(CountBytesIter::from),
            self.block.map(BlockBytesIter::from),
        )
    }
}

impl TryFrom<&[DataType]> for Options {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let mut ops = Options::default();

        let mut args = args.iter().cloned();

        while let Some(arg) = args.next() {
            if arg.matches(COUNT) {
                let Some(cnt) = args.next() else {
                    return Err(Error::VAL_NOT_INT);
                };

                let DataType::Integer(cnt) = cnt.parse_int()? else {
                    unreachable!("DataType::parse_int returns only integers");
                };

                ops.count.replace(cnt.max(0) as usize);

                continue;
            }

            if arg.matches(BLOCK) {
                let Some(ms) = args.next() else {
                    return Err(Error::NotInt("timeout"));
                };

                let ms = u64::try_from(ms).map_err(|err| match err {
                    Error::NotInt(_) => Error::err("timeout is not an integer or out of range"),
                    Error::NegInt(_) => Error::err("timeout is negative"),
                    err => unreachable!("u64::try_from(ms) returned unexpected {err:?}"),
                })?;

                ops.block.replace(Duration::from_millis(ms));

                continue;
            }

            // all options end with a STREAMS keyword
            if arg.matches(STREAMS) {
                break;
            }
        }

        Ok(ops)
    }
}

struct CountBytesIter(Option<Bytes>, Option<usize>);

impl Iterator for CountBytesIter {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let Self(op, cnt) = self;
        op.take()
            .or_else(|| cnt.take().map(|cnt| cnt.to_string().into()))
    }
}

impl From<usize> for CountBytesIter {
    #[inline]
    fn from(count: usize) -> Self {
        Self(Some(COUNT), Some(count))
    }
}

struct BlockBytesIter(Option<Bytes>, Option<Duration>);

impl Iterator for BlockBytesIter {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let Self(op, timeout) = self;
        op.take()
            .or_else(|| timeout.take().map(|d| d.as_millis().to_string().into()))
    }
}

impl From<Duration> for BlockBytesIter {
    #[inline]
    fn from(timeout: Duration) -> Self {
        Self(Some(BLOCK), Some(timeout))
    }
}

struct OptionsBytesIter(Option<CountBytesIter>, Option<BlockBytesIter>);

impl Iterator for OptionsBytesIter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let Self(count, block) = self;

        if let Some(it) = count {
            if let item @ Some(_) = it.next() {
                return item;
            } else {
                count.take();
            }
        }

        if let Some(it) = block {
            if let item @ Some(_) = it.next() {
                return item;
            } else {
                block.take();
            }
        }

        None
    }
}

#[derive(Debug)]
pub struct XRead(pub Options, pub Keys, pub Ids);

impl TryFrom<&[DataType]> for XRead {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        // min command: XREAD STREAMS <key> <id>
        if args.len() < 3 {
            return Err(Error::WrongNumArgs(CMD));
        }

        let ops = Options::try_from(args)?;

        // check for the STREAMS keyword
        match args.get(ops.len()) {
            None => return Err(Error::WrongNumArgs(CMD)),
            Some(arg) if !arg.matches(STREAMS) => return Err(Error::Syntax),
            _ => {}
        }

        // skip over options (including STREAMS)
        let args = &args[ops.len() + 1..];

        match args.len() {
            0 => return Err(Error::Syntax),
            1 => return Err(Error::WrongNumArgs(CMD)),
            n if n % 2 != 0 => {
                return Err(Error::UnbalancedStreams {
                    cmd: CMD,
                    msg: "for each stream key an ID or '$' must be specified",
                });
            }
            _ => {}
        }

        let (keys, ids) = args.split_at(args.len() / 2);
        let keys = Keys::try_from(keys)?;
        let ids = Ids::try_from(ids)?;

        Ok(XRead(ops, keys, ids))
    }
}

impl From<XRead> for Command {
    #[inline]
    fn from(XRead(ops, keys, ids): XRead) -> Self {
        Self::XRead(ops, keys, ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! cmd {
        ($cmd:expr) => {
            $cmd.split_whitespace()
                .map(DataType::string)
                .collect::<Arc<[_]>>()
        };
    }

    macro_rules! try_parse_ops {
        ( $($cmd:expr),+ ) => {
            $(
                let cmd = cmd!($cmd);
                Options::try_from(cmd.as_ref()).expect_err("parsing should fail");
            )+
        };

        ( $( $cmd:expr => $expected:expr ),+ ) => {
            $(
                let cmd = cmd!($cmd);
                let actual = Options::try_from(cmd.as_ref()).expect("options parsed");
                assert_eq!($expected, actual);
            )+
        };
    }

    #[test]
    fn parse_options() {
        try_parse_ops! {
            "XREAD COUNT 10 BLOCK 500 STREAMS stream1 stream2 $" => Options {
                count: Some(10),
                block: Some(Duration::from_millis(500)),
            },
            "XREAD COUNT 10" => Options {
                count: Some(10),
                block: None,
            },
            "XREAD COUNT -1" => Options {
                count: Some(0),
                block: None,
            },
            "XREAD BLOCK 500" => Options {
                count: None,
                block: Some(Duration::from_millis(500)),
            },
            "XREAD STREAMS some-stream 123" => Options {
                count: None,
                block: None,
            },
            "" => Options {
                count: None,
                block: None,
            }
        }

        try_parse_ops! {
            "XREAD COUNT STREAMS stream $",
            "XREAD BLOCK STREAMS stream $",
            "XREAD COUNT abc STREAMS stream $",
            "XREAD BLOCK abc STREAMS stream $",
            "XREAD BLOCK -1 STREAMS stream $"
        }
    }
}
