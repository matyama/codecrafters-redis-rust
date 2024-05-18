use bytes::Bytes;

use crate::data::{DataExt, DataType};
use crate::stream::{Entry, EntryArg};
use crate::{rdb, Command, Error};

const CMD: &str = "xadd";

const NOMKSTREAM: Bytes = Bytes::from_static(b"NOMKSTREAM");

// TODO: support other XADD options: MAXLEN/MINID, LIMIT
#[derive(Clone, Debug, Default)]
pub struct Options {
    pub(crate) no_mkstream: bool,
    len: usize,
}

impl Options {
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn into_bytes(self) -> impl Iterator<Item = Bytes> {
        OptionsBytesIter(if self.no_mkstream {
            Some(NOMKSTREAM)
        } else {
            None
        })
    }
}

impl TryFrom<&[DataType]> for Options {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let mut ops = Options::default();

        let mut args = args.iter().cloned();

        #[allow(clippy::while_let_on_iterator)]
        while let Some(arg) = args.next() {
            if arg.matches(NOMKSTREAM) {
                ops.no_mkstream = true;
                ops.len += 1;
            }

            // all options are between the key and id (i.e., before <* | id>)
            if arg.matches(b"*") || arg.contains(b'-') {
                break;
            }
        }

        Ok(ops)
    }
}

struct OptionsBytesIter(Option<Bytes>);

impl Iterator for OptionsBytesIter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let Self(no_mkstream) = self;

        no_mkstream.take()
    }
}

#[derive(Debug)]
pub struct XAdd(rdb::String, EntryArg, Options);

impl TryFrom<&[DataType]> for XAdd {
    type Error = Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let Some((key, args)) = args.split_first() else {
            return Err(Error::WrongNumArgs(CMD));
        };

        let (DataType::BulkString(key) | DataType::SimpleString(key)) = key else {
            return Err(Error::err("XADD stream key must be a string"));
        };

        let ops = Options::try_from(args)?;

        debug_assert_eq!(EntryArg::DEFAULT_CMD, CMD);
        let entry = Entry::try_from(&args[ops.len()..])?;

        Ok(Self(key.clone(), entry, ops))
    }
}

impl From<XAdd> for Command {
    #[inline]
    fn from(XAdd(key, entry, ops): XAdd) -> Self {
        Self::XAdd(key, entry, ops)
    }
}
