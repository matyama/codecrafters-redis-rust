use bytes::Bytes;

use crate::data::{DataExt, DataType};
use crate::ANY;

const NOMKSTREAM: Bytes = Bytes::from_static(b"NOMKSTREAM");

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
    type Error = anyhow::Error;

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
            if arg.matches(ANY) || arg.contains(b'-') {
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
