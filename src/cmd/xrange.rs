use std::ops::Bound;

use anyhow::{bail, Context};
use bytes::Bytes;

use crate::data::{DataExt, DataType};
use crate::rdb;
use crate::stream::{Id, StreamId};
use crate::{MAX, MIN};

#[derive(Clone, Debug)]
pub enum Bounds {
    Start(Bound<Id>),
    End(Bound<Id>),
}

impl std::fmt::Display for Bounds {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Bound::*;
        match self {
            // SAFETY: MIN/MAX is const and valid UTF-8
            Self::Start(Unbounded) => f.write_str(unsafe { std::str::from_utf8_unchecked(&MIN) }),
            Self::End(Unbounded) => f.write_str(unsafe { std::str::from_utf8_unchecked(&MAX) }),
            Self::Start(Included(id)) | Self::End(Included(id)) => write!(f, "{id}"),
            Self::Start(Excluded(id)) | Self::End(Excluded(id)) => write!(f, "({id}"),
        }
    }
}

impl From<Bounds> for Bound<StreamId> {
    #[inline]
    fn from(b: Bounds) -> Self {
        use {Bound::*, Bounds::*};
        match b {
            Start(Unbounded) | End(Unbounded) => Unbounded,
            Start(Included(id)) => Included(id.or_first()),
            Start(Excluded(id)) => Excluded(id.or_first()),
            End(Included(id)) => Included(id.or_last()),
            End(Excluded(id)) => Excluded(id.or_last()),
        }
    }
}

// XXX: it's a bit sad that this allocates
impl From<Bounds> for rdb::String {
    #[inline]
    fn from(bounds: Bounds) -> Self {
        Self::from(bounds.to_string())
    }
}

impl TryFrom<rdb::String> for Bound<Id> {
    type Error = anyhow::Error;

    fn try_from(s: rdb::String) -> Result<Self, Self::Error> {
        use {rdb::String::*, Bound::*};
        match s {
            s if s.matches(MIN) || s.matches(MAX) => Ok(Unbounded),

            Str(s) if s.starts_with(b"(") => {
                let s = s.strip_prefix(b"(").unwrap();
                Id::try_from(s).map(Excluded).context("bound excluded")
            }

            s => Id::try_from(s).map(Included).context("bound included"),
        }
    }
}

impl TryFrom<&rdb::String> for Bound<Id> {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(s: &rdb::String) -> Result<Self, Self::Error> {
        s.clone().try_into()
    }
}

impl TryFrom<&DataType> for Bound<Id> {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(data: &DataType) -> Result<Self, Self::Error> {
        let (DataType::BulkString(s) | DataType::SimpleString(s)) = data else {
            bail!("expected a string, got {data:?}");
        };
        Self::try_from(s)
    }
}

#[derive(Clone, Debug)]
pub struct Range {
    pub(crate) start: Bound<Id>,
    pub(crate) end: Bound<Id>,
}

impl Range {
    #[inline]
    pub fn lopen(start: Id) -> Self {
        Self {
            start: Bound::Excluded(start),
            end: Bound::Unbounded,
        }
    }
}

impl TryFrom<&[DataType]> for Range {
    type Error = anyhow::Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let [start, end, ..] = args else {
            bail!("range is composed of two arguments: <start> <end>, got: {args:?}");
        };

        Ok(Self {
            start: Bound::try_from(start).context("range start")?,
            end: Bound::try_from(end).context("range end")?,
        })
    }
}

// XXX: it's a bit sad that this allocates
impl From<Range> for (rdb::String, rdb::String) {
    #[inline]
    fn from(Range { start, end }: Range) -> Self {
        use Bounds::*;
        (
            rdb::String::from(Start(start).to_string()),
            rdb::String::from(End(end).to_string()),
        )
    }
}

#[derive(Clone, Debug)]
pub struct IdRange {
    pub(crate) start: Bound<StreamId>,
    pub(crate) end: Bound<StreamId>,
}

impl From<Range> for Option<IdRange> {
    fn from(Range { start, end }: Range) -> Self {
        use {Bound::*, Bounds::*};
        match (Start(start).into(), End(end).into()) {
            (Included(s) | Excluded(s), Included(e) | Excluded(e)) if s > e => None,
            (start, end) => Some(IdRange { start, end }),
        }
    }
}

impl std::ops::RangeBounds<StreamId> for IdRange {
    #[inline]
    fn start_bound(&self) -> Bound<&StreamId> {
        self.start.as_ref()
    }

    #[inline]
    fn end_bound(&self) -> Bound<&StreamId> {
        self.end.as_ref()
    }
}

const COUNT: Bytes = Bytes::from_static(b"COUNT");

#[derive(Clone, Debug, Default, PartialEq)]
#[repr(transparent)]
pub struct Count(pub(crate) Option<usize>);

impl Count {
    pub const ZERO: Count = Count(Some(0));
}

impl From<Count> for Option<usize> {
    #[inline]
    fn from(Count(count): Count) -> Self {
        count
    }
}

impl TryFrom<&[DataType]> for Count {
    type Error = anyhow::Error;

    fn try_from(args: &[DataType]) -> Result<Self, Self::Error> {
        let mut count = None;

        let mut args = args.iter().cloned();

        #[allow(clippy::while_let_on_iterator)]
        while let Some(arg) = args.next() {
            if arg.matches(COUNT) {
                let Some(cnt) = args.next() else {
                    bail!("COUNT option is missing a value");
                };

                match cnt.parse_int().context("COUNT value is not an integer")? {
                    DataType::Integer(c) if c.is_positive() => count.replace(c as usize),
                    DataType::Integer(_) => count.replace(0),
                    val => bail!("COUNT with an unexpected value: {val:?}"),
                };

                // NOTE: there are no options other than COUNT
                break;
            }
        }

        Ok(Count(count))
    }
}