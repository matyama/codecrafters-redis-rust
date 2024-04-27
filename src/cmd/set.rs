use anyhow::{bail, ensure, Result};
use bytes::Bytes;
use tokio::time::Duration;

use crate::{DataExt as _, DataType, EMPTY, GET};

const EX: Bytes = Bytes::from_static(b"EX");
const PX: Bytes = Bytes::from_static(b"PX");
const EXAT: Bytes = Bytes::from_static(b"EXAT");
const PXAT: Bytes = Bytes::from_static(b"PXAT");
const KEEPTTL: Bytes = Bytes::from_static(b"KEEPTTL");

const NX: Bytes = Bytes::from_static(b"NX");
const XX: Bytes = Bytes::from_static(b"XX");

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
pub enum Expiry {
    /// Set the specified expire time, in seconds
    EX(Duration),
    /// Set the specified expire time, in milliseconds
    PX(Duration),
    /// Set the specified Unix time at which the key will expire, in seconds
    #[allow(dead_code)]
    EXAT(i64),
    /// Set the specified Unix time at which the key will expire, in milliseconds
    #[allow(dead_code)]
    PXAT(i64),
    /// Retain the time to live associated with the key
    KeepTTL,
}

impl From<Expiry> for [Bytes; 2] {
    #[inline]
    fn from(expiry: Expiry) -> Self {
        match expiry {
            Expiry::EX(ex) => [EX, ex.as_secs().to_string().into()],
            Expiry::PX(px) => [PX, px.as_millis().to_string().into()],
            Expiry::EXAT(exat) => [EXAT, exat.to_string().into()],
            Expiry::PXAT(pxat) => [PXAT, pxat.to_string().into()],
            Expiry::KeepTTL => [KEEPTTL, EMPTY],
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
enum ExpiryBytesIter {
    EX(Option<Bytes>, Option<Duration>),
    PX(Option<Bytes>, Option<Duration>),
    EXAT(Option<Bytes>, Option<i64>),
    PXAT(Option<Bytes>, Option<i64>),
    KeepTTL(Option<Bytes>),
}

impl From<Expiry> for ExpiryBytesIter {
    #[inline]
    fn from(exp: Expiry) -> Self {
        match exp {
            Expiry::EX(ex) => Self::EX(Some(EX), Some(ex)),
            Expiry::PX(px) => Self::PX(Some(PX), Some(px)),
            Expiry::EXAT(exat) => Self::EXAT(Some(EXAT), Some(exat)),
            Expiry::PXAT(pxat) => Self::PXAT(Some(PXAT), Some(pxat)),
            Expiry::KeepTTL => Self::KeepTTL(Some(KEEPTTL)),
        }
    }
}

impl Iterator for ExpiryBytesIter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::EX(op, ex) => op
                .take()
                .or_else(|| ex.take().map(|ex| ex.as_secs().to_string().into())),
            Self::PX(op, px) => op
                .take()
                .or_else(|| px.take().map(|px| px.as_millis().to_string().into())),
            Self::EXAT(op, exat) => op
                .take()
                .or_else(|| exat.take().map(|exat| exat.to_string().into())),
            Self::PXAT(op, pxat) => op
                .take()
                .or_else(|| pxat.take().map(|pxat| pxat.to_string().into())),
            Self::KeepTTL(op) => op.take(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Condition {
    /// Only set the key if it does not already exist
    NX,
    /// Only set the key if it already exists
    XX,
}

impl From<Condition> for Bytes {
    #[inline]
    fn from(cond: Condition) -> Self {
        match cond {
            Condition::NX => NX,
            Condition::XX => XX,
        }
    }
}

#[repr(transparent)]
struct CondBytesIter(Option<Bytes>);

impl From<Condition> for CondBytesIter {
    #[inline]
    fn from(cond: Condition) -> Self {
        Self(Some(cond.into()))
    }
}

impl Iterator for CondBytesIter {
    type Item = Bytes;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.take()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Options {
    pub(crate) exp: Option<Expiry>,
    pub(crate) cond: Option<Condition>,
    /// Return the old string stored at key, or nil if key did not exist
    pub(crate) get: bool,
}

impl Options {
    #[inline]
    pub fn with_ex(&mut self, ex: u64) -> Result<()> {
        ensure!(self.exp.is_none(), "conflicting option {:?}", self.exp);
        self.exp = Some(Expiry::EX(Duration::from_secs(ex)));
        Ok(())
    }

    #[inline]
    pub fn with_px(&mut self, px: u64) -> Result<()> {
        ensure!(self.exp.is_none(), "conflicting option {:?}", self.exp);
        self.exp = Some(Expiry::PX(Duration::from_millis(px)));
        Ok(())
    }

    #[inline]
    pub fn with_keep_ttl(&mut self) -> Result<()> {
        ensure!(self.exp.is_none(), "conflicting option {:?}", self.exp);
        self.exp = Some(Expiry::KeepTTL);
        Ok(())
    }

    #[inline]
    pub fn with_nx(&mut self) -> Result<()> {
        ensure!(self.cond.is_none(), "conflicting option {:?}", self.cond);
        self.cond = Some(Condition::NX);
        Ok(())
    }

    #[inline]
    pub fn with_xx(&mut self) -> Result<()> {
        ensure!(self.cond.is_none(), "conflicting option {:?}", self.exp);
        self.cond = Some(Condition::XX);
        Ok(())
    }

    #[inline]
    pub fn with_get(&mut self) {
        self.get = true;
    }

    pub fn into_bytes(self) -> impl Iterator<Item = Bytes> {
        OptionsBytesIter(
            self.exp.map(ExpiryBytesIter::from),
            self.cond.map(CondBytesIter::from),
            if self.get { Some(GET) } else { None },
        )
    }
}

impl TryFrom<&[DataType]> for Options {
    type Error = anyhow::Error;

    fn try_from(args: &[DataType]) -> Result<Self> {
        let mut ops = Options::default();

        let mut args = args.iter().cloned();

        while let Some(arg) = args.next() {
            match arg.to_uppercase().as_slice() {
                b"NX" => ops.with_nx()?,
                b"XX" => ops.with_xx()?,
                b"GET" => ops.with_get(),
                o @ b"EX" | o @ b"PX" | o @ b"EXAT" | o @ b"PXAT" => {
                    let Some(arg) = args.next() else {
                        bail!(
                            "syntax error: {} _ is missing an argument",
                            std::str::from_utf8(o).unwrap_or_default()
                        )
                    };

                    // NOTE: this is a rather defensive parsing
                    match arg.parse_int()? {
                        DataType::Integer(int) if int.is_positive() => match o {
                            b"EX" => ops.with_ex(int as u64)?,
                            b"PX" => ops.with_px(int as u64)?,
                            b"EXAT" => bail!("option EXAT isn't currently supported"),
                            b"PXAT" => bail!("option PXAT isn't currently supported"),
                            _ => unreachable!("checked above"),
                        },

                        arg => bail!(
                            "syntax error: {} {arg:?} is invalid",
                            std::str::from_utf8(o).unwrap_or_default()
                        ),
                    }
                }
                b"KEEPTTL" => ops.with_keep_ttl()?,
                _ => bail!("syntax error: {arg:?} is invalid"),
            }
        }

        Ok(ops)
    }
}

struct OptionsBytesIter(
    Option<ExpiryBytesIter>,
    Option<CondBytesIter>,
    Option<Bytes>,
);

impl Iterator for OptionsBytesIter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        let Self(exp, cond, get) = self;

        if let Some(it) = exp {
            if let item @ Some(_) = it.next() {
                return item;
            } else {
                exp.take();
            }
        }

        if let Some(it) = cond {
            if let item @ Some(_) = it.next() {
                return item;
            } else {
                cond.take();
            }
        }

        get.take()
    }
}

#[derive(Debug)]
pub struct StoreOptions {
    pub(crate) exp: Option<Expiry>,
    pub(crate) cond: Option<Condition>,
}

impl From<Options> for (StoreOptions, bool) {
    #[inline]
    fn from(Options { exp, cond, get }: Options) -> Self {
        (StoreOptions { exp, cond }, get)
    }
}
