use std::collections::VecDeque;

use anyhow::{bail, ensure, Result};
use tokio::time::Duration;

use crate::{DataExt as _, DataType};

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub enum Expiry {
    /// Set the specified expire time
    EX(Duration),
    /// Set the specified expire time
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

#[derive(Debug)]
pub enum Condition {
    /// Only set the key if it does not already exist
    NX,
    /// Only set the key if it already exists
    XX,
}

#[derive(Debug, Default)]
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
}

impl TryFrom<VecDeque<DataType>> for Options {
    type Error = anyhow::Error;

    fn try_from(mut args: VecDeque<DataType>) -> Result<Self> {
        let mut ops = Options::default();

        while let Some(arg) = args.pop_front() {
            match arg.to_uppercase().as_slice() {
                b"NX" => ops.with_nx()?,
                b"XX" => ops.with_xx()?,
                b"GET" => ops.with_get(),
                o @ b"EX" | o @ b"PX" | o @ b"EXAT" | o @ b"PXAT" => {
                    let Some(arg) = args.pop_front() else {
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
