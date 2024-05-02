use std::collections::HashMap;
use std::io::{self, Error, ErrorKind};
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;

use crate::store::Database;

pub(crate) mod opcode {
    pub(crate) const EOF: u8 = 0xFF;
    pub(crate) const SELECTDB: u8 = 0xFE;
    pub(crate) const EXPIRETIME: u8 = 0xFD;
    pub(crate) const EXPIRETIMEMS: u8 = 0xFC;
    pub(crate) const RESIZEDB: u8 = 0xFB;
    pub(crate) const AUX: u8 = 0xFA;
}

mod encoding {
    pub(super) const BIT_LEN_6: u8 = 0;
    pub(super) const BIT_LEN_14: u8 = 1;
    pub(super) const BIT_LEN_32: u8 = 0x80;
    pub(super) const BIT_LEN_64: u8 = 0x81;
    pub(super) const ENC_VAL: u8 = 3;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum String {
    Str(Bytes),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    // NOTE: LZF could be another variant or just a Str
}

impl String {
    #[inline]
    pub fn bytes(&self) -> Option<Bytes> {
        match self {
            Self::Str(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl From<i8> for String {
    #[inline]
    fn from(i: i8) -> Self {
        Self::Int8(i)
    }
}

impl From<i16> for String {
    #[inline]
    fn from(i: i16) -> Self {
        Self::Int16(i)
    }
}

impl From<i32> for String {
    #[inline]
    fn from(i: i32) -> Self {
        Self::Int32(i)
    }
}

impl From<std::string::String> for String {
    #[inline]
    fn from(s: std::string::String) -> Self {
        Self::Str(s.into())
    }
}

impl From<&'static str> for String {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self::from(s.as_bytes())
    }
}

impl From<&'static [u8]> for String {
    #[inline]
    fn from(s: &'static [u8]) -> Self {
        Self::from(Bytes::from_static(s))
    }
}

impl From<BytesMut> for String {
    #[inline]
    fn from(s: BytesMut) -> Self {
        Self::from(s.freeze())
    }
}

impl From<Bytes> for String {
    #[inline]
    fn from(s: Bytes) -> Self {
        Self::Str(s)
    }
}

pub async fn read_string<R>(reader: &mut R, buf: &mut BytesMut) -> io::Result<(String, usize)>
where
    R: AsyncReadExt + Unpin,
{
    use Encoding::*;
    use Length::*;

    let (length, mut bytes_read) = Length::read_from(reader).await?;

    let string = match length {
        Len(len) => {
            buf.reserve(len);
            buf.resize(len, 0);
            bytes_read += reader.read_exact(buf).await?;
            String::Str(buf.split().freeze())
        }
        Enc(Int8) => {
            bytes_read += 1;
            reader.read_i8().await.map(String::Int8)?
        }
        Enc(Int16) => {
            bytes_read += 2;
            reader.read_i16_le().await.map(String::Int16)?
        }
        Enc(Int32) => {
            bytes_read += 4;
            reader.read_i32_le().await.map(String::Int32)?
        }
        Enc(LZF) => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "RDB: reading LZF compressed strings is not supported",
            ));
        }
    };

    Ok((string, bytes_read))
}

#[derive(Debug)]
#[repr(u8)]
pub enum ValueType {
    String = 0,
    List = 1,
    Set = 2,
    ZSet = 3,
    Hash = 4,
    HashZipMap = 9,
    ListZipList = 10,
    SetIntSet = 11,
    ZSetZipList = 12,
    HashZipList = 13,
    ListQuickList = 14,
    Stream = 15,
}

impl TryFrom<u8> for ValueType {
    type Error = Error;

    #[inline]
    fn try_from(ty: u8) -> Result<Self, Self::Error> {
        match ty {
            0 => Ok(Self::String),
            1 => Ok(Self::List),
            2 => Ok(Self::Set),
            3 => Ok(Self::ZSet),
            4 => Ok(Self::Hash),
            9 => Ok(Self::HashZipMap),
            10 => Ok(Self::ListZipList),
            11 => Ok(Self::SetIntSet),
            12 => Ok(Self::ZSetZipList),
            13 => Ok(Self::HashZipList),
            14 => Ok(Self::ListQuickList),
            15 => Ok(Self::Stream),
            ty => Err(invalid_data(format!("RDB: invalid value type {ty}"))),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    // TODO: other "valtype"s
}

impl Value {
    pub async fn read_from<T, R>(
        ty: T,
        reader: &mut R,
        buf: &mut BytesMut,
    ) -> io::Result<(Self, usize)>
    where
        T: TryInto<ValueType, Error = Error>,
        R: AsyncReadExt + Unpin,
    {
        match ty.try_into()? {
            ValueType::String => read_string(reader, buf)
                .await
                .map(|(s, n)| (Self::String(s), n)),
            other => Err(Error::new(
                ErrorKind::Unsupported,
                format!("RDB: reading {other:?} is not supported"),
            )),
        }
    }
}

impl From<Value> for String {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => s,
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
#[repr(u8)]
pub enum Encoding {
    Int8 = 0,
    Int16 = 1,
    Int32 = 2,
    #[allow(dead_code)]
    LZF = 3,
}

impl TryFrom<u8> for Encoding {
    type Error = Error;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Int8),
            1 => Ok(Self::Int16),
            2 => Ok(Self::Int32),
            3 => Ok(Self::LZF),
            v => Err(Error::new(
                ErrorKind::InvalidData,
                format!("unknown value type: {v}"),
            )),
        }
    }
}

#[derive(Debug)]
pub enum Length {
    Len(usize),
    Enc(Encoding),
}

impl Length {
    pub async fn read_from<R>(reader: &mut R) -> io::Result<(Self, usize)>
    where
        R: AsyncReadExt + Unpin,
    {
        use encoding::*;

        let mut bytes_read = 0;

        let x = reader.read_u8().await?;
        bytes_read += 1;

        // match on the encoding type
        let len = match (x & 0xC0) >> (u8::BITS - 2) {
            ENC_VAL => Self::Enc((x & 0x3F).try_into()?),
            BIT_LEN_6 => Self::Len((x & 0x3F) as usize),
            BIT_LEN_14 => {
                let y = reader.read_u8().await?;
                bytes_read += 1;
                Self::Len((((x & 0x3F) << u8::BITS) | y) as usize)
            }
            BIT_LEN_32 => {
                let len = reader.read_u32().await?;
                bytes_read += 4;
                Self::Len(len as usize)
            }
            BIT_LEN_64 => {
                let len = reader.read_u64().await?;
                bytes_read += 8;
                Self::Len(len as usize)
            }
            b => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("encoding byte: {b:x}"),
                ));
            }
        };

        Ok((len, bytes_read))
    }

    #[inline]
    pub fn into_length(self) -> usize {
        match self {
            Self::Len(len) => len,
            Self::Enc(val) => val as u8 as usize,
        }
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum ExpiryUnit {
    Seconds,
    Millis,
}

impl TryFrom<u8> for ExpiryUnit {
    type Error = Error;

    #[inline]
    fn try_from(op: u8) -> Result<Self, Self::Error> {
        match op {
            opcode::EXPIRETIME => Ok(Self::Seconds),
            opcode::EXPIRETIMEMS => Ok(Self::Millis),
            opcode => Err(invalid_data(format!("invalid expiration opcode: {opcode}"))),
        }
    }
}

pub async fn read_expiry<U, R>(unit: U, reader: &mut R) -> io::Result<(SystemTime, usize)>
where
    U: TryInto<ExpiryUnit, Error = Error>,
    R: AsyncReadExt + Unpin,
{
    match unit.try_into()? {
        ExpiryUnit::Seconds => {
            let secs = reader.read_u32_le().await?;
            let time = UNIX_EPOCH + Duration::from_secs(secs as u64);
            Ok((time, 4))
        }
        ExpiryUnit::Millis => {
            let millis = reader.read_u64_le().await?;
            let time = UNIX_EPOCH + Duration::from_millis(millis);
            Ok((time, 8))
        }
    }
}

#[derive(Debug, Default)]
pub struct Aux {
    pub(crate) redis_ver: Option<String>,
    pub(crate) redis_bits: Option<String>,
    pub(crate) ctime: Option<String>,
    pub(crate) used_mem: Option<String>,
    pub(crate) aof_base: Option<String>,
}

impl Aux {
    pub fn set(&mut self, key: String, val: String) -> io::Result<()> {
        use self::String::*;
        match (key, val) {
            (k @ (Int8(_) | Int16(_) | Int32(_)), _) => {
                return Err(invalid_data(format!("RDB: expected string key, got {k:?}")))
            }
            (Str(k), v) if matches!(k.as_ref(), b"redis-ver") => self.redis_ver = Some(v),
            (Str(k), v) if matches!(k.as_ref(), b"redis-bits") => self.redis_bits = Some(v),
            (Str(k), v) if matches!(k.as_ref(), b"ctime") => self.ctime = Some(v),
            (Str(k), v) if matches!(k.as_ref(), b"used-mem") => self.used_mem = Some(v),
            (Str(k), v) if matches!(k.as_ref(), b"aof-base") => self.aof_base = Some(v),
            (k, v) => {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    format!("RDB: unknown key-value pair {k:?} {v:?}"),
                ))
            }
        }
        Ok(())
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub struct RDB {
    pub(crate) version: NonZeroU32,
    pub(crate) aux: Aux,
    pub(crate) dbs: HashMap<usize, Database>,
    /// Note: checksum is supported for `version >= 5`
    pub(crate) checksum: Option<Bytes>,
}

#[inline]
fn invalid_data<E>(e: E) -> Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    Error::new(ErrorKind::InvalidData, e)
}
