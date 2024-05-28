use std::collections::HashMap;
use std::fmt::Write;
use std::io::{self, Error, ErrorKind};
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::collections::HashSet;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::data::ParseInt;
use crate::store::Database;
use crate::stream::Stream;

pub(crate) const MAGIC: &[u8] = b"REDIS";
pub(crate) const DEFAULT_DB: usize = Database::DEFAULT;

const U32_MAX: usize = u32::MAX as usize;

pub(crate) mod aux {
    pub const REDIS_VER: &[u8] = b"redis-ver";
    pub const REDIS_BITS: &[u8] = b"redis-bits";
    pub const CTIME: &[u8] = b"ctime";
    pub const USED_MEM: &[u8] = b"used-mem";
    pub const REPL_STREAM_DB: &[u8] = b"repl-stream-db";
    pub const REPL_ID: &[u8] = b"repl-id";
    pub const REPL_OFFSET: &[u8] = b"repl-offset";
    pub const AOF_BASE: &[u8] = b"aof-base";
}

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

    // NOTE: exclusive patterns (lb..ub) are experimental, hence the -1 in upper bounds
    pub(super) const LB_INT8: i64 = -(1 << 7);
    pub(super) const UB_INT8: i64 = (1 << 7) - 1;
    pub(super) const LB_INT16: i64 = -(1 << 15);
    pub(super) const UB_INT16: i64 = (1 << 15) - 1;
    pub(super) const LB_INT32: i64 = -(1 << 31);
    pub(super) const UB_INT32: i64 = (1 << 31) - 1;
}

mod valtype {
    use bytes::Bytes;
    pub(super) const STRING: Bytes = Bytes::from_static(b"string");
    pub(super) const LIST: Bytes = Bytes::from_static(b"list");
    pub(super) const SET: Bytes = Bytes::from_static(b"set");
    pub(super) const ZSET: Bytes = Bytes::from_static(b"zset");
    pub(super) const HASH: Bytes = Bytes::from_static(b"hash");
    pub(super) const STREAM: Bytes = Bytes::from_static(b"stream");
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

impl From<i64> for String {
    #[inline]
    fn from(value: i64) -> Self {
        use encoding::*;
        match value {
            LB_INT8..=UB_INT8 => Self::Int8(value as i8),
            LB_INT16..=UB_INT16 => Self::Int16(value as i16),
            LB_INT32..=UB_INT32 => Self::Int32(value as i32),
            value => value.to_string().into(),
        }
    }
}

impl From<isize> for String {
    #[inline]
    fn from(value: isize) -> Self {
        Self::from(value as i64)
    }
}

impl From<std::string::String> for String {
    #[inline]
    fn from(s: std::string::String) -> Self {
        Self::Str(s.into())
    }
}

impl From<std::borrow::Cow<'_, str>> for String {
    #[inline]
    fn from(s: std::borrow::Cow<'_, str>) -> Self {
        Self::from(s.to_string())
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

impl std::fmt::Display for String {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Str(s) => write!(f, "{}", std::string::String::from_utf8_lossy(s)),
            Self::Int8(i) => write!(f, "{i}"),
            Self::Int16(i) => write!(f, "{i}"),
            Self::Int32(i) => write!(f, "{i}"),
        }
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

impl String {
    pub async fn write_into<W>(self, writer: &mut W, buf: &mut BytesMut) -> io::Result<usize>
    where
        W: AsyncWriteExt + Unpin,
    {
        match self {
            String::Str(bytes) if bytes.len() <= 11 => {
                if let Ok(value) = bytes.as_ref().parse::<i32>() {
                    let mut buf = [0; 5]; // len is max bytes written by encode_integer
                    let n = encode_integer(value as i64, &mut buf);
                    writer.write_all(&buf[..n]).await?;
                    return Ok(n);
                }

                // TODO: LZF compression (if enabled)

                Self::write_str(bytes, writer).await
            }
            String::Str(bytes) => Self::write_str(bytes, writer).await,
            String::Int8(i) => Self::write_int(i as i64, writer, buf).await,
            String::Int16(i) => Self::write_int(i as i64, writer, buf).await,
            String::Int32(i) => Self::write_int(i as i64, writer, buf).await,
        }
    }

    async fn write_int<W>(value: i64, writer: &mut W, buf: &mut BytesMut) -> io::Result<usize>
    where
        W: AsyncWriteExt + Unpin,
    {
        // TODO: inline the buffer
        // buffer with enough capacity for encoding of an i64
        //let mut buf = [0; 32];

        buf.clear();
        buf.resize(32, 0);

        let n = encode_integer(value, buf);

        if n > 0 {
            writer.write_all(&buf[..n]).await?;
            return Ok(n);
        }

        // encode as string
        buf.write_fmt(format_args!("{value}"))
            .map_err(Error::other)?;

        Self::write_str(buf, writer).await
    }

    async fn write_str<B, W>(bytes: B, writer: &mut W) -> io::Result<usize>
    where
        B: AsRef<[u8]>,
        W: AsyncWriteExt + Unpin,
    {
        let bytes = bytes.as_ref();

        let enclen = Length::Len(bytes.len());
        let enclen_len = enclen.write_into(writer).await?;

        writer.write_all(bytes).await?;

        Ok(enclen_len + bytes.len())
    }
}

// TODO: implement
pub async fn read_stream<R>(_reader: &mut R, _buf: &mut BytesMut) -> io::Result<(Stream, usize)>
where
    R: AsyncReadExt + Unpin,
{
    Err(Error::new(
        ErrorKind::Unsupported,
        "RDB: reading streams is not supported",
    ))
}

#[derive(Clone, Copy, Debug)]
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
    StreamListPacks = 15,
    StreamListPacks2 = 19,
    StreamListPacks3 = 21,
}

impl ValueType {
    #[inline]
    pub fn is_stream(val: impl std::ops::Deref<Target = Value>) -> bool {
        matches!(
            Self::from(&*val),
            Self::StreamListPacks | Self::StreamListPacks2 | Self::StreamListPacks3
        )
    }
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
            15 => Ok(Self::StreamListPacks),
            19 => Ok(Self::StreamListPacks2),
            21 => Ok(Self::StreamListPacks3),
            ty => Err(invalid_data(format!("RDB: invalid value type {ty}"))),
        }
    }
}

impl From<ValueType> for Bytes {
    #[inline]
    fn from(ty: ValueType) -> Self {
        use valtype::*;
        match ty {
            ValueType::String => STRING,
            ValueType::List => LIST,
            ValueType::Set => SET,
            ValueType::ZSet => ZSET,
            ValueType::Hash => HASH,
            ValueType::HashZipMap => HASH,
            ValueType::ListZipList => LIST,
            ValueType::SetIntSet => SET,
            ValueType::ZSetZipList => ZSET,
            ValueType::HashZipList => HASH,
            ValueType::ListQuickList => LIST,
            ValueType::StreamListPacks
            | ValueType::StreamListPacks2
            | ValueType::StreamListPacks3 => STREAM,
        }
    }
}

impl From<ValueType> for String {
    #[inline]
    fn from(ty: ValueType) -> Self {
        Self::Str(ty.into())
    }
}

// TODO: other "valtype"s
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub enum Value {
    String(String),
    Stream(Stream),
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
            ValueType::StreamListPacks
            | ValueType::StreamListPacks2
            | ValueType::StreamListPacks3 => read_stream(reader, buf)
                .await
                .map(|(s, n)| (Self::Stream(s), n)),
            other => Err(Error::new(
                ErrorKind::Unsupported,
                format!("RDB: reading {other:?} is not supported"),
            )),
        }
    }

    pub async fn write_into<W>(self, writer: &mut W, buf: &mut BytesMut) -> io::Result<usize>
    where
        W: AsyncWriteExt + Unpin,
    {
        match self {
            Self::String(s) => s.write_into(writer, buf).await,
            Self::Stream(_) => unimplemented!("RDB: writing streams is not supported"),
        }
    }
}

impl From<Value> for String {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => s,
            // TODO: implement
            v => unimplemented!("{v:?} -> rdb::String"),
        }
    }
}

impl From<&Value> for ValueType {
    #[inline]
    fn from(value: &Value) -> Self {
        match value {
            Value::String(_) => ValueType::String,
            Value::Stream(_) => ValueType::StreamListPacks3,
            // TODO: other value types
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
                Self::Len((((x as u16 & 0x3F) << 8) | y as u16) as usize)
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

    pub async fn write_into<W>(self, writer: &mut W) -> io::Result<usize>
    where
        W: AsyncWriteExt + Unpin,
    {
        use encoding::*;

        let Self::Len(len) = self else {
            unimplemented!("use encode_integer instead");
        };

        Ok(match len {
            len if len < (1 << 6) => {
                writer.write_u8(len as u8 | (BIT_LEN_6 << 6)).await?;
                1
            }

            len if len < (1 << 14) => {
                // write u16 as BE bytes
                let buf = [len as u8 | (BIT_LEN_14 << 6), len as u8];
                writer.write_all(&buf).await?;
                2
            }

            ..=U32_MAX => {
                writer.write_u8(BIT_LEN_32).await?;
                writer.write_u32(len as u32).await?;
                1 + 4
            }

            _ => {
                writer.write_u8(BIT_LEN_64).await?;
                writer.write_u64(len as u64).await?;
                1 + 8
            }
        })
    }

    #[inline]
    pub fn into_length(self) -> usize {
        match self {
            Self::Len(len) => len,
            Self::Enc(val) => val as u8 as usize,
        }
    }
}

fn encode_integer(value: i64, enc: &mut [u8]) -> usize {
    use encoding::*;
    match value {
        LB_INT8..=UB_INT8 => {
            enc[0] = (ENC_VAL << 6) | Encoding::Int8 as u8;
            enc[1] = value as u8; // implicitly & 0xFF
            2
        }
        LB_INT16..=UB_INT16 => {
            enc[0] = (ENC_VAL << 6) | Encoding::Int16 as u8;
            enc[1] = (value as u16 & 0xFF) as u8;
            enc[2] = ((value as u16 >> 8) & 0xFF) as u8;
            3
        }
        LB_INT32..=UB_INT32 => {
            enc[0] = (ENC_VAL << 6) | Encoding::Int32 as u8;
            enc[1] = (value as u32 & 0xFF) as u8;
            enc[2] = ((value as u32 >> 8) & 0xFF) as u8;
            enc[3] = ((value as u32 >> 16) & 0xFF) as u8;
            enc[4] = ((value as u32 >> 24) & 0xFF) as u8;
            5
        }
        _ => 0,
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
        ExpiryUnit::Millis => read_time_ms(reader).await,
    }
}

async fn read_time_ms<R>(reader: &mut R) -> io::Result<(SystemTime, usize)>
where
    R: AsyncReadExt + Unpin,
{
    let millis = reader.read_u64_le().await?;
    let time = UNIX_EPOCH + Duration::from_millis(millis);
    Ok((time, 8))
}

pub(crate) async fn write_time_ms<W>(t: SystemTime, writer: &mut W) -> io::Result<usize>
where
    W: AsyncWriteExt + Unpin,
{
    let ms = t
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis();

    writer.write_u64_le(ms as u64).await?;
    Ok(8)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Aux {
    pub(crate) redis_ver: Option<String>,
    pub(crate) redis_bits: Option<String>,
    pub(crate) ctime: Option<String>,
    pub(crate) used_mem: Option<String>,
    pub(crate) repl_stream_db: Option<String>,
    pub(crate) repl_id: Option<String>,
    pub(crate) repl_offset: Option<String>,
    pub(crate) aof_base: Option<String>,
}

impl Aux {
    pub fn set(&mut self, key: String, val: String) -> io::Result<()> {
        use self::{aux::*, String::*};
        match (key, val) {
            (k @ (Int8(_) | Int16(_) | Int32(_)), _) => {
                return Err(invalid_data(format!("RDB: expected string key, got {k:?}")))
            }
            (Str(k), v) if matches!(k.as_ref(), REDIS_VER) => self.redis_ver = Some(v),
            (Str(k), v) if matches!(k.as_ref(), REDIS_BITS) => self.redis_bits = Some(v),
            (Str(k), v) if matches!(k.as_ref(), CTIME) => self.ctime = Some(v),
            (Str(k), v) if matches!(k.as_ref(), USED_MEM) => self.used_mem = Some(v),
            (Str(k), v) if matches!(k.as_ref(), REPL_STREAM_DB) => self.repl_stream_db = Some(v),
            (Str(k), v) if matches!(k.as_ref(), REPL_ID) => self.repl_id = Some(v),
            (Str(k), v) if matches!(k.as_ref(), REPL_OFFSET) => self.repl_offset = Some(v),
            (Str(k), v) if matches!(k.as_ref(), AOF_BASE) => self.aof_base = Some(v),
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
#[derive(Clone, Debug)]
pub struct RDB {
    pub(crate) version: NonZeroU32,
    pub(crate) aux: Aux,
    pub(crate) dbs: HashMap<usize, Database>,
    /// Note: checksum is supported for `version >= 5`
    pub(crate) checksum: Option<Bytes>,
}

#[cfg(test)]
impl RDB {
    pub(crate) fn remove(mut self, mut expired: HashMap<usize, HashSet<String>>) -> Self {
        for db in self.dbs.values_mut() {
            for expired in expired.remove(&db.ix).unwrap_or_default() {
                db.remove(&expired);
            }
        }
        self
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct RDBData(pub(crate) Bytes);

impl std::ops::Deref for RDBData {
    type Target = Bytes;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[inline]
fn invalid_data<E>(e: E) -> Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    Error::new(ErrorKind::InvalidData, e)
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: this calls for a proptest
    #[tokio::test]
    async fn write_read_length() {
        let cases = [
            (Length::Len(63), 1),
            //(Length::Len(16383), 2),
            (Length::Len(257), 2),
            //(Length::Len(u32::MAX as usize + 1), 9),
        ];

        for (input, bytes_expected) in cases {
            let mut writer = Vec::with_capacity(128);

            let bytes_written = input.write_into(&mut writer).await.expect("write length");
            assert_eq!(bytes_expected, bytes_written);

            let mut reader = io::Cursor::new(writer);

            let (output, bytes_read) = Length::read_from(&mut reader).await.expect("read length");
            assert_eq!(bytes_written, bytes_read);

            assert_eq!(input, output);
        }
    }
}
