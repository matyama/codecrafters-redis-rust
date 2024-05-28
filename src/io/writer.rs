use std::fmt::Write;
use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::Pin;

use anyhow::{ensure, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::data::DataType;
use crate::io::CRLF;
use crate::rdb::{self, aux, DEFAULT_DB, MAGIC, RDB};

const NULL: &[u8] = b"_\r\n";

const REDIS_VER: Bytes = Bytes::from_static(aux::REDIS_VER);
const REDIS_BITS: Bytes = Bytes::from_static(aux::REDIS_BITS);
const CTIME: Bytes = Bytes::from_static(aux::CTIME);
const USED_MEM: Bytes = Bytes::from_static(aux::USED_MEM);
const AOF_BASE: Bytes = Bytes::from_static(aux::AOF_BASE);

macro_rules! rdb_write {
    ([$writer:expr, $written:ident]; SELECTDB $ix:expr) => {
        rdb_write! { [$writer, $written]; @SELECTDB }

        $written += rdb::Length::Len($ix)
            .write_into(&mut $writer)
            .await
            .with_context(|| format!("SELECTDB {}", $ix))?;
    };

    ([$writer:expr, $written:ident]; RESIZEDB $($size:ident),+) => {
        rdb_write! { [$writer, $written]; @RESIZEDB }

        $(
            $written += rdb::Length::Len($size)
                .write_into(&mut $writer)
                .await
                .with_context(|| format!("RESIZEDB {}", stringify!($size)))?;
        )+
    };

    ([$writer:expr, $written:ident]; EXPIRETIMEMS $t:expr) => {
        rdb_write! { [$writer, $written]; @EXPIRETIMEMS }

        $written += rdb::write_time_ms($t, &mut $writer)
            .await
            .with_context(|| format!("EXPIRETIMEMS entry expiration time {:?}", $t))?;
    };

    ([$writer:expr, $written:ident]; @$op:ident) => {
        rdb_write! { [$writer, $written, "{:x?} opcode"]; rdb::opcode::$op }
    };

    ([$writer:expr, $written:ident, $cx:expr]; $byte:expr) => {
        $writer
            .write_u8($byte)
            .await
            .with_context(|| format!($cx, $byte))?;
        $written += 1;
    };

    ([$writer:expr, $buf:expr, $written:ident]; $($key:ident: $val:expr),+) => {
        $(
            if let Some(val) = $val {
                rdb_write! {
                    [$writer, $buf, $written, "AUX entry {}"];
                    rdb::String::Str($key) => rdb::String { val } as AUX
                }
            }
        )+
    };

    (
        [$writer:expr, $buf:expr, $written:ident, $cx:expr];
        $key:expr => $vt:ty { $val:expr } as $ty:expr
    ) => {
        rdb_write! { [$writer, $written, $cx]; $ty }

        $written += $key.write_into(&mut $writer, &mut $buf)
            .await
            .with_context(|| format!($cx, "key"))?;

        $written += <$vt>::write_into($val, &mut $writer, &mut $buf)
            .await
            .with_context(|| format!($cx, "value"))?;
    };
}

pub struct DataWriter<W> {
    writer: BufWriter<W>,
    buf: String,
    aux: String,
}

impl<W> DataWriter<W>
where
    W: AsyncWriteExt + Send + Unpin,
{
    #[inline]
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
            buf: String::with_capacity(16),
            aux: String::with_capacity(16),
        }
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn into_inner(self) -> W {
        self.writer.into_inner()
    }

    /// Returns the number of bytes written
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub fn write<'a>(
        &'a mut self,
        resp: &'a DataType,
    ) -> Pin<Box<dyn Future<Output = Result<usize>> + Send + '_>> {
        Box::pin(async move {
            let bytes_written = match resp {
                // nulls: `_\r\n`
                DataType::Null => {
                    self.writer.write_all(NULL).await?;
                    NULL.len()
                }

                // null bulk string|error: `<$|!>-1\r\n`
                DataType::NullBulkString => self.write_bulk('$', None).await?,
                DataType::NullBulkError => self.write_bulk('!', None).await?,

                // boolean: `#<t|f>\r\n`
                DataType::Boolean(boolean) => {
                    self.writer.write_u8(b'#').await?;
                    let boolean = if *boolean { b't' } else { b'f' };
                    self.writer.write_u8(boolean).await?;
                    self.writer.write_all(CRLF).await?;
                    2 + CRLF.len()
                }

                // integer: `:[<+|->]<value>\r\n`
                DataType::Integer(int) => {
                    self.buf.clear();
                    write!(self.buf, ":{int}\r\n")?;
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.buf.len()
                }

                // double: `,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n`
                DataType::Double(double) => {
                    self.buf.clear();
                    if double.is_nan() {
                        write!(self.buf, ",nan\r\n")?;
                    } else {
                        write!(self.buf, ",{double}\r\n")?;
                    }
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.buf.len()
                }

                // simple strings: `+<data>\r\n`
                DataType::SimpleString(data) => self.write_simple(b'+', data).await?,

                // simple errors: `-<data>\r\n`
                DataType::SimpleError(data) => self.write_simple(b'-', data).await?,

                // bulk strings: `$<length>\r\n<data>\r\n`
                DataType::BulkString(data) => self.write_bulk('$', Some(data)).await?,

                // bulk errors: `!<length>\r\n<data>\r\n`
                DataType::BulkError(data) => self.write_bulk('!', Some(data)).await?,

                // array: `*<number-of-elements>\r\n<element-1>...<element-n>`
                DataType::Array(items) => {
                    self.writer.write_u8(b'*').await?;

                    self.buf.clear();
                    write!(self.buf, "{}", items.len())?;
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(CRLF).await?;

                    let mut bytes_written = 1 + self.buf.len() + CRLF.len();

                    if items.is_empty() {
                        return Ok(bytes_written);
                    }

                    for (i, item) in items.iter().enumerate() {
                        bytes_written += self
                            .write(item)
                            .await
                            .with_context(|| format!("failed to write array item {i}"))?;
                    }

                    bytes_written
                }

                // map: `%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>`
                DataType::Map(items) => {
                    self.writer.write_u8(b'%').await?;

                    self.buf.clear();
                    write!(self.buf, "{}", items.len())?;
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(CRLF).await?;

                    let mut bytes_written = 1 + self.buf.len() + CRLF.len();

                    for (key, value) in items.iter() {
                        bytes_written += self
                            .write(key)
                            .await
                            .with_context(|| format!("failed to write map key {key:?}"))?;
                        bytes_written += self
                            .write(value)
                            .await
                            .with_context(|| format!("failed to write map value {value:?}"))?;
                    }

                    bytes_written
                }
            };

            Ok(bytes_written)
        })
    }

    async fn write_simple(&mut self, tag: u8, data: &rdb::String) -> Result<usize> {
        self.buf.clear();
        let data = write_str(&mut self.buf, data)?;
        self.writer.write_u8(tag).await?;
        self.writer.write_all(data).await?;
        self.writer.write_all(CRLF).await?;
        Ok(1 + data.len() + CRLF.len())
    }

    async fn write_bulk(&mut self, tag: char, data: Option<&rdb::String>) -> Result<usize> {
        self.buf.clear();
        self.aux.clear();

        let Some(data) = data else {
            write!(self.buf, "{tag}-1\r\n")?;
            self.writer.write_all(self.buf.as_bytes()).await?;
            return Ok(self.buf.len());
        };

        let data = write_str(&mut self.aux, data)?;
        let len = data.len();

        write!(self.buf, "{tag}{len}\r\n")?;
        self.writer.write_all(self.buf.as_bytes()).await?;
        self.writer.write_all(data).await?;
        self.writer.write_all(CRLF).await?;

        Ok(self.buf.len() + len + CRLF.len())
    }

    /// Write a RDB file
    ///
    /// # Format
    /// `$<length_of_file>\r\n<contents_of_file>`
    ///
    /// Note that the format similar to how [Bulk String](DataType::BulkString)s are encoded, but
    /// without the trailing [CLRF].
    pub async fn write_rdb(&mut self, rdb: RDB) -> Result<()> {
        // TODO: read this from a BGSAVEd file (get length from the file writer or FS)
        //  - implementation detail: write while reading (i.e., pipe the bytes)
        let data = {
            let mut mem_writer = DataWriter::new(Vec::with_capacity(1024));
            let _ = mem_writer.write_rdb_file(rdb).await?;
            mem_writer.flush().await?;
            mem_writer.writer.into_inner()
        };

        self.writer.write_u8(b'$').await?;

        self.buf.clear();
        write!(self.buf, "{}", data.len())?;
        self.writer.write_all(self.buf.as_bytes()).await?;
        self.writer.write_all(CRLF).await?;

        if data.is_empty() {
            return self.flush().await;
        }

        self.writer
            .write_all(&data)
            .await
            .context("RDB file contents")
    }

    /// https://rdb.fnordig.de/file_format.html
    pub async fn write_rdb_file(&mut self, rdb: RDB) -> Result<usize> {
        use rdb::opcode::*;

        ensure!(
            rdb.dbs.contains_key(&DEFAULT_DB),
            "RDB is missing the default DB: {DEFAULT_DB}"
        );

        // TODO: ideally reuse `self.buf` (but that one's currently a String)
        let mut buf = BytesMut::with_capacity(256);
        let mut bytes_written = 0;

        // header (magic + version)
        {
            self.writer.write_all(MAGIC).await.context("magic")?;
            bytes_written += MAGIC.len();

            // NOTE: must be exactly 4B
            buf.write_fmt(format_args!("{:04}", rdb.version))?;
            self.writer.write_all(&buf).await.context("version")?;
            bytes_written += buf.len();

            buf.clear();
        }

        // auxiliary fields
        rdb_write! {
            [self.writer, buf, bytes_written];
            REDIS_VER: rdb.aux.redis_ver,
            REDIS_BITS: rdb.aux.redis_bits,
            CTIME: rdb.aux.ctime,
            USED_MEM: rdb.aux.used_mem,
            AOF_BASE: rdb.aux.aof_base
        }

        // databases
        let mut dbs = rdb.dbs.into_values().collect::<Vec<_>>();
        dbs.sort_unstable_by_key(|db| db.ix);

        for db in dbs {
            let (ix, db, db_size, expires_size) = db.into_inner();

            rdb_write! {
                [self.writer, bytes_written];
                SELECTDB ix
            }

            rdb_write! {
                [self.writer, bytes_written];
                RESIZEDB db_size, expires_size
            }

            // [EXPIRETIMEMS <exipire-time>] <ty> <key> <val>
            for (key, val) in db.into_iter() {
                let (val, expire) = val.into_inner();

                if let Some(expire) = expire {
                    // NOTE: Redis seems to always save the expire time in milliseconds
                    rdb_write! {
                        [self.writer, bytes_written];
                        EXPIRETIMEMS expire
                    }
                }

                let ty = rdb::ValueType::from(&val) as u8;

                rdb_write! {
                    [self.writer, buf, bytes_written, "entry {}"];
                    key => rdb::Value { val } as ty
                }
            }
        }

        self.writer.write_u8(EOF).await.context("EOF")?;
        bytes_written += 1;

        // checksum
        if let Some(checksum) = rdb.checksum {
            self.writer.write_all(&checksum).await.context("checksum")?;
            bytes_written += checksum.len();
        }

        Ok(bytes_written)
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await.context("flush data")
    }
}

#[allow(dead_code)]
#[repr(transparent)]
pub struct RDBFileWriter(DataWriter<File>);

#[allow(dead_code)]
impl RDBFileWriter {
    pub async fn new(file: impl AsRef<Path>) -> Result<Option<Self>> {
        match File::open(file).await {
            Ok(file) => Ok(Some(Self(DataWriter::new(file)))),
            // TODO: this should probably create the file (or rather write to a temp and swap)
            Err(e) if matches!(e.kind(), ErrorKind::NotFound) => Ok(None),
            Err(e) => Err(e).context("failed to read RDB"),
        }
    }

    pub async fn write(&mut self, rdb: RDB) -> Result<usize> {
        self.0.write_rdb_file(rdb).await
    }
}

pub trait Serializer {
    fn serialize(&mut self, resp: &DataType) -> Result<()>;

    fn serialized_size(resp: &DataType) -> Result<usize>;
}

#[derive(Debug, Default)]
pub struct DataSerializer {
    buf: BytesMut,
    aux: String,
}

impl DataSerializer {
    fn serialize_simple(&mut self, tag: char, data: &rdb::String) -> Result<()> {
        self.aux.clear();
        let data = write_str(&mut self.aux, data)?;
        self.buf.reserve(1 + data.len() + CRLF.len());
        self.buf.write_char(tag)?;
        self.buf.put_slice(data);
        write!(self.buf, "\r\n")?;
        Ok(())
    }

    fn serialize_bulk(&mut self, tag: char, data: &rdb::String) -> Result<()> {
        self.aux.clear();
        let data = write_str(&mut self.aux, data)?;
        let len = data.len();
        self.buf.reserve(1 + CRLF.len() + len + CRLF.len());
        write!(self.buf, "{tag}{len}\r\n")?;
        self.buf.put_slice(data);
        write!(self.buf, "\r\n")?;
        Ok(())
    }
}

#[cfg(test)]
impl DataSerializer {
    pub fn clear(&mut self) {
        self.buf.clear();
        self.aux.clear();
    }
}

impl AsRef<[u8]> for DataSerializer {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Serializer for DataSerializer {
    fn serialize(&mut self, resp: &DataType) -> Result<()> {
        match resp {
            // nulls: `_\r\n`
            DataType::Null => {
                self.buf.reserve(NULL.len());
                self.buf.put_slice(NULL);
            }

            // null bulk string|eror: `<$|!>-1\r\n`
            DataType::NullBulkString => self.buf.write_str("$-1\r\n")?,
            DataType::NullBulkError => self.buf.write_str("!-1\r\n")?,

            // boolean: `#<t|f>\r\n`
            DataType::Boolean(true) => self.buf.write_str("#t\r\n")?,
            DataType::Boolean(false) => self.buf.write_str("#f\r\n")?,

            // integer: `:[<+|->]<value>\r\n`
            DataType::Integer(int) => write!(self.buf, ":{int}\r\n")?,

            // double: `,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n`
            DataType::Double(double) if double.is_nan() => write!(self.buf, ",nan\r\n")?,
            DataType::Double(double) => write!(self.buf, ",{double}\r\n")?,

            // simple strings: `+<data>\r\n`
            DataType::SimpleString(data) => self.serialize_simple('+', data)?,

            // simple errors: `-<data>\r\n`
            DataType::SimpleError(data) => self.serialize_simple('-', data)?,

            // bulk strings: `$<length>\r\n<data>\r\n`
            DataType::BulkString(data) => self.serialize_bulk('$', data)?,

            // bulk error: `!<length>\r\n<data>\r\n`
            DataType::BulkError(data) => self.serialize_bulk('!', data)?,

            // array: `*<number-of-elements>\r\n<element-1>...<element-n>`
            DataType::Array(items) if items.is_empty() => write!(self.buf, "*{}\r\n", items.len())?,
            DataType::Array(items) => {
                write!(self.buf, "*{}\r\n", items.len())?;
                for (i, item) in items.iter().enumerate() {
                    self.serialize(item)
                        .with_context(|| format!("failed to serialize array item {i}"))?;
                }
            }

            // map: `%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>`
            DataType::Map(items) if items.is_empty() => write!(self.buf, "%{}\r\n", items.len())?,
            DataType::Map(items) => {
                write!(self.buf, "%{}\r\n", items.len())?;
                for (key, value) in items.iter() {
                    self.serialize(key)
                        .with_context(|| format!("failed to serialize map key {key:?}"))?;
                    self.serialize(value)
                        .with_context(|| format!("failed to serialize map value {value:?}"))?;
                }
            }
        }

        Ok(())
    }

    fn serialized_size(resp: &DataType) -> Result<usize> {
        let mut s = Self::default();
        s.serialize(resp)?;
        Ok(s.buf.len())
    }
}

fn write_str<'b, 's: 'b>(buf: &'b mut String, s: &'s rdb::String) -> Result<&'b [u8]> {
    use rdb::String::*;
    match s {
        Str(s) => Ok(s.as_ref()),
        Int8(i) => {
            write!(buf, "{i}")?;
            Ok(buf.as_bytes())
        }
        Int16(i) => {
            write!(buf, "{i}")?;
            Ok(buf.as_bytes())
        }
        Int32(i) => {
            write!(buf, "{i}")?;
            Ok(buf.as_bytes())
        }
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use super::*;

    const DATA: Bytes = Bytes::from_static(b"some message");

    macro_rules! test_serialize {
        ( $($data:expr => $expected:expr),+ ) => {
            let mut s = DataSerializer::default();
            $(
                s.clear();
                s.serialize(&$data).expect("serialized");
                assert_eq!(s.as_ref(), $expected);
            )+
        };
    }

    #[test]
    fn serialize_null() {
        test_serialize! { DataType::Null => b"_\r\n" }
    }

    #[test]
    fn serialize_null_bulk_string() {
        test_serialize! { DataType::NullBulkString => b"$-1\r\n" }
    }

    #[test]
    fn serialize_bulk() {
        test_serialize! {
            DataType::NullBulkString => b"$-1\r\n",
            DataType::NullBulkError => b"!-1\r\n",
            DataType::string(DATA) => b"$12\r\nsome message\r\n",
            DataType::error(DATA) => b"!12\r\nsome message\r\n"
        }
    }

    #[test]
    fn serialize_boolean() {
        test_serialize! {
            DataType::Boolean(true) => b"#t\r\n",
            DataType::Boolean(false) => b"#f\r\n"
        }
    }

    #[test]
    fn serialize_integer() {
        test_serialize! {
            DataType::Integer(-42) => b":-42\r\n",
            DataType::Integer(0) => b":0\r\n",
            DataType::Integer(42) => b":42\r\n"
        }
    }

    #[test]
    fn serialize_double() {
        test_serialize! {
            DataType::Double(-42.0) => b",-42\r\n",
            DataType::Double(0.0) => b",0\r\n",
            DataType::Double(42.0) => b",42\r\n",
            DataType::Double(42.04E+1) => b",420.4\r\n",
            DataType::Double(f64::INFINITY) => b",inf\r\n",
            DataType::Double(f64::NEG_INFINITY) => b",-inf\r\n",
            DataType::Double(f64::NAN) => b",nan\r\n"
        }
    }

    #[test]
    fn serialize_simple() {
        test_serialize! {
            DataType::str(DATA) => b"+some message\r\n",
            DataType::err(DATA) => b"-some message\r\n"
        }
    }

    #[test]
    fn serialize_array() {
        let ints = DataType::array([DataType::Integer(1), DataType::Integer(2)]);
        let array = DataType::array([DataType::string(DATA), DataType::string(DATA)]);
        test_serialize! {
            DataType::array([]) => b"*0\r\n",
            ints => b"*2\r\n:1\r\n:2\r\n",
            array => b"*2\r\n$12\r\nsome message\r\n$12\r\nsome message\r\n"
        }
    }

    #[test]
    fn serialize_map() {
        let keys = [DataType::Integer(1), DataType::Integer(2)];
        let vals = [DataType::string(DATA), DataType::string(DATA)];
        let items = keys.into_iter().zip(vals);
        let expected = b"%2\r\n:1\r\n$12\r\nsome message\r\n:2\r\n$12\r\nsome message\r\n";
        test_serialize! {
            DataType::map([]) => b"%0\r\n",
            DataType::map(items) => expected
        }
    }
}
