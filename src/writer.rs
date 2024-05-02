use std::future::Future;
use std::pin::Pin;
use std::{fmt::Write, u8};

use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::{rdb, DataType, RDBData, CRLF, NULL};

pub struct DataWriter<W> {
    writer: BufWriter<W>,
    buf: String,
    aux: String,
}

impl<W> DataWriter<W>
where
    W: AsyncWriteExt + Send + Unpin,
{
    // TODO: it's unfortunate that ad-hoc uses of this always allocate `buf`, make it reusable
    #[inline]
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
            buf: String::with_capacity(16),
            aux: String::with_capacity(16),
        }
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

                // null bulk string: `$-1\r\n`
                DataType::NullBulkString => {
                    self.buf.clear();
                    write!(self.buf, "$-1")?;
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(CRLF).await?;
                    self.buf.len() + CRLF.len()
                }

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

                // simple strings: `+<data>\r\n`
                DataType::SimpleString(data) => {
                    self.buf.clear();
                    let data = write_str(&mut self.buf, data)?;
                    self.writer.write_u8(b'+').await?;
                    self.writer.write_all(data).await?;
                    self.writer.write_all(CRLF).await?;
                    1 + data.len() + CRLF.len()
                }

                // simple errors: `-<data>\r\n`
                DataType::SimpleError(data) => {
                    self.buf.clear();
                    let data = write_str(&mut self.buf, data)?;
                    self.writer.write_u8(b'-').await?;
                    self.writer.write_all(data).await?;
                    self.writer.write_all(CRLF).await?;
                    1 + data.len() + CRLF.len()
                }

                // TODO: extend to BulkError with (`!` instead of `$`)
                // bulk strings: `$<length>\r\n<data>\r\n`
                DataType::BulkString(data) => {
                    self.buf.clear();
                    self.aux.clear();

                    let data = write_str(&mut self.aux, data)?;
                    let len = data.len();

                    write!(self.buf, "${len}\r\n")?;
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(data).await?;
                    self.writer.write_all(CRLF).await?;

                    self.buf.len() + len + CRLF.len()
                }

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

    /// Write a RDB file
    ///
    /// # Format
    /// `$<length_of_file>\r\n<contents_of_file>`
    ///
    /// Note that the format similar to how [Bulk String](DataType::BulkString)s are encoded, but
    /// without the trailing [CLRF].
    pub async fn write_rdb(&mut self, RDBData(data): RDBData) -> Result<()> {
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

    pub async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await.context("flush data")
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

            // null bulk string: `$-1\r\n`
            DataType::NullBulkString => self.buf.write_str("$-1\r\n")?,

            // boolean: `#<t|f>\r\n`
            DataType::Boolean(true) => self.buf.write_str("#t\r\n")?,
            DataType::Boolean(false) => self.buf.write_str("#f\r\n")?,

            // integer: `:[<+|->]<value>\r\n`
            DataType::Integer(int) => write!(self.buf, ":{int}\r\n")?,

            // simple strings: `+<data>\r\n`
            DataType::SimpleString(data) => {
                self.aux.clear();
                let data = write_str(&mut self.aux, data)?;
                self.buf.reserve(1 + data.len() + CRLF.len());
                self.buf.write_char('+')?;
                self.buf.put_slice(data);
                write!(self.buf, "\r\n")?;
            }

            // simple errors: `-<data>\r\n`
            DataType::SimpleError(data) => {
                self.aux.clear();
                let data = write_str(&mut self.aux, data)?;
                self.buf.reserve(1 + data.len() + CRLF.len());
                self.buf.write_char('-')?;
                self.buf.put_slice(data);
                write!(self.buf, "\r\n")?;
            }

            // bulk strings: `$<length>\r\n<data>\r\n`
            DataType::BulkString(data) => {
                self.aux.clear();
                let data = write_str(&mut self.aux, data)?;
                let len = data.len();
                self.buf.reserve(1 + CRLF.len() + len + CRLF.len());
                write!(self.buf, "${len}\r\n")?;
                self.buf.put_slice(data);
                write!(self.buf, "\r\n")?;
            }

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
    fn serialize_bulk_string() {
        test_serialize! {
            DataType::NullBulkString => b"$-1\r\n",
            DataType::string(DATA) => b"$12\r\nsome message\r\n"
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
        let items = keys.into_iter().zip(vals.into_iter());
        let expected = b"%2\r\n:1\r\n$12\r\nsome message\r\n:2\r\n$12\r\nsome message\r\n";
        test_serialize! {
            DataType::map([]) => b"%0\r\n",
            DataType::map(items) => expected
        }
    }
}
