use std::fmt::Write;
use std::future::Future;
use std::io::Write as _;
use std::pin::Pin;

use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::{DataType, CRLF, NULL, RDB};

pub struct DataWriter<W> {
    writer: BufWriter<W>,
    buf: String,
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
                    // println!("writing null");
                    self.writer.write_all(NULL).await?;
                    NULL.len()
                }

                // null bulk string: `$-1\r\n`
                DataType::NullBulkString => {
                    // println!("writing null bulk string");
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
                    // println!("writing boolean {boolean:?}");
                    self.writer.write_u8(boolean).await?;
                    self.writer.write_all(CRLF).await?;
                    2 + CRLF.len()
                }

                // integer: `:[<+|->]<value>\r\n`
                DataType::Integer(int) => {
                    self.buf.clear();
                    write!(self.buf, ":{int}\r\n")?;
                    // println!("writing integer {int}: {:?}", self.buf.as_bytes());
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.buf.len()
                }

                // simple strings: `+<data>\r\n`
                DataType::SimpleString(data) => {
                    self.writer.write_u8(b'+').await?;
                    // println!("writing string {data:?}");
                    self.writer.write_all(data).await?;
                    self.writer.write_all(CRLF).await?;
                    1 + data.len() + CRLF.len()
                }

                // simple errors: `-<data>\r\n`
                DataType::SimpleError(data) => {
                    self.writer.write_u8(b'-').await?;
                    // println!("writing error {data:?}");
                    self.writer.write_all(data).await?;
                    self.writer.write_all(CRLF).await?;
                    1 + data.len() + CRLF.len()
                }

                // bulk strings: `$<length>\r\n<data>\r\n`
                DataType::BulkString(data) => {
                    self.writer.write_u8(b'$').await?;
                    self.buf.clear();
                    write!(self.buf, "{}\r\n", data.len())?;
                    // println!(
                    //     "writing bulk string length of {}: {:?}",
                    //     data.len(),
                    //     self.buf.as_bytes()
                    // );
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    // println!("writing {data:?}");
                    self.writer.write_all(data).await?;
                    self.writer.write_all(CRLF).await?;
                    1 + self.buf.len() + data.len() + CRLF.len()
                }

                // array: `*<number-of-elements>\r\n<element-1>...<element-n>`
                DataType::Array(items) => {
                    self.writer.write_u8(b'*').await?;

                    self.buf.clear();
                    write!(self.buf, "{}", items.len())?;
                    // println!(
                    //     "writing array length of {}: {:?}",
                    //     items.len(),
                    //     self.buf.as_bytes()
                    // );
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(CRLF).await?;

                    let mut bytes_written = 1 + self.buf.len() + CRLF.len();

                    if items.is_empty() {
                        return Ok(bytes_written);
                    }

                    for (i, item) in items.iter().enumerate() {
                        // print!("array[{i}]: ");
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
                        // print!("map[{key:?}]: {value:?}");
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

            //self.flush().await?;

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
    pub async fn write_rdb(&mut self, RDB(data): RDB) -> Result<()> {
        self.writer.write_u8(b'$').await?;

        self.buf.clear();
        write!(self.buf, "{}", data.len())?;
        // println!(
        //     "writing RDB file length of {}: {:?}",
        //     data.len(),
        //     self.buf.as_bytes()
        // );
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

impl Serializer for BytesMut {
    fn serialize(&mut self, resp: &DataType) -> Result<()> {
        match resp {
            // nulls: `_\r\n`
            DataType::Null => self.writer().write_all(NULL)?,

            // null bulk string: `$-1\r\n`
            DataType::NullBulkString => self.write_str("$-1\r\n")?,

            // boolean: `#<t|f>\r\n`
            DataType::Boolean(true) => self.write_str("#t\r\n")?,
            DataType::Boolean(false) => self.write_str("#f\r\n")?,

            // integer: `:[<+|->]<value>\r\n`
            DataType::Integer(int) => write!(self, ":{int}\r\n")?,

            // simple strings: `+<data>\r\n`
            DataType::SimpleString(data) => {
                self.write_char('+')?;
                self.writer().write_all(data)?;
                write!(self, "\r\n")?;
            }

            // simple errors: `-<data>\r\n`
            DataType::SimpleError(data) => {
                self.write_char('-')?;
                self.writer().write_all(data)?;
                write!(self, "\r\n")?;
            }

            // bulk strings: `$<length>\r\n<data>\r\n`
            DataType::BulkString(data) => {
                write!(self, "${}\r\n", data.len())?;
                self.writer().write_all(data)?;
                write!(self, "\r\n")?;
            }

            // array: `*<number-of-elements>\r\n<element-1>...<element-n>`
            DataType::Array(items) if items.is_empty() => write!(self, "*{}\r\n", items.len())?,
            DataType::Array(items) => {
                write!(self, "*{}\r\n", items.len())?;
                for (i, item) in items.iter().enumerate() {
                    self.serialize(item)
                        .with_context(|| format!("failed to serialize array item {i}"))?;
                }
            }

            // map: `%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>`
            DataType::Map(items) if items.is_empty() => write!(self, "%{}\r\n", items.len())?,
            DataType::Map(items) => {
                write!(self, "%{}\r\n", items.len())?;
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
        let mut buf = Self::new();
        buf.serialize(resp)?;
        Ok(buf.len())
    }
}

#[cfg(test)]
mod tests {

    use bytes::Bytes;

    use super::*;

    const DATA: Bytes = Bytes::from_static(b"some message");

    macro_rules! test_serialize {
        ( $($data:expr => $expected:expr),+ ) => {
            let mut buf = BytesMut::new();
            $(
                buf.clear();
                buf.serialize(&$data).expect("serialized");
                assert_eq!(buf.as_ref(), $expected);
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
            DataType::BulkString(DATA) => b"$12\r\nsome message\r\n"
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
            DataType::SimpleString(DATA) => b"+some message\r\n",
            DataType::SimpleError(DATA) => b"-some message\r\n"
        }
    }

    #[test]
    fn serialize_array() {
        let ints = DataType::array([DataType::Integer(1), DataType::Integer(2)]);
        let array = DataType::array([DataType::BulkString(DATA), DataType::BulkString(DATA)]);
        test_serialize! {
            DataType::array([]) => b"*0\r\n",
            ints => b"*2\r\n:1\r\n:2\r\n",
            array => b"*2\r\n$12\r\nsome message\r\n$12\r\nsome message\r\n"
        }
    }

    #[test]
    fn serialize_map() {
        let keys = [DataType::Integer(1), DataType::Integer(2)];
        let vals = [DataType::BulkString(DATA), DataType::BulkString(DATA)];
        let items = keys.into_iter().zip(vals.into_iter());
        let expected = b"%2\r\n:1\r\n$12\r\nsome message\r\n:2\r\n$12\r\nsome message\r\n";
        test_serialize! {
            DataType::map([]) => b"%0\r\n",
            DataType::map(items) => expected
        }
    }
}
