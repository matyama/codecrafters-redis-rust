use std::fmt::Write;
use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::tcp::WriteHalf;

use crate::{DataType, CRLF, NULL};

pub struct DataWriter<'w> {
    writer: BufWriter<WriteHalf<'w>>,
    buf: String,
}

impl<'w> DataWriter<'w> {
    #[inline]
    pub fn new(writer: WriteHalf<'w>) -> Self {
        Self {
            writer: BufWriter::new(writer),
            buf: String::with_capacity(16),
        }
    }

    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub fn write(
        &mut self,
        resp: DataType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            match resp {
                // nulls: `_\r\n`
                DataType::Null => {
                    println!("writing null");
                    self.writer.write_all(NULL).await?;
                }

                // null bulk string: `$-1\r\n`
                DataType::NullBulkString => {
                    println!("writing null bulk string");
                    self.buf.clear();
                    write!(self.buf, "$-1")?;
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(CRLF).await?;
                }

                // booleans: `#<t|f>\r\n`
                DataType::Boolean(boolean) => {
                    self.writer.write_u8(b'#').await?;
                    let boolean = if boolean { b't' } else { b'f' };
                    println!("writing boolean {boolean:?}");
                    self.writer.write_u8(boolean).await?;
                    self.writer.write_all(CRLF).await?;
                }

                // integer: `:[<+|->]<value>\r\n`
                DataType::Integer(int) => {
                    self.writer.write_u8(b':').await?;
                    let sign = if int.is_positive() { '+' } else { '-' };
                    self.buf.clear();
                    write!(self.buf, "{sign}{int}\r\n")?;
                    println!("writing integer {int}: {:?}", self.buf.as_bytes());
                    self.writer.write_all(self.buf.as_bytes()).await?;
                }

                // simple strings: `+<data>\r\n`
                DataType::SimpleString(data) => {
                    self.writer.write_u8(b'+').await?;
                    println!("writing string {data:?}");
                    self.writer.write_all(&data).await?;
                    self.writer.write_all(CRLF).await?;
                }

                // simple errors: `-<data>\r\n`
                DataType::SimpleError(data) => {
                    self.writer.write_u8(b'-').await?;
                    println!("writing error {data:?}");
                    self.writer.write_all(&data).await?;
                    self.writer.write_all(CRLF).await?;
                }

                // bulk strings: `$<length>\r\n<data>\r\n`
                DataType::BulkString(data) => {
                    self.writer.write_u8(b'$').await?;
                    self.buf.clear();
                    write!(self.buf, "{}\r\n", data.len())?;
                    println!(
                        "writing bulk string length of {}: {:?}",
                        data.len(),
                        self.buf.as_bytes()
                    );
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    println!("writing {data:?}");
                    self.writer.write_all(&data).await?;
                    self.writer.write_all(CRLF).await?;
                }

                // array: `*<number-of-elements>\r\n<element-1>...<element-n>`
                DataType::Array(items) => {
                    self.writer.write_u8(b'*').await?;

                    self.buf.clear();
                    write!(self.buf, "{}\r\n", items.len())?;
                    println!(
                        "writing array length of {}: {:?}",
                        items.len(),
                        self.buf.as_bytes()
                    );
                    self.writer.write_all(self.buf.as_bytes()).await?;
                    self.writer.write_all(CRLF).await?;

                    if items.is_empty() {
                        return Ok(());
                    }

                    for (i, item) in items.into_iter().enumerate() {
                        print!("array[{i}]: ");
                        self.write(item)
                            .await
                            .with_context(|| format!("failed to write array item {i}"))?;
                    }
                }
            }

            self.flush().await
        })
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .await
            .context("failed to flush a response")
    }
}
