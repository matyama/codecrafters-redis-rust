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

    #[must_use]
    pub fn write(
        &mut self,
        resp: DataType,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        let write = async move {
            match resp {
                // nulls: `_\r\n`
                DataType::Null => {
                    println!("writing null");
                    self.writer.write_all(NULL).await?;
                }

                // booleans: `#<t|f>\r\n`
                DataType::Boolean(boolean) => {
                    self.writer.write_u8(b'#').await?;
                    let boolean = if boolean { b't' } else { b'f' };
                    println!("writing boolean {boolean:?}");
                    self.writer.write_u8(boolean).await?;
                    self.writer.write_all(CRLF).await?;
                }

                // simple strings: `+<data>\r\n`
                DataType::SimpleString(data) => {
                    self.writer.write_u8(b'+').await?;
                    println!("writing {data:?}");
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
        };

        // TODO: try local pinning
        Box::pin(write)
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.writer
            .flush()
            .await
            .context("failed to flush a response")
    }
}
