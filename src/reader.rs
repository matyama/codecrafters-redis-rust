use std::collections::VecDeque;
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::tcp::ReadHalf;

use crate::{Command, DataExt, DataType, CRLF, LF};

pub struct DataReader<'r> {
    reader: BufReader<ReadHalf<'r>>,
    buf: Vec<u8>,
}

impl<'r> DataReader<'r> {
    #[inline]
    pub fn new(reader: ReadHalf<'r>) -> Self {
        Self {
            reader: BufReader::new(reader),
            // TODO: customize capacity
            // here we'd ideally use some sort of buffer pooling
            buf: Vec::with_capacity(1024),
        }
    }

    pub async fn read_next(&mut self) -> Result<Option<Command>> {
        let Some(data) = self.read_data().await? else {
            return Ok(None);
        };

        let (cmd, mut args) = match data {
            s @ DataType::SimpleString(_) | s @ DataType::BulkString(_) => {
                (s.cmd(), VecDeque::new())
            }
            DataType::Array(mut args) => {
                let Some(arg) = args.pop_front() else {
                    bail!("protocol violation: no command found");
                };
                (arg.cmd(), args)
            }
            data => bail!("protocol violation: command must be an array or string, got {data:?}"),
        };

        let cmd = match cmd.as_ref() {
            b"ping" => match args.pop_front() {
                None => Command::Ping(None),
                Some(msg @ DataType::BulkString(_)) => Command::Ping(Some(msg)),
                Some(arg) => bail!("PING only accepts bulk strings as argument, got {arg:?}"),
            },

            b"echo" => match args.pop_front() {
                Some(msg @ DataType::BulkString(_)) => Command::Echo(msg),
                Some(arg) => bail!("ECHO only accepts bulk strings as argument, got {arg:?}"),
                None => bail!("ECHO requires an argument, got none"),
            },

            // TODO: implement other commands
            cmd => bail!(
                "protocol violation: unsupported command '{}'",
                String::from_utf8_lossy(cmd)
            ),
        };

        Ok(Some(cmd))
    }

    async fn read_data(&mut self) -> Result<Option<DataType>> {
        match self.reader.read_u8().await {
            Ok(b'#') => self.read_bool().await.map(Some),
            Ok(b'$') => self.read_bulk_string().await.map(Some),
            Ok(b'*') => self.read_array().await.map(Some),
            Ok(b) => bail!("protocol violation: unsupported control byte '{b}'"),
            Err(e) if matches!(e.kind(), ErrorKind::UnexpectedEof) => Ok(None),
            Err(e) => Err(e).context("failed to read next element"),
        }
    }

    /// Reads next _segment_ into the given buffer.
    ///
    /// Segment is a byte sequence ending with a CRLF byte sequence. Note that the trailing CRLF is
    /// stripped before return.
    async fn read_segment(&mut self) -> Result<usize> {
        let mut n = 0;

        while !self.buf[..n].ends_with(CRLF) {
            n += self
                .reader
                .read_until(LF, &mut self.buf)
                .await
                .context("failed to read payload up to next CRLF terminator")?;
        }

        // strip trailing CRLF
        self.buf.pop();
        self.buf.pop();

        Ok(n - 2)
    }

    async fn read_len(&mut self) -> Result<usize> {
        self.buf.clear();

        let n = self.read_segment().await?;

        let bytes = &self.buf[..n];

        let Ok(slice) = std::str::from_utf8(bytes) else {
            bail!("expected UTF-8 digit sequence, got: {bytes:?}");
        };

        slice
            .parse()
            .with_context(|| format!("expected number, got: {slice}"))
    }

    /// Read a boolean of the form `#<t|f>\r\n`
    async fn read_bool(&mut self) -> Result<DataType> {
        let boolean = self
            .reader
            .read_u8()
            .await
            .context("failed to read a boolean")?;

        match boolean {
            b't' => Ok(DataType::Boolean(true)),
            b'f' => Ok(DataType::Boolean(false)),
            b => bail!("protocol violation: invalid boolean byte {b}"),
        }
    }

    /// Read a bulk strings of the form `$<length>\r\n<data>\r\n`
    async fn read_bulk_string(&mut self) -> Result<DataType> {
        let len = self.read_len().await?;
        println!("reading bulk string of length: {len}");

        let read_len = len + CRLF.len();

        let mut buf = vec![0; read_len];

        self.reader
            .read_exact(&mut buf)
            .await
            .with_context(|| format!("failed to read {read_len} bytes of a bulk string's data"))?;

        // strip trailing CRLF
        buf.pop();
        buf.pop();

        // TODO: ideally this could point to a pooled buffer and not allocate
        Ok(DataType::BulkString(buf.into()))
    }

    /// Read an array of the form: `*<number-of-elements>\r\n<element-1>...<element-n>`
    #[must_use]
    fn read_array(&mut self) -> Pin<Box<dyn Future<Output = Result<DataType>> + Send + '_>> {
        let read = async move {
            let len = self.read_len().await?;
            println!("reading array of length: {len}");

            let mut items = VecDeque::with_capacity(len);

            // XXX: probably should also validate that all items are of the same type
            for i in 0..len {
                let item = self
                    .read_data()
                    .await
                    .with_context(|| format!("failed to read array item {i}"))?;

                let Some(item) = item else {
                    bail!("protocol violation: missing array item {i}");
                };

                items.push_back(item);
            }

            Ok(DataType::Array(items))
        };

        // TODO: try local pinning
        Box::pin(read)
    }
}
