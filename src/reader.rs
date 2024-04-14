use std::collections::VecDeque;
use std::fmt::Debug;
use std::future::Future;
use std::io::ErrorKind;
use std::num::ParseIntError;
use std::pin::Pin;
use std::str::FromStr;

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::{cmd, Command, DataExt, DataType, Resp, CRLF, FULLRESYNC, LF, OK, PONG, RDB};

pub struct DataReader<R> {
    reader: BufReader<R>,
    buf: Vec<u8>,
}

impl<R> DataReader<R>
where
    R: AsyncReadExt + Send + Unpin,
{
    // TODO: it's unfortunate that ad-hoc uses of this always allocate `buf`, make it reusable
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            // TODO: customize capacity
            // here we'd ideally use some sort of buffer pooling
            buf: Vec::with_capacity(1024),
        }
    }

    pub async fn read_rdb(&mut self) -> Result<RDB> {
        // NOTE: wait till leader starts sending the file after FULLRESYNC
        loop {
            match self.reader.read_u8().await {
                Ok(b'$') => break,
                Ok(b) => bail!("RDB file should start with b'$', got {b}"),
                Err(e) if matches!(e.kind(), ErrorKind::UnexpectedEof) => {
                    tokio::task::yield_now().await
                }
                Err(e) => bail!("failed to read initial byte of a RDB file: {e:?}"),
            }
        }

        println!("reading RDB file length");

        let Length::Some(len) = self.read_int().await.context("RDB file length")? else {
            bail!("cannot read RDB file without content");
        };

        println!("reading RDB file content ({len}B)");

        let mut buf = BytesMut::with_capacity(len);
        buf.resize(len, 0);

        self.reader
            .read_exact(&mut buf)
            .await
            .context("read RDB file contents")?;

        println!("RDB file hex: {:x?}", &buf[..len.saturating_sub(8)]);

        // 8-byte checksum
        let checksum = &buf[len.saturating_sub(8)..];
        println!("RDB checksum hex: {checksum:x?}");

        Ok(RDB(buf.freeze()))
    }

    pub async fn read_next(&mut self) -> Result<Option<Resp>> {
        let Some(data) = self.read_data().await? else {
            return Ok(None);
        };

        let (cmd, mut args) = match data {
            s @ DataType::SimpleString(_) | s @ DataType::BulkString(_) => {
                (s.to_uppercase(), VecDeque::new())
            }
            DataType::Array(mut args) => {
                let Some(arg) = args.pop_front() else {
                    bail!("protocol violation: no command found");
                };
                (arg.to_uppercase(), args)
            }
            data => return Ok(Some(data.into())),
        };

        let cmd = match cmd.as_slice() {
            b"OK" => return Ok(Some(DataType::SimpleString(OK).into())),

            b"PING" => match args.pop_front() {
                None => Command::Ping(None),
                Some(DataType::BulkString(msg)) => Command::Ping(Some(msg)),
                Some(arg) => bail!("PING only accepts bulk strings as argument, got {arg:?}"),
            },

            b"PONG" => return Ok(Some(DataType::SimpleString(PONG).into())),

            b"ECHO" => match args.pop_front() {
                Some(DataType::BulkString(msg)) => Command::Echo(msg),
                Some(arg) => bail!("ECHO only accepts bulk strings as argument, got {arg:?}"),
                None => bail!("ECHO requires an argument, got none"),
            },

            b"INFO" => {
                let sections = args
                    .into_iter()
                    .filter_map(|arg| match arg {
                        DataType::BulkString(s) | DataType::SimpleString(s) => Some(s),
                        _ => None,
                    })
                    .collect();

                Command::Info(sections)
            }

            b"GET" => match args.pop_front() {
                Some(DataType::BulkString(key)) => Command::Get(key.into()),
                Some(arg) => bail!("GET only accepts bulk strings as argument, got {arg:?}"),
                None => bail!("GET requires an argument, got none"),
            },

            b"SET" => match (args.pop_front(), args.pop_front()) {
                (Some(DataType::BulkString(key)), Some(DataType::BulkString(val))) => {
                    let ops = cmd::set::Options::try_from(args)
                        .with_context(|| format!("SET {key:?} {val:?} [options]"))?;
                    Command::Set(key.into(), val.into(), ops)
                }
                (Some(key), None) => bail!("SET {key:?} _ is missing a value argument"),
                (None, Some(val)) => bail!("SET _ {val:?} is missing a key argument"),
                (None, None) => bail!("SET requires two arguments, got none"),
                args => bail!("protocol violation: SET with invalid argument types {args:?}"),
            },

            b"REPLCONF" => cmd::replconf::Conf::try_from(args).map(Command::Replconf)?,

            b"PSYNC" => match (args.pop_front(), args.pop_front()) {
                (Some(DataType::BulkString(repl_id)), Some(DataType::BulkString(repl_offset))) => {
                    let state = (repl_id.clone(), repl_offset.clone())
                        .try_into()
                        .with_context(|| format!("PSYNC {repl_id:?} {repl_offset:?}"))?;
                    Command::PSync(state)
                }
                (Some(id), None) => bail!("PSYNC {id:?} _ is missing offset"),
                (None, Some(offset)) => bail!("PSYNC _ {offset:?} is missing ID"),
                (None, None) => bail!("PSYNC requires two arguments, got none"),
                args => bail!("protocol violation: PSYNC with invalid argument types {args:?}"),
            },

            // FULLRESYNC <REPL_ID> <REPL_OFFSET>
            cmd if cmd.starts_with(&FULLRESYNC) => {
                let args = cmd
                    .strip_prefix(FULLRESYNC.as_ref())
                    .expect("prefix checked above")
                    .split(|b| b.is_ascii_whitespace())
                    .collect::<Vec<_>>();

                // NOTE: first split item is the residuum from stripping FULLRESYNC prefix
                let [_, repl_id, repl_offset] = args[..] else {
                    bail!(
                        "protocol violation: FULLRESYNC requires two arguments, got {}",
                        String::from_utf8_lossy(cmd)
                    );
                };

                let repl_id = BytesMut::from(repl_id).freeze();

                let state = (repl_id, repl_offset)
                    .try_into()
                    .with_context(|| format!("{}", String::from_utf8_lossy(cmd)))?;

                Command::FullResync(state)
            }

            cmd => bail!(
                "protocol violation: unsupported command '{}'",
                String::from_utf8_lossy(cmd)
            ),
        };

        Ok(Some(cmd.into()))
    }

    async fn read_data(&mut self) -> Result<Option<DataType>> {
        match self.reader.read_u8().await {
            Ok(b'_') => self.read_null().await.map(Some),
            Ok(b'#') => self.read_bool().await.map(Some),
            Ok(b':') => self.read_int().await.map(DataType::Integer).map(Some),
            Ok(b'+') => self
                .read_simple()
                .await
                .map(DataType::SimpleString)
                .map(Some),
            Ok(b'-') => self
                .read_simple()
                .await
                .map(DataType::SimpleError)
                .map(Some),
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

    async fn read_null(&mut self) -> Result<DataType> {
        self.buf.clear();

        let n = self.read_segment().await?;

        ensure!(
            n == 0,
            "protocol violation: expected no data for null, got {n} bytes"
        );

        Ok(DataType::Null)
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

    /// Read an integer of the form `[:][<+|->]<value>\r\n`
    async fn read_int<T>(&mut self) -> Result<T>
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        self.buf.clear();

        let n = self.read_segment().await?;

        let bytes = &self.buf[..n];

        let Ok(slice) = std::str::from_utf8(bytes) else {
            bail!("expected UTF-8 digit sequence, got: {bytes:?}");
        };

        match slice.parse() {
            Ok(int) => Ok(int),
            Err(e) => Err(anyhow!("{e:?}: expected number, got: {slice}")),
        }
    }

    /// Read a simple strings or errors of the form `[<+|->]<data>\r\n`
    async fn read_simple(&mut self) -> Result<Bytes> {
        println!("reading simple data");
        self.buf.clear();
        let n = self.read_segment().await?;
        Ok(Bytes::copy_from_slice(&self.buf[..n]))
    }

    /// Read a bulk strings of the form `$<length>\r\n<data>\r\n`
    async fn read_bulk_string(&mut self) -> Result<DataType> {
        let Length::Some(len) = self.read_int().await? else {
            println!("reading null bulk string");
            return Ok(DataType::NullBulkString);
        };

        println!("reading bulk string of length: {len}");

        let read_len = len + CRLF.len();

        let mut buf = BytesMut::with_capacity(read_len);
        buf.resize(read_len, 0);

        self.reader
            .read_exact(&mut buf)
            .await
            .with_context(|| format!("failed to read {read_len} bytes of a bulk string's data"))?;

        // strip trailing CRLF
        buf.truncate(len);
        // TODO: do `let data = self.buf.split_to(len).freeze();` instead

        // TODO: ideally this could point to a pooled buffer and not allocate
        Ok(DataType::BulkString(buf.into()))
    }

    /// Read an array of the form: `*<number-of-elements>\r\n<element-1>...<element-n>`
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn read_array(&mut self) -> Pin<Box<dyn Future<Output = Result<DataType>> + Send + '_>> {
        Box::pin(async move {
            let len = self.read_int().await?;
            println!("reading array of length: {len}");

            let mut items = VecDeque::with_capacity(len);

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
        })
    }
}

#[derive(Debug)]
enum Length {
    None,
    Some(usize),
}

impl FromStr for Length {
    type Err = ParseIntError;

    #[inline]
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse()
            .map(Self::Some)
            .or_else(|_| s.parse::<isize>().map(|_| Self::None))
    }
}
