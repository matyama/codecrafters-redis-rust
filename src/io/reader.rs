use std::collections::HashSet;
use std::future::Future;
use std::io::{ErrorKind, Write as _};
use std::num::{NonZeroU32, ParseIntError};
use std::pin::Pin;
use std::str::FromStr;
use std::{fmt::Debug, path::Path};

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::cmd::{config, info, ping, replconf, select, set, sync, wait, xadd, xrange, xread};
use crate::data::{DataExt, DataType};
use crate::io::{Checksum, CRLF};
use crate::rdb::{self, RDBData, MAGIC, RDB};
use crate::store::{Database, DatabaseBuilder};
use crate::{resp::*, Command, Error, Resp};

const LF: u8 = b'\n'; // 10

trait ReadError {
    fn terminates_read(&self) -> bool;
}

impl ReadError for std::io::Error {
    #[inline]
    fn terminates_read(&self) -> bool {
        matches!(
            self.kind(),
            ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset | ErrorKind::ConnectionAborted
        )
    }
}

macro_rules! cmd_try_from {
    ($args:expr => $cmd:path as $name:expr) => {
        match $args.first() {
            Some(DataType::BulkString(arg) | DataType::SimpleString(arg)) => {
                Resp::Cmd($cmd(arg.clone()))
            }
            Some(arg) => Resp::Data(DataType::err(Error::err(format!(
                "{} argument must be a string, got {arg:?}",
                $name
            )))),
            None => Resp::Data(DataType::err(Error::WrongNumArgs($name))),
        }
    };

    ($args:expr => $cmd:ty) => {
        <$cmd>::try_from($args)
            .map(Command::from)
            .map_err(DataType::err)
            .map_or_else(Resp::Data, Resp::Cmd)
    };
}

pub struct DataReader<R> {
    reader: BufReader<R>,
    buf: Vec<u8>,
}

impl<R> DataReader<R>
where
    R: AsyncReadExt + Send + Unpin,
{
    #[inline]
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            // TODO: customize capacity
            // here we'd ideally use some sort of buffer pooling
            buf: Vec::with_capacity(1024),
        }
    }

    pub async fn read_rdb(&mut self) -> Result<RDBData> {
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

        let (Length::Some(len), _) = self.read_num().await.context("RDB file length")? else {
            bail!("cannot read RDB file without content");
        };

        let mut buf = BytesMut::with_capacity(len);
        buf.resize(len, 0);

        self.reader
            .read_exact(&mut buf)
            .await
            .context("read RDB file contents")?;

        Ok(RDBData(buf.freeze()))
    }

    /// https://rdb.fnordig.de/file_format.html
    pub async fn read_rdb_file(&mut self) -> Result<(RDB, usize)> {
        use rdb::opcode::*;

        // TODO: ideally reuse `self.buf` (but that one's currently Vec to `read_until`)
        let mut buf = BytesMut::with_capacity(4096);
        let mut bytes_read = 0;
        let mut cksum = Checksum::default();

        // read header (magic + version)
        let version = {
            buf.resize(MAGIC.len() + 4, 0);

            let n = self.reader.read_exact(&mut buf).await.context("magic")?;

            let magic = &buf[..MAGIC.len()];
            ensure!(magic == MAGIC, "RDB: expected {MAGIC:?}, got {magic:?}");

            let version = std::str::from_utf8(&buf[MAGIC.len()..n])
                .context("version is not ASCII")?
                .parse::<NonZeroU32>()
                .context("version is not a (valid) number")?;

            bytes_read += cksum.write(&buf[..n])?;

            version
        };

        let mut aux = rdb::Aux::default();
        // TODO: default number of databases from config
        let mut dbs = Vec::with_capacity(crate::config::DBNUM);
        let mut seen = HashSet::with_capacity(dbs.capacity());

        let mut db = None;
        let mut expiry = None;

        loop {
            let op = self.reader.read_u8().await.context("opcode")?;
            bytes_read += cksum.write(&[op])?;

            match op {
                AUX => {
                    let (key, bytes_key) = rdb::read_string(&mut self.reader, &mut buf, &mut cksum)
                        .await
                        .context("AUX field key")?;

                    let (val, bytes_val) = rdb::read_string(&mut self.reader, &mut buf, &mut cksum)
                        .await
                        .context("AUX field value")?;

                    aux.set(key, val).context("set AUX field")?;

                    bytes_read += bytes_key + bytes_val;
                }

                SELECTDB => {
                    let default = if let Some(db) = db.replace(Database::builder()) {
                        let db = db.build()?;

                        ensure!(!seen.contains(&db.id), "DBs with the same index={}", db.id);

                        seen.insert(db.id);
                        dbs.push(db);

                        false
                    } else {
                        true
                    };

                    let db = db.as_mut().expect("DB builder must exist");

                    let (id, bytes_id) = rdb::Length::read_from(&mut self.reader, &mut cksum)
                        .await
                        .context("SELECTDB <number>")?;

                    let id = id.into_length();

                    if default {
                        ensure!(id == 0, "expected default DB, got db={id}");
                    }

                    db.with_id(id);
                    bytes_read += bytes_id;
                }

                RESIZEDB => {
                    let Some(db) = db.as_mut() else {
                        bail!("RESIZEDB section before any SELECTDB");
                    };

                    let (size, bytes_size) = rdb::Length::read_from(&mut self.reader, &mut cksum)
                        .await
                        .context("RESIZEDB hash table size")?;

                    let (expire_size, bytes_expire_size) =
                        rdb::Length::read_from(&mut self.reader, &mut cksum)
                            .await
                            .context("RESIZEDB expire hash table size")?;

                    // NOTE: sizes are in terms of the number of entries
                    db.with_capacity(size.into_length() + expire_size.into_length());
                    debug_assert!(db.is_resized(), "DB should now accept key-value pairs");

                    bytes_read += bytes_size + bytes_expire_size;
                }

                unit @ (EXPIRETIME | EXPIRETIMEMS)
                    if db.as_ref().map_or(false, DatabaseBuilder::is_resized) =>
                {
                    let (exp, bytes_expiry) = rdb::read_expiry(unit, &mut self.reader, &mut cksum)
                        .await
                        .context("EXPIRETIME* entry expiration time")?;

                    if expiry.replace(exp).is_some() {
                        bail!("EXPIRETIME* before reading previous EXPIRETIME* key-value");
                    }

                    bytes_read += bytes_expiry;
                }

                EXPIRETIME | EXPIRETIMEMS => {
                    bail!("EXPIRETIME* section before any RESIZEDB")
                }

                EOF => {
                    let db = db
                        .take()
                        .unwrap_or_else(|| DatabaseBuilder::empty(0))
                        .build()?;

                    ensure!(!seen.contains(&db.id), "DBs with the same index={}", db.id);

                    seen.insert(db.id);
                    dbs.push(db);

                    break;
                }

                ty if db.as_ref().map_or(false, DatabaseBuilder::is_resized) => {
                    // NOTE: op here is the value type opcode
                    let Some(db) = db.as_mut() else {
                        unreachable!("DB must be properly initialized");
                    };

                    let (key, bytes_key) = rdb::read_string(&mut self.reader, &mut buf, &mut cksum)
                        .await
                        .context("entry key")?;

                    let (val, bytes_val) =
                        rdb::Value::read_from(ty, &mut self.reader, &mut buf, &mut cksum)
                            .await
                            .context("entry value")?;

                    // NOTE: expiry has potentially been set by EXPIRETIME* in previous iteration
                    db.set(key, val, expiry.take());

                    bytes_read += bytes_key + bytes_val;
                }

                op => bail!("unexpected opcode '{op:x}'"),
            }
        }

        // read checksum (8B)
        if u32::from(version) >= rdb::MIN_CKSUM_VERSION.into() {
            let checksum = self
                .reader
                .read_u64_le()
                .await
                .map(Checksum)
                .context("checksum")?;

            ensure!(
                cksum == checksum,
                "invalid checksum: expected '{checksum:x?}', got '{cksum:x?}'"
            );

            bytes_read += 8;
        }

        Ok((RDB { version, aux, dbs }, bytes_read))
    }

    pub async fn read_next(&mut self) -> Result<Option<(Resp, usize)>> {
        let Some((data, bytes_read)) = self.read_data().await? else {
            return Ok(None);
        };

        let (cmd, args): (_, &[DataType]) = match &data {
            s @ DataType::SimpleString(_) | s @ DataType::BulkString(_) => (s, &[]),
            DataType::Array(args) => args.split_first().expect("cannot read an empty array"),
            _ => return Ok(Some((Resp::from(data), bytes_read))),
        };

        // create a canonical (uppercase) command name
        self.buf.clear();
        cmd.write_into(&mut self.buf)?;
        self.buf.make_ascii_uppercase();

        let resp = match self.buf.as_slice() {
            DBSIZE => Resp::Cmd(Command::DBSize),
            SELECT => cmd_try_from!(args => select::Select),
            PING => cmd_try_from!(args => ping::Ping),
            ECHO => cmd_try_from!(args => Command::Echo as "echo"),
            CONFIG => cmd_try_from!(args => config::ConfigGet),
            INFO => cmd_try_from!(args => info::InfoSections),
            TYPE => cmd_try_from!(args => Command::Type as "type"),
            KEYS => cmd_try_from!(args => Command::Keys as "keys"),
            GET => cmd_try_from!(args => Command::Get as "get"),
            SET => cmd_try_from!(args => set::Set),
            INCR => cmd_try_from!(args => Command::Incr as "incr"),
            MULTI => Resp::Cmd(Command::Multi),
            EXEC => Resp::Cmd(Command::Exec),
            XADD => cmd_try_from!(args => xadd::XAdd),
            XRANGE => cmd_try_from!(args => xrange::XRange),
            XREAD => cmd_try_from!(args => xread::XRead),
            XLEN => cmd_try_from!(args => Command::XLen as "xlen"),
            REPLCONF => cmd_try_from!(args => replconf::Conf),
            PSYNC => cmd_try_from!(args => sync::PSync),
            WAIT => cmd_try_from!(args => wait::Wait),
            s if s.starts_with(FULLRESYNC) => cmd_try_from!(cmd => sync::FullResync),
            OK => Resp::from(DataType::str(OK)),
            PONG => Resp::Data(DataType::str(PONG)),
            _ => Resp::Data(DataType::err(Error::unkown_cmd(cmd, args))),
        };

        Ok(Some((resp, bytes_read)))
    }

    async fn read_data(&mut self) -> Result<Option<(DataType, usize)>> {
        match self.reader.read_u8().await {
            Ok(b'_') => self.read_null().await.map(|(x, n)| (x, n + 1)).map(Some),
            Ok(b'#') => self.read_bool().await.map(|(b, n)| (b, n + 1)).map(Some),
            Ok(b':') => self
                .read_num()
                .await
                .map(|(i, n)| (DataType::Integer(i), n + 1))
                .map(Some),
            Ok(b',') => self
                .read_num()
                .await
                .map(|(d, n)| (DataType::Double(d), n + 1))
                .map(Some),
            Ok(b'+') => self
                .read_simple()
                .await
                .map(|(s, n)| (DataType::SimpleString(s), n + 1))
                .map(Some),
            Ok(b'-') => self
                .read_simple()
                .await
                .map(|(s, n)| (DataType::SimpleError(s), n + 1))
                .map(Some),
            Ok(b'$') => self
                .read_bulk()
                .await
                .map(|(s, n)| (s.map_or(DataType::NullBulkString, DataType::string), n + 1))
                .map(Some),
            Ok(b'!') => self
                .read_bulk()
                .await
                .map(|(s, n)| (s.map_or(DataType::NullBulkError, DataType::error), n + 1))
                .map(Some),
            Ok(b'*') => self.read_array().await.map(|(a, n)| (a, n + 1)).map(Some),
            Ok(b'%') => self.read_map().await.map(|(m, n)| (m, n + 1)).map(Some),
            Ok(b) => bail!("protocol violation: unsupported control byte '{b}'"),
            Err(e) if e.terminates_read() => Ok(None),
            Err(e) => Err(e).context("failed to read next element"),
        }
    }

    /// Reads next _segment_ into an inner buffer.
    ///
    /// Segment is a byte sequence ending with a CRLF byte sequence. Note that the trailing CRLF is
    /// stripped before return.
    ///
    /// Returns a pair of
    ///  1. A view into `self.buf` of the data read (i.e., excluding CRLF)
    ///  2. The total number of bytes read (i.e., including the CRLF)
    async fn read_segment(&mut self) -> Result<(&[u8], usize)> {
        self.buf.clear();

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

        Ok((&self.buf, n))
    }

    async fn read_null(&mut self) -> Result<(DataType, usize)> {
        let (bytes, n_read) = self.read_segment().await?;

        ensure!(
            bytes.is_empty(),
            "protocol violation: expected no data for null, got {} bytes",
            bytes.len(),
        );

        Ok((DataType::Null, n_read))
    }

    /// Read a boolean of the form `#<t|f>\r\n`
    async fn read_bool(&mut self) -> Result<(DataType, usize)> {
        let (bytes, n_read) = self
            .read_segment()
            .await
            .context("failed to read a boolean")?;

        ensure!(
            bytes.len() == 1,
            "protocol violation: expected single byte for a boolean, got {} bytes",
            bytes.len(),
        );

        match bytes[0] {
            b't' => Ok((DataType::Boolean(true), n_read)),
            b'f' => Ok((DataType::Boolean(false), n_read)),
            b => bail!("protocol violation: invalid boolean byte {b}"),
        }
    }

    /// Read a number of one of the following forms
    ///  - Integer: `[:][<+|->]<value>\r\n`
    ///  - Double: `,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n`
    async fn read_num<T>(&mut self) -> Result<(T, usize)>
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        let (bytes, n_read) = self.read_segment().await?;

        let Ok(slice) = std::str::from_utf8(bytes) else {
            bail!("expected UTF-8 digit sequence, got: {bytes:?}");
        };

        match slice.parse() {
            Ok(num) => Ok((num, n_read)),
            Err(e) => Err(anyhow!("{e:?}: expected number, got: {slice}")),
        }
    }

    /// Read a simple strings or errors of the form `[<+|->]<data>\r\n`
    async fn read_simple(&mut self) -> Result<(rdb::String, usize)> {
        let (bytes, n_read) = self.read_segment().await?;
        let data = Bytes::copy_from_slice(bytes);
        Ok((data.into(), n_read))
    }

    /// Read a bulk strings|errors of the form `<$|!><length>\r\n<data>\r\n`
    async fn read_bulk(&mut self) -> Result<(Option<rdb::String>, usize)> {
        let (len, bytes_read) = self.read_num().await?;

        let Length::Some(len) = len else {
            return Ok((None, bytes_read));
        };

        let read_len = len + CRLF.len();

        let mut buf = BytesMut::with_capacity(read_len);
        buf.resize(read_len, 0);

        self.reader
            .read_exact(&mut buf)
            .await
            .with_context(|| format!("failed to read {read_len} bytes of bulk data"))?;

        // strip trailing CRLF
        buf.truncate(len);
        // TODO: do `let data = self.buf.split_to(len).freeze();` instead

        // TODO: ideally this could point to a pooled buffer and not allocate
        Ok((Some(buf.into()), bytes_read + read_len))
    }

    /// Read an array of the form: `*<number-of-elements>\r\n<element-1>...<element-n>`
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn read_array(&mut self) -> Pin<Box<ReadRecFut<'_>>> {
        Box::pin(async move {
            let (len, mut n_total) = self.read_num().await?;

            let mut items = Vec::with_capacity(len);

            for i in 0..len {
                let item = self
                    .read_data()
                    .await
                    .with_context(|| format!("failed to read array item {i}"))?;

                let Some((item, n)) = item else {
                    bail!("protocol violation: missing array item {i}");
                };

                items.push(item);
                n_total += n;
            }

            Ok((DataType::Array(items.into()), n_total))
        })
    }

    /// Read a map of the form: `%<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>`
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    fn read_map(&mut self) -> Pin<Box<ReadRecFut<'_>>> {
        Box::pin(async move {
            let (len, mut n_total) = self.read_num().await?;

            let mut items = Vec::with_capacity(len);

            for i in 0..len {
                let key = self
                    .read_data()
                    .await
                    .with_context(|| format!("failed to read map key {i}"))?;

                let Some((key, n_key)) = key else {
                    bail!("protocol violation: missing map key {i}");
                };

                let val = self
                    .read_data()
                    .await
                    .with_context(|| format!("failed to read map val {i}"))?;

                let Some((val, n_val)) = val else {
                    bail!("protocol violation: missing map val {i}");
                };

                items.push((key, val));
                n_total += n_key + n_val;
            }

            Ok((DataType::Map(items.into()), n_total))
        })
    }
}

type ReadRecFut<'a> = dyn Future<Output = Result<(DataType, usize)>> + Send + 'a;

#[repr(transparent)]
pub struct RDBFileReader(DataReader<File>);

impl RDBFileReader {
    pub async fn new(file: impl AsRef<Path>) -> Result<Option<Self>> {
        match File::open(file).await {
            Ok(file) => Ok(Some(Self(DataReader::new(file)))),
            Err(e) if matches!(e.kind(), ErrorKind::NotFound) => Ok(None),
            Err(e) => Err(e).context("failed to read RDB"),
        }
    }

    pub async fn read(&mut self) -> Result<(RDB, usize)> {
        self.0.read_rdb_file().await
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
