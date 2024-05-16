use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::io::ErrorKind;
use std::num::{NonZeroU32, ParseIntError};
use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;
use std::{fmt::Debug, path::Path};

use anyhow::{anyhow, bail, ensure, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

use crate::cmd::{self, xadd, xrange, xread};
use crate::data::{DataExt, DataType};
use crate::rdb::{self, RDB};
use crate::store::{Database, DatabaseBuilder};
use crate::{Command, Error, RDBData, Resp, CRLF, FULLRESYNC, GET, LF, OK, PONG};

pub(crate) const MAGIC: Bytes = Bytes::from_static(b"REDIS");

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

        println!("reading RDB file length");

        let (Length::Some(len), _) = self.read_int().await.context("RDB file length")? else {
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

        Ok(RDBData(buf.freeze()))
    }

    /// https://rdb.fnordig.de/file_format.html
    pub async fn read_rdb_file(&mut self) -> Result<(RDB, usize)> {
        use rdb::opcode::*;

        // TODO: ideally reuse `self.buf` (but that one's currently Vec to `read_until`)
        let mut buf = BytesMut::with_capacity(4096);
        let mut bytes_read = 0;

        // read header (magic + version)
        let version = {
            buf.resize(MAGIC.len() + 4, 0);

            let n = self.reader.read_exact(&mut buf).await.context("magic")?;
            bytes_read += n;

            let magic = &buf[..MAGIC.len()];
            ensure!(magic == MAGIC, "RDB: expected {MAGIC:?}, got {magic:?}");

            let version = &buf[MAGIC.len()..n];
            std::str::from_utf8(version)
                .context("version is not ASCII")?
                .parse::<NonZeroU32>()
                .context("version is not a (valid) number")?
        };

        let mut aux = rdb::Aux::default();
        // TODO: default number of databases from config
        let mut dbs = HashMap::with_capacity(16);

        let mut db = None;
        let mut expiry = None;

        loop {
            let op = self.reader.read_u8().await.context("opcode")?;
            bytes_read += 1;
            match op {
                AUX => {
                    let (key, bytes_key) = rdb::read_string(&mut self.reader, &mut buf)
                        .await
                        .context("AUX field key")?;

                    let (val, bytes_val) = rdb::read_string(&mut self.reader, &mut buf)
                        .await
                        .context("AUX field value")?;

                    aux.set(key, val).context("set AUX field")?;

                    bytes_read += bytes_key + bytes_val;
                }

                SELECTDB => {
                    let default = if let Some(db) = db.replace(Database::builder()) {
                        let db = db.build()?;

                        let Entry::Vacant(entry) = dbs.entry(db.ix) else {
                            bail!("encountered two database with the same index={}", db.ix)
                        };
                        entry.insert(db);

                        false
                    } else {
                        true
                    };

                    let db = db.as_mut().expect("DB builder must exist");

                    let (ix, bytes_ix) = rdb::Length::read_from(&mut self.reader)
                        .await
                        .context("SELECTDB <number>")?;

                    let ix = ix.into_length();

                    if default {
                        ensure!(
                            ix == Database::DEFAULT,
                            "expected db={}, got db={ix}",
                            Database::DEFAULT
                        );
                    }

                    db.with_index(ix);
                    bytes_read += bytes_ix;
                }

                RESIZEDB => {
                    let Some(db) = db.as_mut() else {
                        bail!("RESIZEDB section before any SELECTDB");
                    };

                    let (size, bytes_size) = rdb::Length::read_from(&mut self.reader)
                        .await
                        .context("RESIZEDB hash table size")?;

                    let (expire_size, bytes_expire_size) = rdb::Length::read_from(&mut self.reader)
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
                    let (exp, bytes_expiry) = rdb::read_expiry(unit, &mut self.reader)
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
                    let Some(db) = db.take() else {
                        bail!("EOF before any SELECTDB");
                    };

                    let db = db.build()?;

                    let Entry::Vacant(entry) = dbs.entry(db.ix) else {
                        bail!("DBs with the same index={}", db.ix)
                    };
                    entry.insert(db);

                    break;
                }

                ty if db.as_ref().map_or(false, DatabaseBuilder::is_resized) => {
                    // NOTE: op here is the value type opcode
                    let Some(db) = db.as_mut() else {
                        unreachable!("DB must be properly initialized");
                    };

                    let (key, bytes_key) = rdb::read_string(&mut self.reader, &mut buf)
                        .await
                        .context("entry key")?;

                    let (val, bytes_val) = rdb::Value::read_from(ty, &mut self.reader, &mut buf)
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
        let checksum = if u32::from(version) >= 5 {
            buf.resize(8, 0);
            bytes_read += self.reader.read_exact(&mut buf).await.context("checksum")?;
            Some(buf.split().freeze())
        } else {
            None
        };

        let rdb = RDB {
            version,
            aux,
            dbs,
            checksum,
        };

        // println!("{rdb:?}");
        Ok((rdb, bytes_read))
    }

    pub async fn read_next(&mut self) -> Result<Option<(Resp, usize)>> {
        let Some((data, bytes_read)) = self.read_data().await? else {
            return Ok(None);
        };

        // println!("read {bytes_read}B of data: {data:?}");

        let (cmd, args): (_, &[DataType]) = match &data {
            s @ DataType::SimpleString(_) | s @ DataType::BulkString(_) => (s, &[]),
            DataType::Array(args) => args.split_first().expect("cannot read an empty array"),
            _ => return Ok(Some((data.into(), bytes_read))),
        };

        // TODO: decompose
        let cmd = match cmd.to_uppercase().as_slice() {
            b"OK" => return Ok(Some((DataType::str(OK).into(), bytes_read))),

            b"PING" => match args.first().cloned() {
                None => Command::Ping(None),
                Some(DataType::BulkString(msg)) => Command::Ping(Some(msg)),
                Some(arg) => bail!("PING only accepts bulk strings as argument, got {arg:?}"),
            },

            b"PONG" => return Ok(Some((DataType::str(PONG).into(), bytes_read))),

            b"ECHO" => match args.first().cloned() {
                Some(DataType::BulkString(msg)) => Command::Echo(msg),
                Some(arg) => bail!("ECHO only accepts bulk strings as argument, got {arg:?}"),
                None => bail!("ECHO requires an argument, got none"),
            },

            b"CONFIG" => {
                let Some(arg) = args.first() else {
                    bail!("protocol violation: CONFIG without arguments");
                };

                if !arg.matches(GET) {
                    bail!("protocol violation: expected 'CONFIG GET ...', got {arg:?}");
                };

                let params = args
                    .iter()
                    .skip(1)
                    .filter_map(|arg| match arg {
                        DataType::BulkString(s) | DataType::SimpleString(s) => s.bytes(),
                        _ => None,
                    })
                    .collect();

                Command::Config(params)
            }

            b"INFO" => {
                let sections = args
                    .iter()
                    .filter_map(|arg| match arg {
                        DataType::BulkString(s) | DataType::SimpleString(s) => s.bytes(),
                        _ => None,
                    })
                    .collect();

                Command::Info(sections)
            }

            b"TYPE" => match args.first().cloned() {
                Some(DataType::BulkString(key) | DataType::SimpleString(key)) => Command::Type(key),
                Some(arg) => bail!("TYPE only accepts strings as argument, got {arg:?}"),
                None => bail!("TYPE requires an argument, got none"),
            },

            b"KEYS" => match args.first().cloned() {
                Some(DataType::BulkString(pattern)) => Command::Keys(pattern),
                Some(arg) => bail!("KEYS only accepts bulk strings as argument, got {arg:?}"),
                None => bail!("KEYS requires an argument, got none"),
            },

            b"GET" => match args.first().cloned() {
                Some(DataType::BulkString(key)) => Command::Get(key),
                Some(arg) => bail!("GET only accepts bulk strings as argument, got {arg:?}"),
                None => bail!("GET requires an argument, got none"),
            },

            b"SET" => match (args.first().cloned(), args.get(1).cloned()) {
                (Some(DataType::BulkString(key)), Some(DataType::BulkString(val))) => {
                    let ops = cmd::set::Options::try_from(&args[2..])
                        .with_context(|| format!("SET {key:?} {val:?} [options]"))?;
                    Command::Set(key, rdb::Value::String(val), ops)
                }
                (Some(key), None) => bail!("SET {key:?} _ is missing a value argument"),
                (None, Some(val)) => bail!("SET _ {val:?} is missing a key argument"),
                (None, None) => bail!("SET requires two arguments, got none"),
                args => bail!("protocol violation: SET with invalid argument types {args:?}"),
            },

            b"XADD" => match xadd::XAdd::try_from(args) {
                Ok(xadd) => Command::from(xadd),
                Err(err) => return Ok(Some((Resp::from(DataType::err(err)), bytes_read))),
            },

            b"XRANGE" => match xrange::XRange::try_from(args) {
                Ok(xrange) => Command::from(xrange),
                Err(err) => return Ok(Some((Resp::from(DataType::err(err)), bytes_read))),
            },

            b"XREAD" => match xread::XRead::try_from(args) {
                Ok(xread) => Command::from(xread),
                Err(err) => return Ok(Some((Resp::from(DataType::err(err)), bytes_read))),
            },

            b"XLEN" => match args.first().cloned() {
                Some(DataType::BulkString(len) | DataType::SimpleString(len)) => Command::XLen(len),
                Some(arg) => bail!("XLEN key must be a string, got {arg:?}"),
                None => {
                    let err = Error::WrongNumArgs("xlen");
                    return Ok(Some((Resp::from(DataType::err(err)), bytes_read)));
                }
            },

            b"REPLCONF" => cmd::replconf::Conf::try_from(args).map(Command::Replconf)?,

            b"PSYNC" => match (args.first().cloned(), args.get(1).cloned()) {
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

            b"WAIT" => match (args.first().cloned(), args.get(1).cloned()) {
                (Some(num_replicas), Some(timeout)) => {
                    let Ok(DataType::Integer(num_replicas)) = num_replicas.parse_int() else {
                        bail!("protocol violation: WAIT with invalid numreplicas type");
                    };

                    ensure!(num_replicas >= 0, "WAIT: numreplicas must be non-negative");

                    let Ok(DataType::Integer(timeout)) = timeout.parse_int() else {
                        bail!("protocol violation: WAIT with invalid timeout type");
                    };

                    ensure!(timeout >= 0, "WAIT: timeout must be non-negative");

                    Command::Wait(num_replicas as usize, Duration::from_millis(timeout as u64))
                }
                (Some(num_replicas), None) => bail!("WAIT {num_replicas:?} _ is missing timeout"),
                (None, Some(timeout)) => bail!("WAIT _ {timeout:?} is missing numreplicas"),
                (None, None) => bail!("PSYNC requires two arguments, got none"),
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

            _ => {
                let err = DataType::err(Error::unkown_cmd(cmd, args));
                return Ok(Some((Resp::from(err), bytes_read)));
            }
        };

        Ok(Some((cmd.into(), bytes_read)))
    }

    async fn read_data(&mut self) -> Result<Option<(DataType, usize)>> {
        match self.reader.read_u8().await {
            Ok(b'_') => self.read_null().await.map(|(x, n)| (x, n + 1)).map(Some),
            Ok(b'#') => self.read_bool().await.map(|(b, n)| (b, n + 1)).map(Some),
            Ok(b':') => self
                .read_int()
                .await
                .map(|(i, n)| (DataType::Integer(i), n + 1))
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

    /// Read an integer of the form `[:][<+|->]<value>\r\n`
    async fn read_int<T>(&mut self) -> Result<(T, usize)>
    where
        T: FromStr,
        <T as FromStr>::Err: Debug,
    {
        let (bytes, n_read) = self.read_segment().await?;

        let Ok(slice) = std::str::from_utf8(bytes) else {
            bail!("expected UTF-8 digit sequence, got: {bytes:?}");
        };

        match slice.parse() {
            Ok(int) => Ok((int, n_read)),
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
        let (len, bytes_read) = self.read_int().await?;

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
            let (len, mut n_total) = self.read_int().await?;

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
            let (len, mut n_total) = self.read_int().await?;

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
