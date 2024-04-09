use std::net::SocketAddr;
use std::sync::Arc;
use std::{collections::VecDeque, fmt::Debug};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use repl::Replication;
use tokio::net::TcpStream;

pub(crate) use cmd::Command;
pub(crate) use config::Config;
pub(crate) use reader::DataReader;
pub(crate) use store::Store;
pub(crate) use writer::DataWriter;

pub(crate) mod cmd;
pub(crate) mod config;
pub(crate) mod reader;
pub(crate) mod repl;
pub(crate) mod store;
pub(crate) mod writer;

pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
pub(crate) const NULL: &[u8] = b"_\r\n";

pub(crate) const PING: Bytes = Bytes::from_static(b"PING");
pub(crate) const PONG: Bytes = Bytes::from_static(b"PONG");
pub(crate) const OK: Bytes = Bytes::from_static(b"OK");
pub(crate) const REPLCONF: Bytes = Bytes::from_static(b"REPLCONF");
pub(crate) const PSYNC: Bytes = Bytes::from_static(b"PSYNC");
pub(crate) const FULLRESYNC: Bytes = Bytes::from_static(b"FULLRESYNC");

pub(crate) const DEFAULT_REPL_ID: Bytes = Bytes::from_static(b"?");

// NOTE: this is based on the codecrafters examples
pub const PROTOCOL: Protocol = Protocol::RESP2;

pub enum Protocol {
    RESP2,
    RESP3,
}

#[derive(Debug)]
pub enum Resp {
    Cmd(Command),
    Data(DataType),
}

impl From<Command> for Resp {
    #[inline]
    fn from(cmd: Command) -> Self {
        Self::Cmd(cmd)
    }
}

impl From<DataType> for Resp {
    #[inline]
    fn from(resp: DataType) -> Self {
        Self::Data(resp)
    }
}

pub trait DataExt {
    // NOTE: this could probably benefit from small vec optimization
    fn to_uppercase(&self) -> Vec<u8>;
    fn to_lowercase(&self) -> Vec<u8>;
}

impl DataExt for Bytes {
    #[inline]
    fn to_uppercase(&self) -> Vec<u8> {
        self.to_ascii_uppercase()
    }

    #[inline]
    fn to_lowercase(&self) -> Vec<u8> {
        self.to_ascii_lowercase()
    }
}

#[derive(Debug)]
pub enum DataType {
    Null,
    NullBulkString,
    Boolean(bool),
    Integer(i64),
    SimpleString(Bytes),
    SimpleError(Bytes),
    BulkString(Bytes),
    Array(VecDeque<DataType>),
}

impl DataType {
    #[inline]
    pub(crate) fn string(bytes: &'static [u8]) -> Self {
        Self::BulkString(Bytes::from_static(bytes))
    }

    #[inline]
    pub(crate) fn array(items: impl Into<VecDeque<Self>>) -> Self {
        Self::Array(items.into())
    }

    pub(crate) fn parse_int(self) -> Result<Self> {
        match self {
            Self::Null => bail!("null cannot be converted to an integer"),
            Self::Boolean(b) => Ok(Self::Integer(b.into())),
            i @ Self::Integer(_) => Ok(i),
            Self::SimpleString(s) | Self::BulkString(s) => {
                let s = std::str::from_utf8(&s)
                    .with_context(|| format!("{s:?} is not a UTF-8 string"))?;
                s.parse()
                    .map(Self::Integer)
                    .with_context(|| format!("'{s}' does not represent an integer"))
            }
            Self::NullBulkString => bail!("null bulk string cannot be converted to an integer"),
            Self::SimpleError(_) => bail!("simple error cannot be converted to an integer"),
            Self::Array(_) => bail!("array cannot be converted to an integer"),
        }
    }
}

impl DataExt for DataType {
    fn to_uppercase(&self) -> Vec<u8> {
        match self {
            Self::NullBulkString => vec![],
            Self::SimpleString(s) => s.to_uppercase(),
            Self::BulkString(s) => s.to_uppercase(),
            other => {
                let mut other = format!("{other:?}");
                other.make_ascii_uppercase();
                other.into()
            }
        }
    }

    fn to_lowercase(&self) -> Vec<u8> {
        match self {
            Self::NullBulkString => vec![],
            Self::SimpleString(s) => s.to_lowercase(),
            Self::BulkString(s) => s.to_lowercase(),
            other => {
                let mut other = format!("{other:?}");
                other.make_ascii_lowercase();
                other.into()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[repr(transparent)]
pub struct ReplId(Bytes);

impl ReplId {
    #[inline]
    pub fn new(repl_id: Bytes) -> Result<Option<ReplId>> {
        match repl_id.as_ref() {
            b"?" => Ok(None),
            id if id.is_ascii() && id.len() == 40 => Ok(Some(Self(repl_id))),
            id => bail!("invalid REPL_ID: {id:?}"),
        }
    }
}

impl From<ReplId> for Bytes {
    #[inline]
    fn from(ReplId(id): ReplId) -> Self {
        id
    }
}

impl Default for ReplId {
    #[inline]
    fn default() -> Self {
        Self(DEFAULT_REPL_ID)
    }
}

impl std::fmt::Display for ReplId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Ok(repl_id) = std::str::from_utf8(&self.0) else {
            unreachable!("REPL_ID is valid UTF-8 by construction");
        };
        f.write_str(repl_id)
    }
}

#[derive(Debug)]
pub struct ReplState {
    /// Pseudo random alphanumeric string of 40 characters
    repl_id: Option<ReplId>,
    repl_offset: isize,
}

impl<B> TryFrom<(Bytes, B)> for ReplState
where
    B: std::ops::Deref<Target = [u8]> + Debug,
{
    type Error = anyhow::Error;

    fn try_from((repl_id, repl_offset): (Bytes, B)) -> Result<Self> {
        let repl_id = ReplId::new(repl_id)?;

        let Ok(repl_offset) = std::str::from_utf8(&repl_offset) else {
            bail!("non-UTF-8 REPL_OFFSET: {repl_offset:?}");
        };

        let Ok(repl_offset) = repl_offset.parse() else {
            bail!("non-int REPL_OFFSET: {repl_offset}");
        };

        Ok(ReplState {
            repl_id,
            repl_offset,
        })
    }
}

impl Default for ReplState {
    #[inline]
    fn default() -> Self {
        Self {
            repl_id: None,
            repl_offset: -1,
        }
    }
}

impl std::fmt::Display for ReplState {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {}",
            self.repl_id.clone().unwrap_or_default(),
            self.repl_offset
        )
    }
}

#[derive(Debug)]
pub enum Instance {
    Leader {
        repl: ReplState,
        store: Store,
        cfg: Config,
    },
    Replica {
        repl: ReplState,
        store: Store,
        cfg: Config,
    },
}

impl Instance {
    pub async fn new(cfg: Config) -> Result<Self> {
        // TODO: this is just an initial placeholder replication state
        let repl = ReplState {
            repl_id: ReplId::new(Bytes::from_static(
                b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            ))
            .expect("valid REPL_ID"),
            repl_offset: 0,
        };

        let store = Store::default();

        let Some(leader) = cfg.replica_of else {
            return Ok(Self::Leader { repl, store, cfg });
        };

        let _ = Replication::handshake(leader, cfg.clone())
            .await
            .with_context(|| format!("handshake with leader at {leader}"))?;

        Ok(Self::Replica { repl, store, cfg })
    }

    #[inline]
    pub(crate) fn cfg(&self) -> &Config {
        match self {
            Self::Leader {
                repl: _,
                store: _,
                cfg,
            } => cfg,
            Self::Replica {
                repl: _,
                store: _,
                cfg,
            } => cfg,
        }
    }

    #[inline]
    pub(crate) fn store(&self) -> &Store {
        let (Self::Leader { repl: _, store, .. } | Self::Replica { repl: _, store, .. }) = self;
        store
    }

    #[inline]
    pub fn addr(&self) -> SocketAddr {
        self.cfg().addr
    }

    pub async fn handle_connection(self: Arc<Self>, mut stream: TcpStream) -> Result<()> {
        let (reader, writer) = stream.split();

        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        while let Some(resp) = reader.read_next().await? {
            let resp = match resp {
                Resp::Cmd(cmd) => {
                    println!("executing {cmd:?}");
                    cmd.exec(Arc::clone(&self)).await
                }
                Resp::Data(resp) => {
                    bail!("protocol violation: expected a command, got {resp:?} instead")
                }
            };

            writer.write(resp).await?;
        }

        println!("flushing and closing connection");
        writer.flush().await
    }
}
