use std::borrow::Cow;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use repl::Handshake;
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

#[derive(Debug)]
pub struct ReplState {
    pub(crate) repl_id: Cow<'static, str>,
    pub(crate) repl_offset: usize,
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
            repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into(),
            repl_offset: 0,
        };

        let store = Store::default();

        let Some(leader) = cfg.replica_of else {
            return Ok(Self::Leader { repl, store, cfg });
        };

        let _handshake = Handshake::ping(leader, cfg.clone()).await?.conf().await?;
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

            // NOTE: for now we just ignore the payload and hard-code the response to PING
            writer.write(resp).await?;
        }

        println!("flushing and closing connection");
        writer.flush().await
    }
}
