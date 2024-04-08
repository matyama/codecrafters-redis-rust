use std::fmt::Write;
use std::{marker::PhantomData, net::SocketAddr};

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

use crate::{Config, DataExt as _, DataReader, DataType, DataWriter, Resp, PING};

const TIMEOUT: Duration = Duration::from_secs(10);

const REPLCONF: Bytes = Bytes::from_static(b"REPLCONF");

pub struct Connected;
pub struct Configured;
// pub struct Synced;

pub trait HandshakeState {}

impl HandshakeState for () {}
impl HandshakeState for Connected {}
impl HandshakeState for Configured {}

pub struct Handshake<S: HandshakeState = ()> {
    leader: SocketAddr,
    conn: TcpStream,
    cfg: Config,
    state: PhantomData<S>,
}

impl Handshake<()> {
    pub async fn ping(leader: SocketAddr, cfg: Config) -> Result<Handshake<Connected>> {
        // NOTE: this could use some form of connection pooling
        let mut conn = TcpStream::connect(leader)
            .await
            .with_context(|| format!("handshake({leader}): failed to establish connection"))?;

        let (reader, writer) = conn.split();
        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        let ping = DataType::BulkString(PING);

        writer
            .write(DataType::array([ping]))
            .await
            .with_context(|| format!("handshake({leader}): init replication"))?;

        writer
            .flush()
            .await
            .with_context(|| format!("handshake({leader}): failed to flush PING"))?;

        let resp = timeout(TIMEOUT, reader.read_next())
            .await
            .with_context(|| format!("handshake({leader}): PING timed out"))?
            .with_context(|| format!("handshake({leader}): no response to PING"))?;

        let pong = |s| String::from_utf8_lossy(s).to_uppercase() == "PONG";

        match resp {
            Some(Resp::Data(DataType::SimpleString(s))) if pong(&s) => Ok(Handshake {
                leader,
                conn,
                cfg,
                state: PhantomData,
            }),
            other => bail!("handshake({leader}): unexpected response to PING {other:?}"),
        }
    }
}

impl Handshake<Connected> {
    pub async fn conf(mut self) -> Result<Handshake<Configured>> {
        let (reader, writer) = self.conn.split();
        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        let mut port = BytesMut::with_capacity(4);
        write!(port, "{}", self.cfg.addr.port())?;
        let port = port.freeze();

        let replconf = [
            DataType::BulkString(REPLCONF),
            DataType::string(b"listening-port"),
            DataType::BulkString(port),
        ];

        writer
            .write(DataType::array(replconf))
            .await
            .with_context(|| {
                format!(
                    "handshake({}): REPLCONF listening-port {}",
                    self.leader,
                    self.cfg.addr.port()
                )
            })?;

        writer
            .flush()
            .await
            .with_context(|| format!("handshake({}): failed to flush REPLCONF", self.leader))?;

        let ok = |s: &Bytes| s.to_uppercase() == b"OK";

        let resp = timeout(TIMEOUT, reader.read_next())
            .await
            .with_context(|| format!("handshake({}): REPLCONF timed out", self.leader))?
            .with_context(|| format!("handshake({}): no response to REPLCONF", self.leader))?;

        match resp {
            Some(Resp::Data(DataType::SimpleString(s))) if ok(&s) => {}
            other => bail!(
                "handshake({}): unexpected response to REPLCONF {other:?}",
                self.leader
            ),
        }

        let replconf = [
            DataType::BulkString(REPLCONF),
            DataType::string(b"capa"),
            DataType::string(b"psync2"),
        ];

        writer
            .write(DataType::array(replconf))
            .await
            .with_context(|| format!("handshake({}): REPLCONF capa psync2", self.leader,))?;

        writer
            .flush()
            .await
            .with_context(|| format!("handshake({}): failed to flush REPLCONF", self.leader))?;

        let resp = timeout(TIMEOUT, reader.read_next())
            .await
            .with_context(|| format!("handshake({}): REPLCONF timed out", self.leader))?
            .with_context(|| format!("handshake({}): no response to REPLCONF", self.leader))?;

        match resp {
            Some(Resp::Data(DataType::SimpleString(s))) if ok(&s) => Ok(Handshake {
                leader: self.leader,
                conn: self.conn,
                cfg: self.cfg,
                state: PhantomData,
            }),
            other => bail!(
                "handshake({}): unexpected response to REPLCONF {other:?}",
                self.leader
            ),
        }
    }
}
