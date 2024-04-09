use std::fmt::Write;
use std::net::SocketAddr;

use anyhow::{bail, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

use crate::{
    Command, Config, DataExt as _, DataReader, DataType, DataWriter, ReplState, Resp, PING, PSYNC,
    REPLCONF,
};

const TIMEOUT: Duration = Duration::from_secs(10);

pub struct Replication {
    conn: TcpStream,
    cfg: Config,
}

impl Replication {
    async fn new(leader: SocketAddr, cfg: Config) -> Result<Self> {
        // NOTE: this could use some form of connection pooling
        let conn = TcpStream::connect(leader)
            .await
            .context("failed to establish connection")?;

        Ok(Self { conn, cfg })
    }

    pub async fn handshake(leader: SocketAddr, cfg: Config) -> Result<Self> {
        Self::new(leader, cfg)
            .await?
            .ping()
            .await
            .context("handshake init stage (PING)")?
            .conf()
            .await
            .context("handshake config stage (REPLCONF)")?
            .sync()
            .await
            .context("handshake sync stage (PSYNC)")
    }

    async fn ping(mut self) -> Result<Self> {
        let pong = |s| String::from_utf8_lossy(s).to_uppercase() == "PONG";
        let args = [DataType::BulkString(PING)];
        match self.request(args).await.context("init replication")? {
            Resp::Data(DataType::SimpleString(s)) if pong(&s) => Ok(self),
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn conf(mut self) -> Result<Self> {
        let mut port = BytesMut::with_capacity(4);
        write!(port, "{}", self.cfg.addr.port())?;
        let port = port.freeze();

        // TODO: replace with an optimized Matcher<'_> that uses Cow internally and is_lowercase
        let ok = |s: &Bytes| s.to_uppercase() == b"OK";

        let replconf = [
            DataType::BulkString(REPLCONF),
            DataType::string(b"listening-port"),
            DataType::BulkString(port),
        ];

        let resp = self
            .request(replconf)
            .await
            .with_context(|| format!("REPLCONF listening-port {}", self.cfg.addr.port()))?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if ok(&s) => {}
            other => bail!("unexpected response {other:?}"),
        }

        let replconf = [
            DataType::BulkString(REPLCONF),
            DataType::string(b"capa"),
            DataType::string(b"psync2"),
        ];

        let resp = self
            .request(replconf)
            .await
            .context("REPLCONF capa psync2")?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if ok(&s) => Ok(self),
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn sync(mut self) -> Result<Self> {
        let state = ReplState::default();

        let psync = [
            DataType::BulkString(PSYNC),
            DataType::BulkString(state.repl_id.clone().unwrap_or_default().into()),
            DataType::BulkString(state.repl_offset.to_string().into()),
        ];

        let resp = self
            .request(psync)
            .await
            .with_context(|| format!("PSYNC {state}"))?;

        match resp {
            Resp::Cmd(Command::FullResync(_state)) => Ok(self),
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn request<const N: usize>(&mut self, args: [DataType; N]) -> Result<Resp> {
        let (reader, writer) = self.conn.split();
        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        writer
            .write(DataType::array(args))
            .await
            .context("failed to write request")?;

        writer.flush().await.context("failed to flush request")?;

        let resp = timeout(TIMEOUT, reader.read_next())
            .await
            .context("request timed out")?
            .context("no response")?;

        match resp {
            Some(resp) => Ok(resp),
            other => bail!("unexpected response {other:?}"),
        }
    }
}
