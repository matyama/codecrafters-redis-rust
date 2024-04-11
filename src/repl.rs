use std::fmt::Write;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::net::{tcp::OwnedReadHalf, TcpStream};
use tokio::time::timeout;

use crate::{
    Command, DataExt as _, DataReader, DataType, DataWriter, ReplState, Resp, PING, PSYNC, RDB,
    REPLCONF, TIMEOUT,
};

#[derive(Default)]
pub enum ReplSubscriber {
    #[default]
    Noop,
    RecvWrite(DataReader<OwnedReadHalf>),
}

impl ReplSubscriber {
    pub fn recv_write(
        mut self,
    ) -> Pin<Box<impl Future<Output = Result<(Self, Command), (Self, anyhow::Error)>> + 'static>>
    {
        Box::pin(async {
            let Self::RecvWrite(ref mut reader) = self else {
                return Err((
                    self,
                    anyhow!("trying to receive write commands on a noop subscriber"),
                ));
            };
            loop {
                match reader.read_next().await {
                    Ok(Some(Resp::Cmd(cmd))) if cmd.is_write() => break Ok((self, cmd)),
                    Ok(Some(resp)) => {
                        break Err((self, anyhow!("subscribed to write commands, got: {resp:?}")))
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(e) => break Err((self, e)),
                }
            }
        })
    }
}

impl std::fmt::Display for ReplSubscriber {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "ReplSubscriber::Noop"),
            Self::RecvWrite(_) => write!(f, "ReplSubscriber::RecvWrite"),
        }
    }
}

pub struct Replication {
    conn: TcpStream,
    repl: SocketAddr,
}

impl Replication {
    async fn new(leader: SocketAddr, replica: SocketAddr) -> Result<Self> {
        // NOTE: this could use some form of connection pooling
        let conn = TcpStream::connect(leader)
            .await
            .context("failed to establish connection")?;

        Ok(Self {
            conn,
            repl: replica,
        })
    }

    pub async fn handshake(
        leader: SocketAddr,
        replica: SocketAddr,
    ) -> Result<(RDB, ReplSubscriber)> {
        let (Replication { conn, .. }, rdb) = Self::new(leader, replica)
            .await?
            .ping()
            .await
            .context("handshake init stage (PING)")?
            .conf()
            .await
            .context("handshake config stage (REPLCONF)")?
            .sync()
            .await
            .context("handshake sync stage (PSYNC)")?;

        let (reader, _) = conn.into_split();
        let subscriber = ReplSubscriber::RecvWrite(DataReader::new(reader));

        Ok((rdb, subscriber))
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
        write!(port, "{}", self.repl.port())?;
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
            .with_context(|| format!("REPLCONF listening-port {}", self.repl.port()))?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if ok(&s) => {}
            other => bail!("unexpected response {other:?}"),
        }

        let replconf = [
            DataType::BulkString(REPLCONF),
            DataType::string(b"capa"),
            DataType::string(b"eof"),
            DataType::string(b"capa"),
            DataType::string(b"psync2"),
        ];

        let resp = self
            .request(replconf)
            .await
            .context("REPLCONF capa eof capa psync2")?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if ok(&s) => Ok(self),
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn sync(mut self) -> Result<(Self, RDB)> {
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
            // TODO: don't forget about the state
            Resp::Cmd(Command::FullResync(_state)) => {
                let (reader, _) = self.conn.split();
                let mut reader = DataReader::new(reader);
                let rdb = timeout(TIMEOUT, reader.read_rdb())
                    .await
                    .with_context(|| format!("reading RDB file after PSYNC {state}"))?
                    .with_context(|| format!("reading RDB file after PSYNC {state} timed out"))?;
                Ok((self, rdb))
            }
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn request<const N: usize>(&mut self, args: [DataType; N]) -> Result<Resp> {
        let (reader, writer) = self.conn.split();
        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        writer
            .write(&DataType::array(args))
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
