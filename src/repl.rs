use std::fmt::Write;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use anyhow::{anyhow, bail, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::{
    Command, DataExt as _, DataReader, DataType, DataWriter, Offset, ReplState, Resp, PING, PSYNC,
    RDB, REPLCONF, TIMEOUT,
};

// NOTE: it's actually necessary not to shutdown the write part to keep the connection alive
// TODO: support reconnect
pub struct Connection {
    reader: DataReader<OwnedReadHalf>,
    writer: DataWriter<OwnedWriteHalf>,
}

pub type RecvResult<T> = Result<(T, Command, Offset), (T, anyhow::Error)>;

#[derive(Default)]
pub enum ReplConnection {
    #[default]
    Noop,
    Recv(Connection),
}

impl ReplConnection {
    pub fn recv(mut self) -> Pin<Box<impl Future<Output = RecvResult<Self>> + 'static>> {
        Box::pin(async {
            let Self::Recv(ref mut conn) = self else {
                return Err((
                    self,
                    anyhow!("trying to receive replication commands on a noop connection"),
                ));
            };
            loop {
                match conn.reader.read_next().await {
                    Ok(Some((Resp::Cmd(cmd), offset))) => break Ok((self, cmd, offset)),
                    Ok(Some((resp, _))) => {
                        break Err((self, anyhow!("subscribed to commands, got: {resp:?}")))
                    }
                    Ok(None) => tokio::task::yield_now().await,
                    Err(e) => break Err((self, e)),
                }
            }
        })
    }

    pub async fn resp(&mut self, data: DataType) -> Result<()> {
        let Self::Recv(Connection { writer, .. }) = self else {
            bail!("trying to respond with {data:?} on a noop connection");
        };
        writer.write(&data).await?;
        writer.flush().await
    }
}

impl std::fmt::Display for ReplConnection {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "ReplSubscriber::Noop"),
            Self::Recv(_) => write!(f, "ReplSubscriber::RecvWrite"),
        }
    }
}

pub struct Replication {
    conn: Connection,
    repl: SocketAddr,
}

impl Replication {
    async fn new(leader: SocketAddr, replica: SocketAddr) -> Result<Self> {
        // NOTE: this could use some form of connection pooling
        let conn = TcpStream::connect(leader)
            .await
            .context("failed to establish connection")?;

        let (reader, writer) = conn.into_split();
        let reader = DataReader::new(reader);
        let writer = DataWriter::new(writer);

        let conn = Connection { reader, writer };

        Ok(Self {
            conn,
            repl: replica,
        })
    }

    pub async fn handshake(
        leader: SocketAddr,
        replica: SocketAddr,
    ) -> Result<(RDB, ReplConnection)> {
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

        let subscriber = ReplConnection::Recv(conn);

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
            // DataType::string(b"capa"),
            // DataType::string(b"eof"),
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

        println!("response to PSYNC {state}: {resp:?}");

        match resp {
            // TODO: don't forget about the state
            Resp::Cmd(Command::FullResync(_state)) => {
                let rdb = timeout(TIMEOUT, self.conn.reader.read_rdb())
                    .await
                    .with_context(|| format!("reading RDB file after PSYNC {state} timed out"))?
                    .with_context(|| format!("reading RDB file after PSYNC {state}"))?;
                Ok((self, rdb))
            }
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn request<const N: usize>(&mut self, args: [DataType; N]) -> Result<Resp> {
        let Connection { reader, writer } = &mut self.conn;

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
            Some((resp, _)) => Ok(resp),
            other => bail!("unexpected response {other:?}"),
        }
    }
}
