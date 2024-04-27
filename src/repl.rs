use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

use crate::cmd::replconf;
use crate::{Command, DataExt as _, DataReader, DataType, DataWriter, Resp, RDB, TIMEOUT, UNKNOWN};

pub(crate) const UNKNOWN_REPL_STATE: ReplState = ReplState {
    repl_id: None,
    repl_offset: -1,
};

#[derive(Clone, Debug, PartialEq)]
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
        Self(UNKNOWN)
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

#[derive(Clone, Debug)]
pub struct ReplState {
    /// Pseudo random alphanumeric string of 40 characters
    pub repl_id: Option<ReplId>,
    // TODO: this should probably be an i128
    pub repl_offset: isize,
}

impl<B> TryFrom<(Bytes, B)> for ReplState
where
    B: std::ops::Deref<Target = [u8]> + std::fmt::Debug,
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
            self.repl_offset,
        )
    }
}

// NOTE: it's actually necessary not to shutdown the write part to keep the connection alive
// TODO: support reconnect
pub struct Connection {
    peer: SocketAddr,
    reader: DataReader<OwnedReadHalf>,
    writer: DataWriter<OwnedWriteHalf>,
    state: ReplState,
}

impl Connection {
    pub fn new(conn: TcpStream, state: Option<ReplState>) -> Result<Self> {
        let peer = conn.peer_addr().context("replication connection")?;

        let (reader, writer) = conn.into_split();
        let reader = DataReader::new(reader);
        let writer = DataWriter::new(writer);

        Ok(Self {
            peer,
            reader,
            writer,
            state: state.unwrap_or_default(),
        })
    }

    #[inline]
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    #[inline]
    pub fn state(&self) -> &ReplState {
        &self.state
    }

    #[inline]
    pub fn set_offset(&mut self, offset: isize) {
        self.state.repl_offset = offset;
    }

    pub async fn forward(&mut self, cmd: &DataType) -> Result<usize> {
        let bytes_req = self
            .writer
            .write(cmd)
            .await
            .with_context(|| format!("failed to forward command {cmd:?}"))?;

        self.writer
            .flush()
            .await
            .context("failed to flush replication")?;

        // NOTE: Here we implement an async replication, so no sync waiting for a response.
        // This also means that we shouldn't shift the offset as there's no proof of delivery.

        Ok(bytes_req)
    }

    /// Returns response object together with the number of request and response bytes written/read
    pub async fn request(
        &mut self,
        cmd: &DataType,
        timeout: Duration,
    ) -> Result<(Resp, usize, usize)> {
        let bytes_req = self
            .writer
            .write(cmd)
            .await
            .context("failed to write request")?;

        self.writer
            .flush()
            .await
            .context("failed to flush request")?;

        // if timeout is zero, then await forever
        let resp = if timeout.is_zero() {
            self.reader.read_next().await.context("request failed")?
        } else {
            tokio::time::timeout(timeout, self.reader.read_next())
                .await
                .context("request timed out")?
                .context("request failed")?
        };

        // NOTE: contrary to forward, here the response witnesses that the request went through
        self.state.repl_offset += bytes_req as isize;

        match resp {
            Some((resp, bytes_resp)) => Ok((resp, bytes_req, bytes_resp)),
            other => bail!("unexpected response {other:?}"),
        }
    }

    pub async fn ping(&mut self) -> Result<(usize, usize)> {
        let pong = |s| String::from_utf8_lossy(s).to_uppercase() == "PONG";
        let ping = DataType::from(Command::Ping(None));

        let resp = self
            .request(&ping, TIMEOUT)
            .await
            .context("init replication")?;

        match resp {
            (Resp::Data(DataType::SimpleString(s)), bytes_req, bytes_resp) if pong(&s) => {
                Ok((bytes_req, bytes_resp))
            }
            other => bail!("unexpected response {other:?}"),
        }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("peer", &self.peer)
            .field("reader", &"<DataReader>")
            .field("writer", &"<DataWriter>")
            .finish()
    }
}

pub type RecvResult<T> = Result<(T, Command, usize), (T, anyhow::Error)>;

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
                    Ok(Some((Resp::Cmd(cmd), bytes_read))) => break Ok((self, cmd, bytes_read)),
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
        // println!("replication connection: writing {data:?}");
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
            .with_context(|| format!("replication connection: {leader} - {replica}"))
            .and_then(|conn| Connection::new(conn, None))?;

        Ok(Self {
            conn,
            repl: replica,
        })
    }

    pub async fn handshake(
        leader: SocketAddr,
        replica: SocketAddr,
    ) -> Result<(ReplState, RDB, ReplConnection)> {
        let (Replication { conn, .. }, state, rdb) = Self::new(leader, replica)
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

        Ok((state, rdb, subscriber))
    }

    async fn ping(mut self) -> Result<Self> {
        self.conn.ping().await.map(move |_| self)
    }

    async fn conf(mut self) -> Result<Self> {
        // TODO: replace with an optimized Matcher<'_> that uses Cow internally and is_lowercase
        let ok = |s: &Bytes| s.to_uppercase() == b"OK";

        let replconf = Command::Replconf(replconf::Conf::ListeningPort(self.repl.port()));
        let replconf = DataType::from(replconf);

        let (resp, ..) = self
            .conn
            .request(&replconf, TIMEOUT)
            .await
            .with_context(|| format!("{replconf:?}"))?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if ok(&s) => {}
            other => bail!("unexpected response {other:?}"),
        }

        let capabilities = [
            //Bytes::from_static(b"eof"),
            Bytes::from_static(b"psync2"),
        ];

        let replconf = Command::Replconf(replconf::Conf::Capabilities(capabilities.into()));
        let replconf = DataType::from(replconf);

        let (resp, ..) = self
            .conn
            .request(&replconf, TIMEOUT)
            .await
            .with_context(|| format!("{replconf:?}"))?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if ok(&s) => Ok(self),
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn sync(mut self) -> Result<(Self, ReplState, RDB)> {
        let state = ReplState::default();

        let psync = DataType::from(Command::PSync(state));

        let (resp, ..) = self
            .conn
            .request(&psync, TIMEOUT)
            .await
            .with_context(|| format!("{psync:?}"))?;

        println!("response to {psync:?}: {resp:?}");

        match resp {
            Resp::Cmd(Command::FullResync(state)) => {
                let rdb = tokio::time::timeout(TIMEOUT, self.conn.reader.read_rdb())
                    .await
                    .with_context(|| format!("reading RDB file after {psync:?} timed out"))?
                    .with_context(|| format!("reading RDB file after {psync:?}"))?;
                Ok((self, state, rdb))
            }
            other => bail!("unexpected response {other:?}"),
        }
    }
}
