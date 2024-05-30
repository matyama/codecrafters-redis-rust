use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicIsize, AtomicUsize, Ordering::*};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;

use crate::cmd::replconf;
use crate::data::{DataExt as _, DataType, ParseInt};
use crate::rdb::{self, RDBData};
use crate::{Command, DataReader, DataWriter, Error, Resp, TIMEOUT};

const UNKNOWN: Bytes = Bytes::from_static(b"?");

pub(crate) const UNKNOWN_REPL_STATE: ReplState = ReplState {
    repl_id: None,
    repl_offset: -1,
};

#[derive(Clone, Debug, PartialEq)]
#[repr(transparent)]
pub struct ReplId(Bytes);

impl ReplId {
    // NOTE: Redis does not seem to actually validate UTF-8 (alphanumeric) and the 40-character len
    #[inline]
    pub fn new(repl_id: Bytes) -> Option<ReplId> {
        match repl_id.as_ref() {
            b"?" => None,
            _ => Some(Self(repl_id)),
        }
    }
}

impl From<ReplId> for Bytes {
    #[inline]
    fn from(ReplId(id): ReplId) -> Self {
        id
    }
}

impl From<ReplId> for rdb::String {
    #[inline]
    fn from(ReplId(id): ReplId) -> Self {
        Self::from(id)
    }
}

impl Default for ReplId {
    #[inline]
    fn default() -> Self {
        Self(UNKNOWN)
    }
}

impl Display for ReplId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match String::from_utf8_lossy(&self.0) {
            Cow::Borrowed(repl_id) => f.write_str(repl_id),
            Cow::Owned(repl_id) => write!(f, "{repl_id}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplState {
    /// Pseudo random alphanumeric string of 40 characters
    pub repl_id: Option<ReplId>,
    // TODO: this should probably be an i128 (blocked by AtomicUsize/AtomicI128)
    pub repl_offset: isize,
}

impl<B> TryFrom<(Bytes, B)> for ReplState
where
    B: ParseInt + Debug,
{
    type Error = Error;

    #[inline]
    fn try_from((repl_id, repl_offset): (Bytes, B)) -> Result<Self, Self::Error> {
        Ok(ReplState {
            repl_id: ReplId::new(repl_id),
            repl_offset: repl_offset.parse()?,
        })
    }
}

impl TryFrom<(rdb::String, rdb::String)> for ReplState {
    type Error = Error;

    fn try_from((repl_id, repl_offset): (rdb::String, rdb::String)) -> Result<Self, Self::Error> {
        use rdb::String::*;

        let repl_id = repl_id.bytes().context("invalid REPL_ID")?;

        let repl_offset = match repl_offset {
            Str(repl_offset) => return (repl_id, repl_offset).try_into(),
            Int8(offset) => offset as isize,
            Int16(offset) => offset as isize,
            Int32(offset) => offset as isize,
        };

        Ok(ReplState {
            repl_id: ReplId::new(repl_id),
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

impl Display for ReplState {
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

// TODO: support reconnect
pub struct Connection {
    peer: SocketAddr,
    reader: DataReader<OwnedReadHalf>,
    writer: DataWriter<OwnedWriteHalf>,
}

impl Connection {
    pub fn new(conn: TcpStream) -> Result<Self> {
        let peer = conn.peer_addr().context("replication connection")?;

        let (reader, writer) = conn.into_split();
        let reader = DataReader::new(reader);
        let writer = DataWriter::new(writer);

        Ok(Self {
            peer,
            reader,
            writer,
        })
    }

    #[inline]
    pub fn peer(&self) -> SocketAddr {
        self.peer
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

        match resp {
            Some((resp, bytes_resp)) => Ok((resp, bytes_req, bytes_resp)),
            other => bail!("unexpected response {other:?}"),
        }
    }

    pub async fn ping(&mut self) -> Result<(usize, usize)> {
        let ping = DataType::from(Command::Ping(None));

        let resp = self
            .request(&ping, TIMEOUT)
            .await
            .context("init replication")?;

        match resp {
            (Resp::Data(DataType::SimpleString(s)), bytes_req, bytes_resp) if s.matches("PONG") => {
                Ok((bytes_req, bytes_resp))
            }
            other => bail!("unexpected response {other:?}"),
        }
    }
}

impl Debug for Connection {
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

impl Display for ReplConnection {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Noop => write!(f, "ReplSubscriber::Noop"),
            Self::Recv(_) => write!(f, "ReplSubscriber::RecvWrite"),
        }
    }
}

#[derive(Debug)]
pub struct Replica {
    conn: Mutex<Connection>,
    #[allow(dead_code)]
    repl_id: ReplId,
    repl_offset: AtomicIsize,
    // TODO: AtomicPtr<ReplicaState { alive: bool, last_ack: Instant }>
    // TODO: implement replica healthcheck (i.e, send periodic one-way PINGs)
    #[allow(dead_code)]
    alive: AtomicBool,
}

impl Replica {
    #[inline]
    pub fn new(
        conn: Connection,
        ReplState {
            repl_id,
            repl_offset,
        }: ReplState,
    ) -> Self {
        Self {
            conn: Mutex::new(conn),
            repl_id: repl_id.unwrap_or_default(),
            repl_offset: AtomicIsize::new(repl_offset),
            alive: AtomicBool::new(true),
        }
    }
}

#[derive(Debug, Default)]
pub struct ReplicaSet {
    refs: RwLock<HashMap<SocketAddr, Arc<Replica>>>,
    db: AtomicUsize,
}

impl ReplicaSet {
    pub(crate) async fn register(&self, conn: TcpStream, state: ReplState) -> Result<()> {
        let conn = Connection::new(conn)?;
        let addr = conn.peer();

        let replica = Arc::new(Replica::new(conn, state));

        {
            let mut replicas = self.refs.write().await;
            replicas.insert(addr, Arc::clone(&replica));
        }

        {
            let replicas = self.refs.read().await;
            println!(
                "{addr} added to the current replica set: {:?}",
                replicas.keys()
            );
        }

        // TODO: register with a replica healthcheck service (i.e., periodic one-way PINGs)

        Ok(())
    }

    #[inline]
    pub(crate) fn db(&self) -> usize {
        self.db.load(Acquire)
    }

    pub(crate) async fn select(&self, db: usize) {
        self.forward(Command::Select(db)).await;
        self.db.store(db, Release);
    }

    pub(crate) async fn forward(&self, cmd: impl Into<Resp> + Debug) {
        println!("forwarding {cmd:?}");

        let cmd = match cmd.into() {
            Resp::Cmd(cmd) => DataType::from(cmd),
            Resp::Data(cmd) => cmd,
        };

        let replicas = self.refs.read().await;

        // NOTE: spawn should be fine (i.e., without ordering issues), since there's a replica lock
        for (&addr, replica) in replicas.iter() {
            let cmd = cmd.clone();
            let replica = Arc::clone(replica);
            tokio::spawn(async move {
                let mut replica = replica.conn.lock().await;
                if let Err(e) = replica.forward(&cmd).await {
                    // NOTE: when async replication fails, it's the failed replicas' job to re-sync
                    eprintln!("replica {addr}: write command replication failed with {e:?}");
                }
            });
        }
    }

    pub(crate) async fn wait(
        &self,
        num_replicas: usize,
        timeout: Duration,
        state: ReplState,
    ) -> Result<(usize, usize)> {
        let replicas = self.refs.read().await;
        let num_registered = replicas.len();

        // fast path: if all replicas are sufficiently up to date, then skip sending GETACK
        let num_acks: usize = replicas
            .values()
            .map(|replica| state.repl_offset <= replica.repl_offset.load(Acquire))
            .map(usize::from)
            .sum();

        if num_acks >= num_replicas.min(num_registered) {
            return Ok((num_registered, 0));
        }

        // slow path: send GETACK to all replicas with given timeout
        let (getack, req_offset) = DataType::replconf_getack();

        let state = Arc::new(state);
        let num_acks = Arc::new(AtomicUsize::new(0));

        let mut tasks = JoinSet::new();

        for (addr, replica) in replicas.iter().map(|(&a, r)| (a, Arc::clone(r))) {
            let state = Arc::clone(&state);
            let num_acks = Arc::clone(&num_acks);

            tasks.spawn(async move {
                // send REPLCONF GETACK * to the replica
                let mut conn = replica.conn.lock().await;

                // NOTE: use default timeout for per request as there's an overall wait timeout
                let resp = conn.request(getack, Duration::ZERO).await;

                // NOTE: ok response witnesses that the request went through, so update offset
                match resp.with_context(|| format!("replica {addr}")) {
                    Ok((
                        Resp::Cmd(Command::Replconf(replconf::Conf::Ack(repl_offset))),
                        req_offset,
                        ..,
                    )) => {
                        let new_repl_offset = repl_offset + req_offset as isize;
                        let old_repl_offset = replica.repl_offset.swap(new_repl_offset, AcqRel);
                        println!("replica {addr} offset: {old_repl_offset} -> {new_repl_offset}");

                        // hold the lock up until the offset is updated
                        drop(conn);

                        if state.repl_offset <= repl_offset {
                            num_acks.fetch_add(1, AcqRel);
                        }

                        Ok(())
                    }

                    Ok((other, ..)) => {
                        bail!("replica {addr}: expected 'REPLCONF ACK _' response, got {other:?}")
                    }

                    Err(e) => Err(e).context("failed to get ack"),
                }
            });
        }

        drop(replicas);

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                () = &mut sleep, if !timeout.is_zero() => break,
                ack = tasks.join_next() => match ack {
                    Some(Ok(Ok(_))) => tokio::task::yield_now().await,
                    Some(Ok(Err(e))) => eprintln!("replica failed to ack during wait: {e:?}"),
                    Some(Err(e)) => eprintln!("task to get ack from replica panicked: {e:?}"),
                    None => break,
                },
            }
        }

        let num_acks = num_acks.load(Acquire);

        Ok((num_acks, *req_offset))
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
            .and_then(Connection::new)?;

        Ok(Self {
            conn,
            repl: replica,
        })
    }

    pub async fn handshake(
        leader: SocketAddr,
        replica: SocketAddr,
    ) -> Result<(ReplState, RDBData, ReplConnection)> {
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
        let replconf = Command::Replconf(replconf::Conf::ListeningPort(self.repl.port()));
        let replconf = DataType::from(replconf);

        let (resp, ..) = self
            .conn
            .request(&replconf, TIMEOUT)
            .await
            .with_context(|| format!("{replconf:?}"))?;

        match resp {
            Resp::Data(DataType::SimpleString(s)) if s.matches("OK") => {}
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
            Resp::Data(DataType::SimpleString(s)) if s.matches("OK") => Ok(self),
            other => bail!("unexpected response {other:?}"),
        }
    }

    async fn sync(mut self) -> Result<(Self, ReplState, RDBData)> {
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
