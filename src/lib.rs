use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{collections::VecDeque, fmt::Debug};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio::task;

use repl::{ReplConnection, Replication};

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

// pub(crate) const EOF: u8 = b'\xFF'; // 0xFF (e.g., RDB EOF op code)
pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
pub(crate) const NULL: &[u8] = b"_\r\n";

pub(crate) const EMPTY: Bytes = Bytes::from_static(b"");

pub(crate) const PING: Bytes = Bytes::from_static(b"PING");
pub(crate) const PONG: Bytes = Bytes::from_static(b"PONG");
pub(crate) const ECHO: Bytes = Bytes::from_static(b"ECHO");
pub(crate) const GET: Bytes = Bytes::from_static(b"GET");
pub(crate) const SET: Bytes = Bytes::from_static(b"SET");
pub(crate) const INFO: Bytes = Bytes::from_static(b"INFO");
pub(crate) const REPLCONF: Bytes = Bytes::from_static(b"REPLCONF");
pub(crate) const PSYNC: Bytes = Bytes::from_static(b"PSYNC");
pub(crate) const FULLRESYNC: Bytes = Bytes::from_static(b"FULLRESYNC");
pub(crate) const WAIT: Bytes = Bytes::from_static(b"WAIT");

pub(crate) const OK: Bytes = Bytes::from_static(b"OK");
pub(crate) const ACK: Bytes = Bytes::from_static(b"ACK");

pub(crate) const DEFAULT_REPL_ID: Bytes = Bytes::from_static(b"?");

// TODO: generalize to non-unix systems via cfg target_os
const EMPTY_RDB: Bytes = Bytes::from_static(include_bytes!("../data/empty_rdb.dat"));

pub(crate) const TIMEOUT: Duration = Duration::from_secs(5);

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

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RDB(pub(crate) Bytes);

impl RDB {
    #[inline]
    pub fn empty() -> Self {
        Self(EMPTY_RDB)
    }

    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct Offset(usize);

impl From<usize> for Offset {
    #[inline]
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl From<Offset> for usize {
    #[inline]
    fn from(Offset(value): Offset) -> Self {
        value
    }
}

impl<T: Into<Self>> std::ops::Add<T> for Offset {
    type Output = Self;

    #[inline]
    fn add(self, rhs: T) -> Self::Output {
        Self(self.0 + rhs.into().0)
    }
}

impl<T: Into<Self>> std::ops::AddAssign<T> for Offset {
    #[inline]
    fn add_assign(&mut self, rhs: T) {
        self.0 += rhs.into().0;
    }
}

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

#[derive(Clone, Debug)]
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
#[repr(transparent)]
pub struct Leader(SocketAddr);

#[derive(Debug)]
pub struct Replica {
    addr: SocketAddr,
    conn: TcpStream,
}

impl Replica {
    pub(crate) async fn forward(&mut self, cmd: &DataType) -> Result<()> {
        let (_, writer) = self.conn.split();
        let mut writer = DataWriter::new(writer);

        writer
            .write(cmd)
            .await
            .with_context(|| format!("failed to forward a write command to {}", self.addr))?;

        writer
            .flush()
            .await
            .with_context(|| format!("failed to flush replication to {}", self.addr))?;

        // NOTE: here we implement an async replication, so no sync waiting for a response

        Ok(())
    }
}

#[derive(Debug, Default)]
#[repr(transparent)]
pub struct ReplicaSet(RwLock<HashMap<SocketAddr, Arc<Mutex<Replica>>>>);

impl ReplicaSet {
    pub(crate) async fn register(&self, conn: TcpStream) -> Result<()> {
        let Ok(addr) = conn.peer_addr() else {
            bail!("cannot obtain replica address");
        };
        println!("registering new replica at {addr}");
        let mut replicas = self.0.write().await;
        let replica = Arc::new(Mutex::new(Replica { addr, conn }));
        replicas.insert(addr, replica);
        Ok(())
    }

    pub(crate) async fn forward(&self, cmd: Command) {
        if !cmd.is_write() {
            println!("WARN: only write commands can be forwarded to replicas, skipping {cmd:?}");
            return;
        };

        let cmd: Arc<DataType> = Arc::new(cmd.into());

        let replicas = self.0.read().await;

        println!(
            "forwarding {cmd:?} to current replica set {:?}",
            replicas.keys()
        );

        // NOTE: spawn should be fine (i.e., without ordering issues), since there's a replica lock
        for (&addr, replica) in replicas.iter() {
            let cmd = Arc::clone(&cmd);
            let replica = Arc::clone(replica);
            tokio::spawn(async move {
                let mut replica = replica.lock().await;
                let result = replica.forward(&cmd).await;
                drop(replica);

                // NOTE: when async replication fails, it's the failed replicas' job to re-sync
                match result.with_context(|| format!("replicating to {addr}")) {
                    Ok(_) => println!("update sent to replica {addr}"),
                    Err(e) => eprintln!("write command replication failed with {e:?}"),
                }
            });
        }
    }
}

#[derive(Debug)]
enum Role {
    Leader(ReplicaSet),
    Replica(Leader),
}

impl std::fmt::Display for Role {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Leader(_) => write!(f, "leader"),
            Self::Replica(Leader(addr)) => write!(f, "replica of {addr}"),
        }
    }
}

// XXX: might need to store the both the state and store under a common mutex
#[derive(Debug)]
pub struct Instance {
    addr: SocketAddr,
    role: Role,
    state: ReplState,
    store: Store,
    offset: AtomicUsize,
}

impl Instance {
    pub async fn new(cfg: Config) -> Result<(Self, ReplConnection)> {
        // TODO: this is just an initial placeholder replication state
        let state = ReplState {
            repl_id: ReplId::new(Bytes::from_static(
                b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            ))
            .expect("valid REPL_ID"),
            repl_offset: 0,
        };

        let store = Store::default();

        let Some(leader) = cfg.replica_of else {
            let instance = Self {
                addr: cfg.addr,
                role: Role::Leader(ReplicaSet::default()),
                state,
                store,
                offset: AtomicUsize::new(0),
            };
            return Ok((instance, ReplConnection::default()));
        };

        // TODO: apply initial RDB
        let (_rdb, subscriber) = Replication::handshake(leader, cfg.addr)
            .await
            .with_context(|| format!("handshake with leader at {leader}"))?;

        let instance = Self {
            addr: cfg.addr,
            role: Role::Replica(Leader(leader)),
            state,
            store,
            offset: AtomicUsize::new(0),
        };

        Ok((instance, subscriber))
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        matches!(self.role, Role::Replica(_))
    }

    /// Returns previous [`Offset`]
    pub fn shift_offset(&self, Offset(offset): Offset) -> Offset {
        let mut old = self.offset.load(Ordering::Relaxed);
        loop {
            let new = old + offset;
            match self
                .offset
                .compare_exchange_weak(old, new, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(old) => break old.into(),
                Err(new_old) => old = new_old,
            }
        }
    }

    pub async fn listen(&self) -> Result<TcpListener> {
        println!("{self}: binding TCP listener");
        TcpListener::bind(self.addr)
            .await
            .with_context(|| format!("{self}: failed to bind a TCP listener"))
    }

    pub async fn handle_connection(self: Arc<Self>, mut conn: TcpStream) -> Result<()> {
        let (reader, writer) = conn.split();

        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        while let Some((resp, _offset)) = reader.read_next().await? {
            match resp {
                Resp::Cmd(cmd @ Command::PSync(_)) => {
                    let Role::Leader(replicas) = &self.role else {
                        bail!("protocol violation: {cmd:?} is only supported by a leader");
                    };

                    println!("executing {cmd:?}");
                    let resp = cmd.exec(Arc::clone(&self)).await;
                    writer.write(&resp).await?;

                    let rdb = RDB::empty();
                    writer.write_rdb(rdb).await?;
                    writer.flush().await?;

                    return replicas.register(conn).await;
                }
                Resp::Cmd(cmd) if cmd.is_write() => {
                    // TODO: skip this clone (should be cheap tho) on replicas
                    let repl_cmd = cmd.clone();

                    println!("executing write command {cmd:?}");
                    let resp = cmd.exec(Arc::clone(&self)).await;
                    writer.write(&resp).await?;

                    println!("flushing and closing connection");
                    writer.flush().await?;

                    if let Role::Leader(replicas) = &self.role {
                        replicas.forward(repl_cmd).await;
                    }
                }
                Resp::Cmd(cmd) => {
                    println!("executing non-write command {cmd:?}");
                    let resp = cmd.exec(Arc::clone(&self)).await;
                    writer.write(&resp).await?;

                    println!("flushing and closing connection");
                    writer.flush().await?;
                }
                Resp::Data(resp) => {
                    bail!("protocol violation: expected a command, got {resp:?} instead")
                }
            };
        }

        Ok(())
    }

    pub async fn spawn_replicator(self: Arc<Self>) -> ReplHandle {
        match self.role {
            Role::Leader(_) => {
                let (dummy, _) = mpsc::channel::<ReplCommand>(1);
                ReplHandle(dummy)
            }

            Role::Replica(_) => {
                // NOTE: buffer up some writes and unblock new read connections
                let (tx, mut rx) = mpsc::channel::<ReplCommand>(64);
                let instance = Arc::clone(&self);

                task::spawn(async move {
                    while let Some(ReplCommand { cmd, ack }) = rx.recv().await {
                        println!("replication: executing command {cmd:?}");

                        let resp = cmd.exec(Arc::clone(&instance)).await;
                        println!("command replicated: {resp:?}");

                        if let Some(ack) = ack {
                            println!("replying with replication response: {resp:?}");
                            if let Err(e) = ack.send(resp) {
                                eprintln!("replication: response dropped due to {e:?}");
                            }
                        }
                    }
                });

                ReplHandle(tx)
            }
        }
    }
}

impl std::fmt::Display for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} @ {} ({})", self.role, self.addr, self.state)
    }
}

#[derive(Debug)]
struct ReplCommand {
    cmd: Command,
    ack: Option<oneshot::Sender<DataType>>,
}

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ReplHandle(mpsc::Sender<ReplCommand>);

impl ReplHandle {
    pub async fn exec(&self, cmd: Command) -> Result<Option<DataType>> {
        if !cmd.is_ack() {
            let cmd = ReplCommand { cmd, ack: None };
            self.0.send(cmd).await.context("replicator disconnected")?;
            return Ok(None);
        }

        let (ack, result) = oneshot::channel();

        let cmd = ReplCommand {
            cmd,
            ack: Some(ack),
        };

        self.0.send(cmd).await.context("replicator disconnected")?;

        result.await.context("replicator disconnected").map(Some)
    }
}
