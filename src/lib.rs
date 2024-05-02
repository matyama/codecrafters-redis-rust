use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicPtr, Ordering::*};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use reader::RDBFileReader;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use repl::{ReplConnection, Replication};

pub(crate) use cmd::Command;
pub(crate) use config::Config;
pub(crate) use reader::DataReader;
pub(crate) use repl::{ReplId, ReplState, ReplicaSet, UNKNOWN_REPL_STATE};
pub(crate) use store::Store;
pub(crate) use writer::DataWriter;

use crate::writer::{DataSerializer, Serializer as _};

pub(crate) mod cmd;
pub(crate) mod config;
pub(crate) mod rdb;
pub(crate) mod reader;
pub(crate) mod repl;
pub(crate) mod store;
pub(crate) mod writer;

// pub(crate) const EOF: u8 = b'\xFF'; // 0xFF (e.g., RDB EOF op code)
pub(crate) const LF: u8 = b'\n'; // 10
pub(crate) const CRLF: &[u8] = b"\r\n"; // [13, 10]
pub(crate) const NULL: &[u8] = b"_\r\n";

pub(crate) const EMPTY: Bytes = Bytes::from_static(b"");
pub(crate) const ANY: Bytes = Bytes::from_static(b"*");
pub(crate) const UNKNOWN: Bytes = Bytes::from_static(b"?");

pub(crate) const PING: Bytes = Bytes::from_static(b"PING");
pub(crate) const PONG: Bytes = Bytes::from_static(b"PONG");
pub(crate) const ECHO: Bytes = Bytes::from_static(b"ECHO");
pub(crate) const CONFIG: Bytes = Bytes::from_static(b"CONFIG");
pub(crate) const KEYS: Bytes = Bytes::from_static(b"KEYS");
pub(crate) const GET: Bytes = Bytes::from_static(b"GET");
pub(crate) const SET: Bytes = Bytes::from_static(b"SET");
pub(crate) const INFO: Bytes = Bytes::from_static(b"INFO");
pub(crate) const REPLCONF: Bytes = Bytes::from_static(b"REPLCONF");
pub(crate) const PSYNC: Bytes = Bytes::from_static(b"PSYNC");
pub(crate) const FULLRESYNC: Bytes = Bytes::from_static(b"FULLRESYNC");
pub(crate) const WAIT: Bytes = Bytes::from_static(b"WAIT");

pub(crate) const OK: Bytes = Bytes::from_static(b"OK");
pub(crate) const GETACK: Bytes = Bytes::from_static(b"GETACK");
pub(crate) const ACK: Bytes = Bytes::from_static(b"ACK");

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

impl DataExt for rdb::String {
    fn to_uppercase(&self) -> Vec<u8> {
        use rdb::String::*;
        let mut s = match self {
            Str(s) => return s.to_uppercase(),
            Int8(i) => i.to_string(),
            Int16(i) => i.to_string(),
            Int32(i) => i.to_string(),
        };
        s.make_ascii_uppercase();
        s.into()
    }

    fn to_lowercase(&self) -> Vec<u8> {
        use rdb::String::*;
        let mut s = match self {
            Str(s) => return s.to_lowercase(),
            Int8(i) => i.to_string(),
            Int16(i) => i.to_string(),
            Int32(i) => i.to_string(),
        };
        s.make_ascii_lowercase();
        s.into()
    }
}

// NOTE: immutable with cheap Clone impl
#[derive(Debug, Clone)]
pub enum DataType {
    Null,
    NullBulkString,
    Boolean(bool),
    Integer(i64),
    SimpleString(rdb::String),
    SimpleError(rdb::String),
    BulkString(rdb::String),
    Array(Arc<[DataType]>),
    Map(Arc<[(DataType, DataType)]>),
}

impl DataType {
    pub(crate) fn err(e: impl Into<rdb::String>) -> Self {
        Self::SimpleError(e.into())
    }

    #[inline]
    pub(crate) fn str(s: impl Into<rdb::String>) -> Self {
        Self::SimpleString(s.into())
    }

    #[inline]
    pub(crate) fn string(s: impl Into<rdb::String>) -> Self {
        Self::BulkString(s.into())
    }

    #[inline]
    pub(crate) fn array<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        Self::Array(items.into_iter().collect())
    }

    #[inline]
    pub(crate) fn map<I>(items: I) -> Self
    where
        I: IntoIterator<Item = (Self, Self)>,
    {
        Self::Map(items.into_iter().collect())
    }

    #[inline]
    pub(crate) fn cmd<T, I>(args: I) -> Self
    where
        T: Into<rdb::String>,
        I: IntoIterator<Item = T>,
    {
        Self::array(args.into_iter().map(Self::string))
    }

    /// Returns a static reference to the 'REPLCONF GETACK *' command and its serialized size
    pub(crate) fn replconf_getack() -> &'static (Self, usize) {
        static REPLCONF_GETACK: OnceLock<(DataType, usize)> = OnceLock::new();
        REPLCONF_GETACK.get_or_init(|| {
            let data = Self::from(Command::Replconf(cmd::replconf::Conf::GetAck(ANY)));
            let size = DataSerializer::serialized_size(&data)
                .expect("'REPLCONF GETACK *' should be serializable");
            debug_assert!(size > 0, "'{data:?}' got serialized to 0B");
            (data, size)
        })
    }

    pub(crate) fn parse_int(self) -> Result<Self> {
        use rdb::String::*;
        match self {
            Self::Null => bail!("null cannot be converted to an integer"),
            Self::Boolean(b) => Ok(Self::Integer(b.into())),
            i @ Self::Integer(_) => Ok(i),
            Self::SimpleString(s) | Self::BulkString(s) => match s {
                Str(s) => {
                    let s = std::str::from_utf8(&s)
                        .with_context(|| format!("{s:?} is not a UTF-8 string"))?;
                    s.parse()
                        .map(Self::Integer)
                        .with_context(|| format!("'{s}' does not represent an integer"))
                }
                Int8(i) => Ok(Self::Integer(i as i64)),
                Int16(i) => Ok(Self::Integer(i as i64)),
                Int32(i) => Ok(Self::Integer(i as i64)),
            },
            Self::NullBulkString => bail!("null bulk string cannot be converted to an integer"),
            Self::SimpleError(_) => bail!("simple error cannot be converted to an integer"),
            Self::Array(_) => bail!("array cannot be converted to an integer"),
            Self::Map(_) => bail!("map cannot be converted to an integer"),
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

// TODO: deprecate in favor of RDB
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct RDBData(pub(crate) Bytes);

impl RDBData {
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

#[derive(Debug)]
#[repr(transparent)]
pub struct Leader(SocketAddr);

#[derive(Debug)]
pub(crate) enum Role {
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

#[derive(Debug)]
pub struct Instance {
    cfg: Config,
    role: Role,
    state: AtomicPtr<ReplState>,
    store: Store,
}

impl Instance {
    pub async fn new(cfg: Config) -> Result<(Self, ReplConnection)> {
        let store = if let Some(mut reader) = RDBFileReader::new(cfg.db_path()).await? {
            let (rdb, bytes_read) = reader.read().await?;

            println!(
                "read initial RDB (v{}, {bytes_read}B) {:?} // {:x?}",
                rdb.version, rdb.aux, rdb.checksum
            );

            Store::from(rdb)
        } else {
            Store::default()
        };

        let Some(leader) = cfg.replica_of else {
            // TODO: this is just an initial placeholder replication state
            let state = AtomicPtr::new(Box::into_raw(Box::new(ReplState {
                repl_id: ReplId::new(Bytes::from_static(
                    b"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
                ))
                .expect("valid REPL_ID"),
                repl_offset: 0,
            })));

            let instance = Self {
                cfg,
                role: Role::Leader(ReplicaSet::default()),
                state,
                store,
            };

            return Ok((instance, ReplConnection::default()));
        };

        let (state, _rdb, subscriber) = Replication::handshake(leader, cfg.addr)
            .await
            .with_context(|| format!("handshake with leader at {leader}"))?;

        // TODO: apply initial RDB

        let instance = Self {
            cfg,
            role: Role::Replica(Leader(leader)),
            state: AtomicPtr::new(Box::into_raw(Box::new(state))),
            store,
        };

        Ok((instance, subscriber))
    }

    #[inline]
    pub fn is_replica(&self) -> bool {
        matches!(self.role, Role::Replica(_))
    }

    #[inline]
    pub(crate) fn role(&self) -> &Role {
        &self.role
    }

    pub fn state(&self) -> ReplState {
        let state = self.state.load(Acquire);
        // NOTE: if the below is not correct, this could easily use some delayed reclamation scheme
        // SAFETY: the Acquire load above should synchronize to see all the Release stores from the
        // CAS in `shift_offset` - that is, it won't see any old values that would be dropped
        let state = unsafe { &*state };
        // NOTE: this is a cheap clone due to `Bytes::clone`
        state.clone()
    }

    /// Returns previous state and the new offset
    pub fn shift_offset(&self, offset: usize) -> (ReplState, isize) {
        if offset == 0 {
            let state = self.state();
            return (state.clone(), state.repl_offset);
        }
        let mut old = self.state.load(Relaxed);
        let new = Box::into_raw(Box::new(UNKNOWN_REPL_STATE));
        loop {
            {
                // SAFETY: always points to a valid allocation as long as self lives
                //  1. `self.state` is initialized to a valid non-null allocation
                //  2. The only way it changes is through the CAS below, which (if successful) sets
                //     the value for the next iteration to the `new` allocation above.
                //  3. If the CAS below fails, it's updated to some other valid allocation
                //  4. Its allocation is freed only in the success case which breaks the loop and
                //     we never give out references to it.
                let old = unsafe { &*old };
                // SAFETY: new never changes in these iterations and points to the allocation above
                let new = unsafe { &mut *new };
                new.repl_id = old.repl_id.clone();
                new.repl_offset = old.repl_offset.max(0) + offset as isize;
            }

            match self.state.compare_exchange_weak(old, new, AcqRel, Acquire) {
                Ok(old) => {
                    // SAFETY: old is always a valid allocation and there cannot be refs to it
                    //  1. It's been either created during instance initialization or
                    //  2. It's been created during some previous invocation of this method
                    let old_state = unsafe { Box::from_raw(old) };
                    let old_state = ReplState {
                        repl_id: old_state.repl_id.clone(),
                        repl_offset: old_state.repl_offset,
                    };
                    let new_offset = old_state.repl_offset.max(0) + offset as isize;
                    break (old_state, new_offset);
                }
                Err(new_old) => old = new_old,
            }
        }
    }

    pub async fn listen(&self) -> Result<TcpListener> {
        println!("{self}: binding TCP listener");
        TcpListener::bind(self.cfg.addr)
            .await
            .with_context(|| format!("{self}: failed to bind a TCP listener"))
    }

    pub async fn handle_connection(self: Arc<Self>, mut conn: TcpStream) -> Result<()> {
        let (reader, writer) = conn.split();

        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        while let Some((resp, bytes_read)) = reader.read_next().await? {
            match resp {
                Resp::Cmd(cmd @ Command::PSync(_)) => {
                    let Role::Leader(replicas) = &self.role else {
                        bail!("protocol violation: {cmd:?} is only supported by a leader");
                    };

                    println!("executing internal command: {cmd:?}");
                    let resp = cmd.exec(Arc::clone(&self)).await;

                    writer.write(&resp).await?;

                    let rdb = RDBData::empty();
                    writer.write_rdb(rdb).await?;
                    writer.flush().await?;

                    // assume that after PSYNC the replica will end up in the current state
                    return replicas.register(conn, self.state()).await;
                }
                Resp::Cmd(cmd) if cmd.is_write() => {
                    let repl_cmd = cmd.clone();

                    println!("executing write command {cmd:?}");
                    let resp = cmd.exec(Arc::clone(&self)).await;

                    writer.write(&resp).await?;
                    writer.flush().await?;

                    if let Role::Leader(replicas) = &self.role {
                        let (old_state, new_offset) = self.shift_offset(bytes_read);
                        println!("master offset: {} -> {new_offset}", old_state.repl_offset);
                        replicas.forward(repl_cmd).await;
                    }

                    println!("write command handled: {resp:?}");
                }
                Resp::Cmd(cmd) => {
                    println!("executing non-write command {cmd:?}");
                    let resp = cmd.exec(Arc::clone(&self)).await;
                    writer.write(&resp).await?;
                    writer.flush().await?;
                    println!("non-write command handled: {resp:?}");
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
                        println!("replica: executing command {cmd:?}");

                        let resp = cmd.exec(Arc::clone(&instance)).await;

                        if let Some(ack) = ack {
                            println!("replica: responding with {resp:?}");
                            if let Err(e) = ack.send(resp) {
                                eprintln!("replica: response dropped due to {e:?}");
                            }
                        }
                    }
                });

                ReplHandle(tx)
            }
        }
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let state = self.state.swap(std::ptr::null_mut(), AcqRel);
        if !std::ptr::eq(state, std::ptr::null_mut()) {
            // SAFETY: always points to a valid allocation of a state and never gives out refs
            let _ = unsafe { Box::from_raw(state) };
        }
    }
}

impl std::fmt::Display for Instance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} @ {} ({})", self.role, self.cfg.addr, self.state())
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
