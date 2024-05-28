use std::fmt::Debug;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicPtr, Ordering::*};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task;

use rdb::RDB;
use repl::{ReplConnection, Replication};

pub(crate) use cmd::Command;
pub(crate) use config::Config;
pub(crate) use data::{Args, DataType};
pub(crate) use io::{DataReader, DataWriter, RDBFileReader};
pub(crate) use repl::{ReplId, ReplState, ReplicaSet, UNKNOWN_REPL_STATE};
pub(crate) use store::Store;

pub(crate) mod cmd;
pub(crate) mod config;
pub(crate) mod data;
pub(crate) mod io;
pub(crate) mod rdb;
pub(crate) mod repl;
pub(crate) mod store;
pub(crate) mod stream;

pub(crate) mod resp {
    pub const PING: &[u8] = b"PING";
    pub const ECHO: &[u8] = b"ECHO";
    pub const CONFIG: &[u8] = b"CONFIG";
    pub const KEYS: &[u8] = b"KEYS";
    pub const TYPE: &[u8] = b"TYPE";
    pub const GET: &[u8] = b"GET";
    pub const SET: &[u8] = b"SET";
    pub const XADD: &[u8] = b"XADD";
    pub const XRANGE: &[u8] = b"XRANGE";
    pub const XREAD: &[u8] = b"XREAD";
    pub const XLEN: &[u8] = b"XLEN";
    pub const INFO: &[u8] = b"INFO";
    pub const REPLCONF: &[u8] = b"REPLCONF";
    pub const PSYNC: &[u8] = b"PSYNC";
    pub const FULLRESYNC: &[u8] = b"FULLRESYNC";
    pub const WAIT: &[u8] = b"WAIT";

    pub const OK: &[u8] = b"OK";
    pub const PONG: &[u8] = b"PONG";
}

pub(crate) const TIMEOUT: Duration = Duration::from_secs(5);

// NOTE: this is based on the codecrafters examples
pub const PROTOCOL: Protocol = Protocol::RESP2;

pub enum Protocol {
    RESP2,
    RESP3,
}

// SAFETY: version is clearly non-zero
pub const VERSION: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(11) };
pub const REDIS_VER: Bytes = Bytes::from_static(b"7.2.0");
//pub const REDIS_VER: Bytes = Bytes::from_static(b"7.2.4");

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("WRONGTYPE Operation against a key holding the wrong kind of value")]
    WrongType,

    #[error("ERR unknown command '{cmd}', with args beginning with: {args}")]
    UnknownCommand { cmd: String, args: String },

    #[error("ERR unknown subcommand '{cmd}'. Try {help}.")]
    UnknownSubCommand { cmd: String, help: &'static str },

    #[error("ERR Unbalanced '{cmd}' list of streams: {msg}.")]
    UnbalancedStreams {
        cmd: &'static str,
        msg: &'static str,
    },

    #[error("ERR wrong number of arguments for '{0}' command")]
    WrongNumArgs(&'static str),

    #[error("ERR {0} is not an integer or out of range")]
    NotInt(&'static str),

    #[error("ERR {0} is negative")]
    NegInt(&'static str),

    #[error("ERR syntax error")]
    Syntax,

    #[error("ERR {0}")]
    Err(String),

    #[error("ERR {0}")]
    Any(#[from] anyhow::Error),
}

impl Error {
    pub(crate) const VAL_NOT_INT: Error = Error::NotInt("value");
    pub(crate) const VAL_NEG_INT: Error = Error::NegInt("value");

    #[inline]
    pub fn err(e: impl ToString) -> Self {
        Self::Err(e.to_string())
    }

    #[inline]
    pub fn unkown_cmd(cmd: &DataType, args: &[DataType]) -> Self {
        Self::UnknownCommand {
            cmd: cmd.to_string(),
            args: Args(args).to_string(),
        }
    }

    #[inline]
    pub fn unkown_subcmd(cmd: &DataType, help: &'static str) -> Self {
        Self::UnknownSubCommand {
            cmd: cmd.to_string(),
            help,
        }
    }
}

impl From<Error> for rdb::String {
    #[inline]
    fn from(error: Error) -> Self {
        Self::from(error.to_string())
    }
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
    store: Arc<Store>,
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
                )),
                repl_offset: 0,
            })));

            let instance = Self {
                cfg,
                role: Role::Leader(ReplicaSet::default()),
                state,
                store: Arc::new(store),
            };

            return Ok((instance, ReplConnection::default()));
        };

        let (state, rdb, subscriber) = Replication::handshake(leader, cfg.addr)
            .await
            .with_context(|| format!("handshake with leader at {leader}"))?;

        // TODO: persist the initial RDB (i.e., override local dump file)
        let store = Store::from_rdb(rdb).await.context("apply initial RDB")?;

        let instance = Self {
            cfg,
            role: Role::Replica(Leader(leader)),
            state: AtomicPtr::new(Box::into_raw(Box::new(state))),
            store: Arc::new(store),
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
                new.repl_id.clone_from(&old.repl_id);
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

    // TODO: implement actual process fork to background save (BGSAVE) and serve the resulting RDB
    async fn fork(&self) -> Result<RDB> {
        Ok(self.store.snapshot(|| self.state()).await)
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

                    let rdb = self.fork().await?;
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
                        if resp.is_replicable() {
                            let (old_state, new_offset) = self.shift_offset(bytes_read);
                            println!("master offset: {} -> {new_offset}", old_state.repl_offset);
                            replicas.forward(repl_cmd).await;
                        }
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
                // XXX: || resp.is_null()
                Resp::Data(resp) if resp.is_err() => {
                    writer.write(&resp).await?;
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
