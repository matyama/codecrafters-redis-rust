use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use anyhow::Context as _;
use bytes::{Bytes, BytesMut};

use crate::data::{DataType, Keys};
use crate::repl::ReplState;
use crate::store::{LockType, StoreHandle};
use crate::{rdb, resp, stream, Client, Error};
use crate::{Instance, Protocol, Role, PROTOCOL};

use replconf::{ACK, GETACK};

pub mod config;
pub mod info;
pub mod ping;
pub mod replconf;
pub mod select;
pub mod set;
pub mod sync;
pub mod wait;
pub mod xadd;
pub mod xrange;
pub mod xread;

const NULL: DataType = match PROTOCOL {
    Protocol::RESP2 => DataType::NullBulkString,
    Protocol::RESP3 => DataType::Null,
};

pub(crate) const DBSIZE: Bytes = Bytes::from_static(resp::DBSIZE);
pub(crate) const SELECT: Bytes = Bytes::from_static(resp::SELECT);
pub(crate) const PING: Bytes = Bytes::from_static(resp::PING);
pub(crate) const ECHO: Bytes = Bytes::from_static(resp::ECHO);
pub(crate) const CONFIG: Bytes = Bytes::from_static(resp::CONFIG);
pub(crate) const KEYS: Bytes = Bytes::from_static(resp::KEYS);
pub(crate) const TYPE: Bytes = Bytes::from_static(resp::TYPE);
pub(crate) const GET: Bytes = Bytes::from_static(resp::GET);
pub(crate) const SET: Bytes = Bytes::from_static(resp::SET);
pub(crate) const INCR: Bytes = Bytes::from_static(resp::INCR);
pub(crate) const MULTI: Bytes = Bytes::from_static(resp::MULTI);
pub(crate) const EXEC: Bytes = Bytes::from_static(resp::EXEC);
pub(crate) const DISCARD: Bytes = Bytes::from_static(resp::DISCARD);
pub(crate) const XADD: Bytes = Bytes::from_static(resp::XADD);
pub(crate) const XRANGE: Bytes = Bytes::from_static(resp::XRANGE);
pub(crate) const XREAD: Bytes = Bytes::from_static(resp::XREAD);
pub(crate) const XLEN: Bytes = Bytes::from_static(resp::XLEN);
pub(crate) const INFO: Bytes = Bytes::from_static(resp::INFO);
pub(crate) const REPLCONF: Bytes = Bytes::from_static(resp::REPLCONF);
pub(crate) const PSYNC: Bytes = Bytes::from_static(resp::PSYNC);
pub(crate) const FULLRESYNC: Bytes = Bytes::from_static(resp::FULLRESYNC);
pub(crate) const WAIT: Bytes = Bytes::from_static(resp::WAIT);

pub(crate) const OK: Bytes = Bytes::from_static(resp::OK);
pub(crate) const PONG: Bytes = Bytes::from_static(resp::PONG);
pub(crate) const QUEUED: Bytes = Bytes::from_static(resp::QUEUED);
pub(crate) const NONE: Bytes = Bytes::from_static(b"none");

#[derive(Clone, Debug)]
pub enum Command {
    DBSize,
    Select(usize),
    Ping(Option<rdb::String>),
    Echo(rdb::String),
    Config(Arc<[Bytes]>),
    Info(Arc<[Bytes]>),
    Type(rdb::String),
    Keys(rdb::String),
    Get(rdb::String),
    Set(rdb::String, rdb::Value, set::Options),
    Incr(rdb::String),
    Multi,
    Exec,
    Discard,
    XAdd(rdb::String, stream::EntryArg, xadd::Options),
    XRange(rdb::String, xrange::Range, xrange::Count),
    XRead(xread::Options, Keys, xread::Ids),
    XLen(rdb::String),
    Replconf(replconf::Conf),
    PSync(ReplState),
    FullResync(ReplState),
    Wait(usize, Duration),
}

impl Command {
    pub async fn exec(self, server: &Instance, client: &mut Client) -> Resp {
        match self {
            // TODO: WAIT interaction
            Self::Exec => {
                let Some(commands) = client.tx_end() else {
                    return Resp::Resp(DataType::err(Error::err("EXEC without MULTI")));
                };

                if commands.is_empty() {
                    return Resp::Resp(DataType::array([]));
                }

                let mut resp = Vec::with_capacity(commands.len());
                let mut repl = Vec::with_capacity(commands.len());
                repl.push(DataType::from(Command::Multi));

                // NOTE: Redis guarantees that a request sent by another client will never be
                // served in the middle of an ongoing transaction. See:
                // https://redis.io/docs/latest/develop/interact/transactions
                let store = server.store.lock(LockType::Store).await;

                for cmd in commands {
                    // TODO: impl From<&Command> for DataType to avoid the clone
                    repl.push(cmd.clone().into());
                    let data = cmd.exec_locked(server, client, &store).await;

                    // TODO: support nested EXEC
                    let Resp::Resp(data) = data else {
                        unimplemented!("nested EXEC");
                    };

                    resp.push(data);
                }

                repl.push(DataType::from(Command::Exec));

                Resp::Exec {
                    resp: DataType::array(resp),
                    repl,
                }
            }

            Self::Discard => Resp::Resp(match client.tx_end() {
                Some(_) => DataType::str(OK),
                _ => DataType::err(Error::err("DISCARD without MULTI")),
            }),

            // Delay command execution after MULTI (i.e., during transactional execution)
            cmd if client.is_tx() => {
                client.tx_add(cmd);
                Resp::Resp(DataType::str(QUEUED))
            }

            cmd => {
                let store = server.store.lock(LockType::Database).await;
                cmd.exec_locked(server, client, &store).await
            }
        }
    }

    async fn exec_locked(
        self,
        server: &Instance,
        client: &mut Client,
        store: &StoreHandle<'_>,
    ) -> Resp {
        use rdb::String::*;
        match self {
            Self::DBSize => {
                let (persist_size, expire_size) = store.dbsize(client.db).await;
                Resp::Resp(DataType::Integer((persist_size + expire_size) as i64))
            }

            // TODO: SELECT replication
            Self::Select(db) => {
                client.db = db;
                Resp::Resp(DataType::str(OK))
            }

            Self::Ping(msg) => Resp::Resp(msg.map_or(DataType::str(PONG), DataType::BulkString)),

            Self::Echo(msg) => Resp::Resp(DataType::string(msg)),

            Self::Config(params) => {
                let items = params.iter().filter_map(|param| {
                    server
                        .cfg
                        .get(param)
                        .map(|value| (DataType::string(param.clone()), value))
                });

                let data = match PROTOCOL {
                    Protocol::RESP2 => DataType::array(items.flat_map(|(k, v)| [k, v])),
                    Protocol::RESP3 => DataType::map(items),
                };

                Resp::Resp(data)
            }

            Self::Info(sections) => {
                let num_sections = sections.len();
                let info = info::Info::new(server, &sections);
                let mut data = BytesMut::with_capacity(1024 * num_sections);
                let data = match write!(data, "{}", info) {
                    Ok(_) => DataType::string(data),
                    Err(e) => DataType::error(format!("failed to serialize {info:?}: {e:?}")),
                };
                Resp::Resp(data)
            }

            Self::Type(key) => Resp::Resp(
                store
                    .ty(client.db, key)
                    .await
                    .map_or(DataType::str(NONE), DataType::str),
            ),

            Self::Keys(pattern @ (Int8(_) | Int16(_) | Int32(_))) => {
                let data = if store.contains(client.db, &pattern).await {
                    DataType::array([DataType::string(pattern)])
                } else {
                    DataType::array([])
                };
                Resp::Resp(data)
            }

            Self::Keys(Str(pattern)) => match pattern.as_ref() {
                b"*" => {
                    let keys = store
                        .keys(client.db)
                        .await
                        .into_iter()
                        .map(DataType::BulkString);

                    Resp::Resp(DataType::array(keys))
                }
                _ => {
                    // TODO: support glob-style patterns
                    let key = Str(pattern);
                    let data = if store.contains(client.db, &key).await {
                        DataType::array([DataType::string(key)])
                    } else {
                        DataType::array([])
                    };
                    Resp::Resp(data)
                }
            },

            Self::Get(key) => Resp::Resp(
                store
                    .get(client.db, key)
                    .await
                    .map_or(NULL, DataType::string),
            ),

            Self::Set(key, val, ops) => {
                let (ops, get) = ops.into();
                let data = match store.set(client.db, key, val, ops).await {
                    Ok(Some(val)) if get => DataType::string(val),
                    Ok(_) => DataType::str(OK),
                    Err(_) => NULL,
                };
                Resp::Resp(data)
            }

            Self::Incr(key) => Resp::Resp(
                store
                    .incr(client.db, key)
                    .await
                    .map_or_else(DataType::err, DataType::Integer),
            ),

            Self::Multi => {
                client.tx_begin();
                Resp::Resp(DataType::str(OK))
            }

            Self::Exec => unreachable!("EXEC handled externally"),
            Self::Discard => unreachable!("DISCARD handled externally"),

            Self::XAdd(key, entry, ops) => Resp::Resp(
                store
                    .xadd(client.db, key, entry, ops)
                    .await
                    .map_or_else(DataType::err, |id| id.map_or(NULL, DataType::string)),
            ),

            Self::XRange(key, range, xrange::Count(count)) => Resp::Resp(
                store
                    .xrange(client.db, key, range, count)
                    .await
                    .map_or_else(DataType::err, |entries| {
                        entries.map_or(NULL, |es| {
                            DataType::array(es.into_iter().map(DataType::from))
                        })
                    }),
            ),

            Self::XRead(ops, keys, ids) => {
                Resp::Resp(store.xread(client.db, keys, ids, ops).await.map_or_else(
                    DataType::err,
                    |items| {
                        items.map_or(NULL, |items| {
                            let items = items.into_iter().map(|(key, entries)| {
                                (
                                    DataType::string(key),
                                    DataType::array(entries.into_iter().map(DataType::from)),
                                )
                            });

                            match PROTOCOL {
                                Protocol::RESP2 => DataType::array(
                                    items.map(|(key, entries)| DataType::array([key, entries])),
                                ),
                                Protocol::RESP3 => DataType::map(items),
                            }
                        })
                    },
                ))
            }

            Self::XLen(key) => Resp::Resp(
                store
                    .xlen(client.db, key)
                    .await
                    .map_or_else(DataType::err, |len| DataType::Integer(len as i64)),
            ),

            Self::Replconf(replconf::Conf::GetAck(_)) => {
                let ReplState { repl_offset, .. } = server.state();
                let repl_offset = repl_offset.to_string().into();
                let data = DataType::cmd([REPLCONF, ACK, repl_offset]);
                Resp::Resp(data)
            }

            // TODO: handle ListeningPort | Capabilities
            Self::Replconf(_) => Resp::Resp(DataType::str(OK)),

            Self::PSync(state) if matches!(server.role, Role::Replica(_)) => Resp::Resp(
                DataType::err(format!("unsupported command in a replica: PSYNC {state} ")),
            ),

            Self::PSync(ReplState {
                repl_id: None,
                repl_offset,
            }) if repl_offset.is_negative() => {
                let mut data = BytesMut::with_capacity(64);
                let data = match write!(data, "FULLRESYNC {}", server.state()) {
                    Ok(_) => DataType::str(data),
                    Err(e) => {
                        DataType::err(format!("failed to write response to PSYNC ? -1: {e:?}"))
                    }
                };
                Resp::Resp(data)
            }

            Self::PSync(state) => unimplemented!("handle PSYNC {state:?}"),
            Self::FullResync(state) => unimplemented!("handle FULLRESYNC {state:?}"),

            Self::Wait(num_replicas, timeout) => {
                let Role::Leader(replicas) = server.role() else {
                    return Resp::Resp(DataType::err(
                        "protocol violation: WAIT is only supported by a leader",
                    ));
                };

                // Snapshot current replication state/offset. This will include all the writes in
                // the master, including this connection's, completed _before_ this WAIT command.
                let state = server.state();

                let result = replicas
                    .wait(num_replicas, timeout, state)
                    .await
                    .with_context(|| format!("WAIT {num_replicas} {timeout:?}"));

                let data = match result {
                    Ok((n, offset)) if offset > 0 => {
                        let (state, new_offset) = server.shift_offset(offset);
                        println!("master offset: {} -> {new_offset}", state.repl_offset);
                        DataType::Integer(n as i64)
                    }
                    Ok((n, _)) => DataType::Integer(n as i64),
                    Err(e) => {
                        DataType::err(format!("WAIT {num_replicas} {timeout:?} failed with {e:?}"))
                    }
                };

                Resp::Resp(data)
            }
        }
    }

    /// Returns `true` iff this command represents a _write_ operation that's subject to
    /// replication.
    #[inline]
    pub(crate) fn is_write(&self) -> bool {
        // NOTE: SELECT is not a write although it can be sent over the replication connection.
        // Whether it is sent to a replica if client's selected DB differs from the repl conn DB.
        matches!(self, Self::Set(..) | Self::Incr(_) | Self::XAdd(..))
    }

    /// Returns `true` iff this command represents a replicated operation that should be
    /// acknowledged (i.e., responded to).
    #[inline]
    pub(crate) fn is_ack(&self) -> bool {
        matches!(self, Self::Replconf(replconf::Conf::GetAck(_)))
    }
}

#[derive(Debug)]
pub enum Resp {
    Resp(DataType),
    Exec { resp: DataType, repl: Vec<DataType> },
}

impl From<Command> for DataType {
    fn from(cmd: Command) -> Self {
        let items = match cmd {
            Command::DBSize => vec![Self::string(DBSIZE)],

            Command::Select(index) => vec![Self::string(SELECT), Self::string(index)],

            Command::Ping(None) => vec![Self::string(PING)],

            Command::Ping(Some(msg)) => vec![Self::string(PING), Self::string(msg)],

            Command::Echo(msg) => vec![Self::string(ECHO), Self::string(msg)],

            Command::Config(params) => {
                let mut items = Vec::with_capacity(2 + params.len());
                items.push(Self::string(CONFIG));
                items.push(Self::string(GET));
                items.extend(params.iter().cloned().map(Self::string));
                items
            }

            Command::Info(sections) => {
                let mut items = Vec::with_capacity(1 + sections.len());
                items.push(Self::string(INFO));
                items.extend(sections.iter().cloned().map(Self::string));
                items
            }

            Command::Type(key) => vec![Self::string(TYPE), Self::string(key)],

            Command::Keys(pattern) => vec![Self::string(KEYS), Self::string(pattern)],

            Command::Get(key) => vec![Self::string(GET), Self::string(key)],

            Command::Set(key, val, ops) => {
                let mut items = Vec::with_capacity(8);
                items.push(DataType::string(SET));
                items.push(DataType::string(key));
                items.push(DataType::string(val));
                items.extend(ops.into_bytes().map(DataType::string));
                items
            }

            Command::Incr(key) => vec![Self::string(INCR), Self::string(key)],

            Command::Multi => vec![Self::string(MULTI)],
            Command::Exec => vec![Self::string(EXEC)],
            Command::Discard => vec![Self::string(DISCARD)],

            Command::XAdd(key, stream::Entry { id, fields }, ops) => {
                let mut items = Vec::with_capacity(2 + ops.len() + 1 + 2 * fields.len());
                items.push(DataType::string(XADD));
                items.push(DataType::string(key));
                items.extend(ops.into_bytes().map(DataType::string));
                items.push(DataType::string(id));
                for (key, val) in fields.iter() {
                    items.push(DataType::string(key.clone()));
                    items.push(DataType::string(val.clone()));
                }
                items
            }

            Command::XRange(key, range, count) => {
                let (start, end) = range.into();
                let mut items = Vec::with_capacity(5);
                items.push(DataType::string(XRANGE));
                items.push(DataType::string(key));
                items.push(DataType::string(start));
                items.push(DataType::string(end));
                if let xrange::Count(Some(count)) = count {
                    items.push(DataType::string(count.to_string()));
                }
                items
            }

            Command::XRead(ops, keys, ids) => {
                let mut items = Vec::with_capacity(1 + ops.len() + 1 + keys.len() + ids.len());
                items.push(DataType::string(XREAD));
                items.extend(ops.into_bytes().map(DataType::string));
                items.push(DataType::string(xread::STREAMS));
                items.extend(keys.iter().cloned().map(DataType::string));
                items.extend(ids.iter_bytes().map(DataType::string));
                items
            }

            Command::XLen(key) => vec![Self::string(XLEN), Self::string(key)],

            Command::Replconf(replconf::Conf::ListeningPort(port)) => {
                vec![
                    Self::string(REPLCONF),
                    Self::string("listening-port"),
                    Self::string(port.to_string()),
                ]
            }

            Command::Replconf(replconf::Conf::GetAck(dummy)) => {
                vec![
                    Self::string(REPLCONF),
                    Self::string(GETACK),
                    Self::string(dummy),
                ]
            }

            Command::Replconf(replconf::Conf::Ack(offset)) => {
                let mut buf = BytesMut::with_capacity(16);
                write!(buf, "{offset}").expect("failed to serialize REPLCONF ACK");
                vec![Self::string(REPLCONF), Self::string(ACK), Self::string(buf)]
            }

            Command::Replconf(replconf::Conf::Capabilities(capabilities)) => {
                let mut items = Vec::with_capacity(1 + 2 * capabilities.len());
                items.push(Self::string(REPLCONF));
                for capa in capabilities.iter().cloned() {
                    items.push(Self::string("capa"));
                    items.push(Self::string(capa));
                }
                items
            }

            Command::PSync(ReplState {
                repl_id,
                repl_offset,
            }) => {
                vec![
                    Self::string(PSYNC),
                    Self::string(repl_id.unwrap_or_default()),
                    Self::string(repl_offset.to_string()),
                ]
            }

            Command::FullResync(state) => {
                let mut data = BytesMut::with_capacity(64);
                write!(data, "FULLRESYNC {state}").expect("failed to serialize FULLRESYNC");
                return DataType::SimpleString(data.freeze().into());
            }

            Command::Wait(num_replicas, timeout) => {
                vec![
                    Self::string(WAIT),
                    Self::string(num_replicas.to_string()),
                    Self::string(timeout.as_millis().to_string()),
                ]
            }
        };

        Self::array(items)
    }
}
