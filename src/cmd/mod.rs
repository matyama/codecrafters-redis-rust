use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use anyhow::Context;
use bytes::{Bytes, BytesMut};

use crate::data::DataType;
use crate::rdb;
use crate::repl::ReplState;
use crate::{
    Instance, Protocol, Role, ACK, CONFIG, ECHO, GET, GETACK, INFO, KEYS, NONE, OK, PING, PONG,
    PROTOCOL, PSYNC, REPLCONF, SET, TYPE, WAIT,
};

pub mod info;
pub mod replconf;
pub mod set;

const NULL: DataType = match PROTOCOL {
    Protocol::RESP2 => DataType::NullBulkString,
    Protocol::RESP3 => DataType::Null,
};

#[derive(Clone, Debug)]
pub enum Command {
    Ping(Option<rdb::String>),
    Echo(rdb::String),
    Config(Arc<[Bytes]>),
    Info(Arc<[Bytes]>),
    Type(rdb::String),
    Keys(rdb::String),
    Get(rdb::String),
    Set(rdb::String, rdb::Value, set::Options),
    Replconf(replconf::Conf),
    PSync(ReplState),
    FullResync(ReplState),
    Wait(usize, Duration),
}

impl Command {
    pub async fn exec(self, instance: Arc<Instance>) -> DataType {
        use rdb::String::*;
        match self {
            Self::Ping(msg) => msg.map_or(DataType::str(PONG), DataType::BulkString),

            Self::Echo(msg) => DataType::string(msg),

            Self::Config(params) if params.is_empty() => {
                DataType::err("ERR wrong number of arguments for 'config|get' command")
            }

            Self::Config(params) => {
                let items = params.iter().filter_map(|param| {
                    instance
                        .cfg
                        .get(param)
                        .map(|value| (DataType::string(param.clone()), value))
                });

                match PROTOCOL {
                    Protocol::RESP2 => DataType::array(items.flat_map(|(k, v)| [k, v])),
                    Protocol::RESP3 => DataType::map(items),
                }
            }

            Self::Info(sections) => {
                let num_sections = sections.len();
                let info = info::Info::new(&instance, &sections);
                let mut data = BytesMut::with_capacity(1024 * num_sections);
                match write!(data, "{}", info) {
                    Ok(_) => DataType::string(data),
                    Err(e) => DataType::err(format!("failed to serialize {info:?}: {e:?}")),
                }
            }

            Self::Type(key) => instance
                .store
                .ty(key)
                .await
                .map_or(DataType::str(NONE), DataType::str),

            Self::Keys(pattern @ (Int8(_) | Int16(_) | Int32(_))) => {
                // TODO: impl as `store.contains(pattern)`
                eprintln!("KEYS {pattern:?} is not supported");
                DataType::array([])
            }

            Self::Keys(Str(pattern)) => match pattern.as_ref() {
                b"*" => {
                    let keys = instance
                        .store
                        .keys()
                        .await
                        .into_iter()
                        .map(DataType::BulkString);

                    DataType::array(keys)
                }
                pattern => {
                    eprintln!("KEYS {pattern:?} is not supported");
                    DataType::array([])
                }
            },

            Self::Get(key) => instance.store.get(key).await.map_or(NULL, DataType::string),

            Self::Set(key, val, ops) => {
                let (ops, get) = ops.into();
                match instance.store.set(key, val, ops).await {
                    Ok(Some(val)) if get => DataType::string(val),
                    Ok(_) => DataType::str(OK),
                    Err(_) => NULL,
                }
            }

            Self::Replconf(replconf::Conf::GetAck(_)) => {
                let ReplState { repl_offset, .. } = instance.state();
                let repl_offset = repl_offset.to_string().into();
                DataType::cmd([REPLCONF, ACK, repl_offset])
            }

            // TODO: handle ListeningPort | Capabilities
            Self::Replconf(_) => DataType::str(OK),

            Self::PSync(state) if matches!(instance.role, Role::Replica(_)) => {
                DataType::err(format!("unsupported command in a replica: PSYNC {state} "))
            }

            Self::PSync(ReplState {
                repl_id: None,
                repl_offset,
            }) if repl_offset.is_negative() => {
                let mut data = BytesMut::with_capacity(64);
                match write!(data, "FULLRESYNC {}", instance.state()) {
                    Ok(_) => DataType::str(data),
                    Err(e) => {
                        DataType::err(format!("failed to write response to PSYNC ? -1: {e:?}"))
                    }
                }
            }

            Self::PSync(state) => unimplemented!("handle PSYNC {state:?}"),
            Self::FullResync(state) => unimplemented!("handle FULLRESYNC {state:?}"),

            Self::Wait(num_replicas, timeout) => {
                let Role::Leader(replicas) = instance.role() else {
                    return DataType::err("protocol violation: WAIT is only supported by a leader");
                };

                // Snapshot current replication state/offset. This will include all the writes in
                // the master, including this connection's, completed _before_ this WAIT command.
                let state = instance.state();

                let result = replicas
                    .wait(num_replicas, timeout, state)
                    .await
                    .with_context(|| format!("WAIT {num_replicas} {timeout:?}"));

                match result {
                    Ok((n, offset)) if offset > 0 => {
                        let (state, new_offset) = instance.shift_offset(offset);
                        println!("master offset: {} -> {new_offset}", state.repl_offset);
                        DataType::Integer(n as i64)
                    }
                    Ok((n, _)) => DataType::Integer(n as i64),
                    Err(e) => {
                        DataType::err(format!("WAIT {num_replicas} {timeout:?} failed with {e:?}"))
                    }
                }
            }
        }
    }

    /// Returns `true` iff this command represents either an operation that's subject to
    /// replication or a command that can be forwarded to a replication connection.
    #[inline]
    pub(crate) fn is_repl(&self) -> bool {
        self.is_write() || matches!(self, Self::Replconf(replconf::Conf::GetAck(_)))
    }

    /// Returns `true` iff this command represents a _write_ operation that's subject to
    /// replication.
    #[inline]
    pub(crate) fn is_write(&self) -> bool {
        matches!(self, Self::Set(..))
    }

    /// Returns `true` iff this command represents a replicated operation that should be
    /// acknowledged (i.e., responded to).
    #[inline]
    pub(crate) fn is_ack(&self) -> bool {
        matches!(self, Self::Replconf(replconf::Conf::GetAck(_)))
    }
}

impl From<Command> for DataType {
    fn from(cmd: Command) -> Self {
        let items = match cmd {
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
