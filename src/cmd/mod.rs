use std::fmt::Write;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use anyhow::Context;
use bytes::{Bytes, BytesMut};

use crate::store::{Key, Value};
use crate::{
    DataType, Instance, Protocol, ReplState, Role, ACK, CONFIG, ECHO, GET, GETACK, INFO, OK, PING,
    PONG, PROTOCOL, PSYNC, REPLCONF, SET, WAIT,
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
    Ping(Option<Bytes>),
    Echo(Bytes),
    Config(Arc<[Bytes]>),
    Info(Arc<[Bytes]>),
    Get(Key),
    Set(Key, Value, set::Options),
    Replconf(replconf::Conf),
    PSync(ReplState),
    FullResync(ReplState),
    Wait(usize, Duration),
}

impl Command {
    pub async fn exec(self, instance: Arc<Instance>) -> DataType {
        match self {
            Self::Ping(msg) => msg.map_or(DataType::SimpleString(PONG), DataType::BulkString),

            Self::Echo(msg) => DataType::BulkString(msg),

            Self::Config(params) if params.is_empty() => DataType::SimpleError(Bytes::from_static(
                b"ERR wrong number of arguments for 'config|get' command",
            )),

            Self::Config(params) => {
                let items = params.iter().filter_map(|param| {
                    instance
                        .cfg
                        .get(param)
                        .map(|value| (DataType::BulkString(param.clone()), value))
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
                    Ok(_) => DataType::BulkString(data.freeze()),
                    Err(e) => {
                        let error = format!("failed to serialize {info:?}: {e:?}");
                        DataType::SimpleError(error.into())
                    }
                }
            }

            Self::Get(key) => instance
                .store
                .get(key)
                .await
                .map_or(NULL, |Value(value)| DataType::BulkString(value)),

            Self::Set(key, value, ops) => {
                let (ops, get) = ops.into();
                match instance.store.set(key, value, ops).await {
                    Ok(Some(Value(data))) if get => DataType::BulkString(data),
                    Ok(_) => DataType::SimpleString(OK),
                    Err(_) => NULL,
                }
            }

            Self::Replconf(replconf::Conf::GetAck(_)) => {
                let ReplState { repl_offset, .. } = instance.state();
                DataType::array([
                    DataType::BulkString(REPLCONF),
                    DataType::BulkString(ACK),
                    DataType::BulkString(repl_offset.to_string().into()),
                ])
            }

            // TODO: handle ListeningPort | Capabilities
            Self::Replconf(_) => DataType::SimpleString(OK),

            Self::PSync(state) if matches!(instance.role, Role::Replica(_)) => {
                let error = format!("unsupported command in a replica: PSYNC {state} ");
                DataType::SimpleError(error.into())
            }

            Self::PSync(ReplState {
                repl_id: None,
                repl_offset,
            }) if repl_offset.is_negative() => {
                let mut data = BytesMut::with_capacity(64);
                match write!(data, "FULLRESYNC {}", instance.state()) {
                    Ok(_) => DataType::SimpleString(data.freeze()),
                    Err(e) => {
                        let error = format!("failed to write response to PSYNC ? -1: {e:?}");
                        DataType::SimpleError(error.into())
                    }
                }
            }

            Self::PSync(state) => unimplemented!("handle PSYNC {state:?}"),
            Self::FullResync(state) => unimplemented!("handle FULLRESYNC {state:?}"),

            Self::Wait(num_replicas, timeout) => {
                let Role::Leader(replicas) = instance.role() else {
                    return DataType::SimpleError(Bytes::from_static(
                        b"protocol violation: WAIT is only supported by a leader",
                    ));
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
                        let error = format!("WAIT {num_replicas} {timeout:?} failed with {e:?}");
                        DataType::SimpleError(error.into())
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
            Command::Ping(None) => vec![PING],

            Command::Ping(Some(msg)) => vec![PING, msg],

            Command::Echo(msg) => vec![ECHO, msg],

            Command::Config(params) => {
                let mut items = Vec::with_capacity(2 + params.len());
                items.push(CONFIG);
                items.push(GET);
                items.extend(params.iter().cloned());
                items
            }

            Command::Info(sections) => {
                let mut items = Vec::with_capacity(1 + sections.len());
                items.push(INFO);
                items.extend(sections.iter().cloned());
                items
            }

            Command::Get(Key(key)) => vec![GET, key],

            Command::Set(Key(key), Value(value), ops) => {
                let mut items = Vec::with_capacity(8);
                items.push(SET);
                items.push(key);
                items.push(value);
                items.extend(ops.into_bytes());
                items
            }

            Command::Replconf(replconf::Conf::ListeningPort(port)) => {
                let port = port.to_string().into();
                vec![REPLCONF, Bytes::from_static(b"listening-port"), port]
            }

            Command::Replconf(replconf::Conf::GetAck(dummy)) => {
                vec![REPLCONF, GETACK, dummy]
            }

            Command::Replconf(replconf::Conf::Ack(offset)) => {
                let mut buf = BytesMut::with_capacity(16);
                write!(buf, "{offset}").expect("failed to serialize REPLCONF ACK");
                vec![REPLCONF, ACK, buf.freeze()]
            }

            Command::Replconf(replconf::Conf::Capabilities(capabilities)) => {
                let mut items = Vec::with_capacity(1 + 2 * capabilities.len());
                items.push(REPLCONF);
                for capa in capabilities.iter().cloned() {
                    items.push(Bytes::from_static(b"capa"));
                    items.push(capa);
                }
                items
            }

            Command::PSync(ReplState {
                repl_id,
                repl_offset,
            }) => {
                let repl_id = repl_id.unwrap_or_default().into();
                let repl_offset = repl_offset.to_string().into();
                vec![PSYNC, repl_id, repl_offset]
            }

            Command::FullResync(state) => {
                let mut data = BytesMut::with_capacity(64);
                write!(data, "FULLRESYNC {state}").expect("failed to serialize FULLRESYNC");
                return DataType::SimpleString(data.freeze());
            }

            Command::Wait(num_replicas, timeout) => {
                let num_replicas = num_replicas.to_string().into();
                let timeout = timeout.as_millis().to_string().into();
                vec![WAIT, num_replicas, timeout]
            }
        };

        DataType::cmd(items)
    }
}
