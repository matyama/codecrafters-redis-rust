use std::collections::VecDeque;
use std::fmt::Write;
use std::future::ready;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::time::sleep;

use crate::store::{Key, Value};
use crate::{
    DataType, Instance, Protocol, ReplState, Role, ACK, ECHO, GET, INFO, OK, PING, PONG, PROTOCOL,
    PSYNC, REPLCONF, SET, WAIT,
};

pub mod info;
pub mod replconf;
pub mod set;

const NULL: DataType = match PROTOCOL {
    Protocol::RESP2 => DataType::NullBulkString,
    Protocol::RESP3 => DataType::Null,
};

#[derive(Debug, Clone)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Info(Vec<Bytes>),
    Get(Key),
    Set(Key, Value, set::Options),
    Replconf(replconf::Conf),
    PSync(ReplState),
    FullResync(ReplState),
    Wait(usize, Option<Duration>),
}

impl Command {
    pub async fn exec(self, instance: Arc<Instance>) -> DataType {
        match self {
            Self::Ping(msg) => msg.map_or(DataType::SimpleString(PONG), DataType::BulkString),

            Self::Echo(msg) => DataType::BulkString(msg),

            Self::Info(sections) => {
                let num_sections = sections.len();
                let info = info::Info::new(&instance, sections);
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
                let offset = instance.offset.load(Ordering::Acquire);
                DataType::array([
                    DataType::BulkString(REPLCONF),
                    DataType::BulkString(ACK),
                    DataType::BulkString(offset.to_string().into()),
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
                repl_offset: -1,
            }) => {
                let mut data = BytesMut::with_capacity(64);
                match write!(data, "FULLRESYNC {}", instance.state) {
                    Ok(_) => DataType::SimpleString(data.freeze()),
                    Err(e) => {
                        let error = format!("failed to write response to PSYNC ? -1: {e:?}");
                        DataType::SimpleError(error.into())
                    }
                }
            }

            Self::PSync(state) => unimplemented!("handle PSYNC {state:?}"),
            Self::FullResync(state) => unimplemented!("handle FULLRESYNC {state:?}"),

            // TODO: implement actual WAIT
            Self::Wait(num_replicas, Some(timeout)) => {
                let num_acks = tokio::select! {
                    _ = sleep(timeout) => num_replicas,
                    num_acks = ready(num_replicas) => num_acks,
                };
                DataType::Integer(num_acks as i64)
            }

            Self::Wait(_, None) => unimplemented!("WAIT forever is not supported yet"),
        }
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
            Command::Ping(None) => VecDeque::from([DataType::BulkString(PING)]),

            Command::Ping(Some(msg)) => {
                VecDeque::from([DataType::BulkString(PING), DataType::BulkString(msg)])
            }

            Command::Echo(msg) => {
                VecDeque::from([DataType::BulkString(ECHO), DataType::BulkString(msg)])
            }

            Command::Info(sections) => {
                let mut items = VecDeque::with_capacity(1 + sections.len());
                items.push_back(DataType::BulkString(INFO));
                items.extend(sections.into_iter().map(DataType::BulkString));
                items
            }

            Command::Get(Key(key)) => {
                VecDeque::from([DataType::BulkString(GET), DataType::BulkString(key)])
            }

            Command::Set(Key(key), Value(value), ops) => {
                // TODO(optimization): cmd writer with single BytesMut alloc and Bytes splits
                let mut items: VecDeque<DataType> = ops.into();
                items.push_front(DataType::BulkString(value));
                items.push_front(DataType::BulkString(key));
                items.push_front(DataType::BulkString(SET));
                items
            }

            Command::Replconf(replconf::Conf::ListeningPort(port)) => {
                let port = port.to_string().into();
                VecDeque::from([
                    DataType::BulkString(REPLCONF),
                    DataType::string(b"listening-port"),
                    DataType::BulkString(port),
                ])
            }

            Command::Replconf(replconf::Conf::GetAck(dummy)) => VecDeque::from([
                DataType::BulkString(REPLCONF),
                DataType::string(b"getack"),
                DataType::BulkString(dummy),
            ]),

            Command::Replconf(replconf::Conf::Capabilities(mut capabilities)) => {
                capabilities.push_front(REPLCONF);
                capabilities.into_iter().map(DataType::BulkString).collect()
            }

            Command::PSync(ReplState {
                repl_id,
                repl_offset,
            }) => {
                let repl_id = repl_id.unwrap_or_default().into();
                let repl_offset = repl_offset.to_string().into();
                VecDeque::from([
                    DataType::BulkString(PSYNC),
                    DataType::BulkString(repl_id),
                    DataType::BulkString(repl_offset),
                ])
            }

            Command::FullResync(state) => {
                let mut data = BytesMut::with_capacity(64);
                write!(data, "FULLRESYNC {state}").expect("failed to serialize FULLRESYNC");
                return DataType::SimpleString(data.freeze());
            }

            Command::Wait(num_replicas, timeout) => {
                let timeout = timeout.unwrap_or_default().as_millis();
                VecDeque::from([
                    DataType::BulkString(WAIT),
                    DataType::BulkString(num_replicas.to_string().into()),
                    DataType::BulkString(timeout.to_string().into()),
                ])
            }
        };

        DataType::Array(items)
    }
}
