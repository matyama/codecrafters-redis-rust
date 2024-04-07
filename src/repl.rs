use std::{marker::PhantomData, net::SocketAddr};

use anyhow::{bail, Context, Result};
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

use crate::{DataReader, DataType, DataWriter, Resp, PING};

const TIMEOUT: Duration = Duration::from_secs(10);

pub struct Connected;
// pub struct Configured;
// pub struct Synced;

pub trait HandshakeState {}

impl HandshakeState for () {}
impl HandshakeState for Connected {}

pub struct Handshake<S: HandshakeState = ()> {
    _conn: TcpStream,
    state: PhantomData<S>,
}

impl Handshake<()> {
    pub async fn ping(leader: SocketAddr) -> Result<Handshake<Connected>> {
        // NOTE: this could use some form of connection pooling
        let mut conn = TcpStream::connect(leader)
            .await
            .with_context(|| format!("handshake({leader}): failed to establish connection"))?;

        let (reader, writer) = conn.split();
        let mut reader = DataReader::new(reader);
        let mut writer = DataWriter::new(writer);

        let ping = DataType::BulkString(PING);

        writer
            .write(DataType::array([ping]))
            .await
            .with_context(|| format!("handshake({leader}): init replication"))?;

        writer
            .flush()
            .await
            .with_context(|| format!("handshake({leader}): failed to flush PING"))?;

        let resp = timeout(TIMEOUT, reader.read_next())
            .await
            .with_context(|| format!("handshake({leader}): PING timed out"))?
            .with_context(|| format!("handshake({leader}): no response to PING"))?;

        let pong = |s| String::from_utf8_lossy(s).to_uppercase() == "PONG";

        match resp {
            Some(Resp::Data(DataType::SimpleString(s))) if pong(&s) => Ok(Handshake {
                _conn: conn,
                state: PhantomData,
            }),
            other => bail!("handshake({leader}): unexpected response to PING {other:?}"),
        }
    }
}
