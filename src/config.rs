use std::env::Args;
use std::net::{SocketAddr, ToSocketAddrs as _};

use anyhow::{bail, Context, Result};

fn listen_socket_addr(port: &impl std::fmt::Display) -> Result<SocketAddr> {
    format!("0.0.0.0:{port}")
        .parse()
        .with_context(|| format!("failed to parse listen socket address: '0.0.0.0:{port}'"))
}

#[derive(Debug)]
pub struct Config {
    pub(crate) addr: SocketAddr,
    pub(crate) replica_of: Option<SocketAddr>,
}

impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self {
            addr: listen_socket_addr(&6379).expect("default listen address"),
            replica_of: None,
        }
    }
}

impl TryFrom<Args> for Config {
    type Error = anyhow::Error;

    fn try_from(args: Args) -> Result<Self> {
        let mut args = args.into_iter().skip(1);

        let mut cfg = Self::default();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" | "-p" => {
                    let Some(port) = args.next() else {
                        bail!("missing argument value for --port");
                    };

                    let Ok(addr) = listen_socket_addr(&port) else {
                        bail!("invalid argument value for --port: '{port}'");
                    };

                    cfg.addr = addr;
                }

                "--replicaof" => {
                    let Some(host) = args.next() else {
                        bail!("missing host part of --replicaof <MASTER_HOST> <MASTER_PORT>");
                    };

                    let Some(port) = args.next() else {
                        bail!("missing port part of --replicaof <MASTER_HOST> <MASTER_PORT>");
                    };

                    let replica_of = format!("{host}:{port}")
                        .to_socket_addrs()
                        .context("failed to resolve socket address for --replicaof")?
                        .next()
                        .context("invalid argument values for --replicaof")?;

                    cfg.replica_of = Some(replica_of);
                }

                _ => continue,
            }
        }

        Ok(cfg)
    }
}
