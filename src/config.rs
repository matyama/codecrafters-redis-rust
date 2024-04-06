use std::net::SocketAddr;
use std::{env::Args, net::ToSocketAddrs};

use anyhow::{bail, Context, Result};

#[derive(Debug)]
pub struct Config {
    pub port: u16,
    pub replica_of: Option<SocketAddr>,
}

impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self {
            port: 6379,
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
                    cfg.port = args
                        .next()
                        .context("missing argument value for --port")?
                        .parse()
                        .context("invalid argument value for --port")?;
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
