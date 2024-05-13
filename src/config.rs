use std::env::Args;
use std::net::{SocketAddr, ToSocketAddrs as _};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use bytes::Bytes;

use crate::data::{DataExt as _, DataType};

fn listen_socket_addr(port: &impl std::fmt::Display) -> Result<SocketAddr> {
    format!("0.0.0.0:{port}")
        .parse()
        .with_context(|| format!("failed to parse listen socket address: '0.0.0.0:{port}'"))
}

#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) addr: SocketAddr,
    pub(crate) replica_of: Option<SocketAddr>,
    pub(crate) dir: PathBuf,
    pub(crate) dbfilename: PathBuf,
}

impl Config {
    #[inline]
    pub(crate) fn db_path(&self) -> impl AsRef<Path> {
        self.dir.join(self.dbfilename.as_path())
    }

    // TODO: support glob params
    pub(crate) fn get(&self, param: &Bytes) -> Option<DataType> {
        if param.matches("dir") {
            return Some(DataType::string(Bytes::copy_from_slice(
                self.dir.as_os_str().as_bytes(),
            )));
        }

        if param.matches("dbfilename") {
            return Some(DataType::string(Bytes::copy_from_slice(
                self.dbfilename.as_os_str().as_bytes(),
            )));
        }

        None
    }
}

impl Default for Config {
    #[inline]
    fn default() -> Self {
        Self {
            addr: listen_socket_addr(&6379).expect("default listen address"),
            replica_of: None,
            dir: PathBuf::from("./"),
            dbfilename: PathBuf::from("dump.rdb"),
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
                    let Some(replica_of) = args.next() else {
                        bail!(
                            "missing argument value of --replicaof \"<MASTER_HOST> <MASTER_PORT>\""
                        );
                    };

                    let replica_of = replica_of
                        .replace(' ', ":")
                        .to_socket_addrs()
                        .context("failed to resolve socket address for --replicaof")?
                        .next()
                        .context("invalid argument values for --replicaof")?;

                    cfg.replica_of = Some(replica_of);
                }

                "--dir" => {
                    let Some(dir) = args.next().map(PathBuf::from) else {
                        bail!("missing argument value for --dir");
                    };

                    cfg.dir = dir;
                }

                "--dbfilename" => {
                    let Some(dbfilename) = args.next().map(PathBuf::from) else {
                        bail!("missing argument value for --dbfilename");
                    };

                    cfg.dbfilename = dbfilename;
                }

                _ => continue,
            }
        }

        Ok(cfg)
    }
}
