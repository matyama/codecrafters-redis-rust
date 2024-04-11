use crate::{Instance, ReplId};

#[derive(Debug)]
pub enum Role {
    Master,
    Slave,
}

impl From<&crate::Role> for Role {
    #[inline]
    fn from(role: &crate::Role) -> Self {
        match role {
            crate::Role::Leader(_) => Self::Master,
            crate::Role::Replica(_) => Self::Slave,
        }
    }
}

impl std::fmt::Display for Role {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master => write!(f, "master"),
            Self::Slave => write!(f, "slave"),
        }
    }
}

#[derive(Debug, Default)]
pub enum FailoverState {
    #[default]
    NoFailover,
}

impl std::fmt::Display for FailoverState {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoFailover => write!(f, "no-failover"),
        }
    }
}

// TODO: add additional contextual fields (replica, ongoing SYNC op, link to master is down, etc.)
#[derive(Debug)]
pub struct Replication {
    /// Value is "master" if the instance is replica of no one, or "slave" if the instance is a
    /// replica of some master instance.
    ///
    /// Note that a replica can be master of another replica (chained replication).
    role: Role,

    /// Number of connected replicas
    connected_slaves: usize,

    /// The state of an ongoing failover, if any
    master_failover_state: FailoverState,

    /// The replication ID of the Redis server
    master_replid: ReplId,

    /// The secondary replication ID, used for PSYNC after a failover
    master_replid2: ReplId,

    /// The server's current replication offset
    master_repl_offset: isize,

    /// The offset up to which replication IDs are accepted
    second_repl_offset: isize,

    /// Flag indicating replication backlog is active
    repl_backlog_active: bool,

    /// Total size in bytes of the replication backlog buffer
    repl_backlog_size: usize,

    /// The master offset of the replication backlog buffer
    repl_backlog_first_byte_offset: usize,

    /// Size in bytes of the data in the replication backlog buffer
    repl_backlog_histlen: usize,
}

impl std::fmt::Display for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "# Replication")?;
        writeln!(f, "role:{}", self.role)?;
        writeln!(f, "connected_slaves:{}", self.connected_slaves)?;
        writeln!(f, "master_failover_state:{}", self.master_failover_state)?;
        writeln!(f, "master_replid:{}", self.master_replid)?;
        writeln!(f, "master_replid2:{}", self.master_replid2)?;
        writeln!(f, "master_repl_offset:{}", self.master_repl_offset)?;
        writeln!(f, "second_repl_offset:{}", self.second_repl_offset)?;
        writeln!(f, "repl_backlog_active:{}", self.repl_backlog_active as u8)?;
        writeln!(f, "repl_backlog_size:{}", self.repl_backlog_size)?;
        writeln!(
            f,
            "repl_backlog_first_byte_offset:{}",
            self.repl_backlog_first_byte_offset
        )?;
        write!(f, "repl_backlog_histlen:{}", self.repl_backlog_histlen)?;
        Ok(())
    }
}

impl Replication {
    #[inline]
    pub(crate) fn new(instance: &Instance) -> Self {
        Self {
            role: Role::from(&instance.role),
            connected_slaves: 0,
            master_failover_state: FailoverState::default(),
            master_replid: instance.state.repl_id.clone().unwrap_or_default(),
            master_replid2: instance.state.repl_id.clone().unwrap_or_default(),
            master_repl_offset: instance.state.repl_offset,
            second_repl_offset: -1,
            repl_backlog_active: false,
            repl_backlog_size: 0,
            repl_backlog_first_byte_offset: 0,
            repl_backlog_histlen: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct Info {
    replication: Option<Replication>,
}

impl Info {
    pub(crate) fn new(instance: &Instance, sections: Vec<bytes::Bytes>) -> Self {
        if sections.is_empty() {
            return Self {
                replication: Some(Replication::new(instance)),
            };
        }

        let mut info = Self::default();

        for section in sections {
            match section.to_ascii_lowercase().as_slice() {
                b"replication" => info.replication = Some(Replication::new(instance)),
                _ => continue,
            }
        }

        info
    }
}

impl std::fmt::Display for Info {
    // TODO: other fields, separate sections by newline
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref replication) = self.replication {
            write!(f, "{}", replication)?;
        }
        Ok(())
    }
}
