use std::collections::hash_map::{Entry, OccupiedEntry, VacantEntry};
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::{bail, Result};
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock};
use tokio::task::JoinSet;

use crate::cmd::{set, xadd, xrange, xread};
use crate::data::Keys;
use crate::{rdb, stream, Error};

impl From<ValueCell> for rdb::Value {
    #[inline]
    fn from(ValueCell { data, .. }: ValueCell) -> Self {
        data
    }
}

impl From<&ValueCell> for rdb::ValueType {
    #[inline]
    fn from(value: &ValueCell) -> Self {
        Self::from(&value.data)
    }
}

#[derive(Debug)]
pub struct ValueCell {
    data: rdb::Value,
    write: Arc<Notify>,
    expiry: Option<SystemTime>,
}

impl ValueCell {
    #[inline]
    fn new(data: rdb::Value, expiry: Option<SystemTime>) -> Self {
        Self {
            data,
            write: Arc::new(Notify::new()),
            expiry,
        }
    }

    /// Replace the content of this cell in-place and notify all write waiters.
    ///
    /// Returns the previous [`Value`](rdb::Value) stored in this cell.
    fn write(&mut self, mut data: rdb::Value, expiry: Option<SystemTime>) -> rdb::Value {
        mem::swap(&mut self.data, &mut data);
        self.expiry = expiry;
        self.write.notify_waiters();
        data
    }
}

#[derive(Debug)]
pub struct Database {
    pub(crate) ix: usize,
    db: HashMap<rdb::String, ValueCell>,
    // XXX: replace (also in cells) by single watch (issue: no wait_for requires tokio >= 1.37)
    /// Notify subscribers about recently inserted keys
    new: Arc<Notify>,
}

impl Database {
    pub const DEFAULT: usize = 0;

    #[inline]
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct DatabaseBuilder {
    ix: Option<usize>,
    db: Option<HashMap<rdb::String, ValueCell>>,
}

impl DatabaseBuilder {
    #[inline]
    pub fn with_index(&mut self, ix: usize) {
        self.ix = Some(ix);
    }

    #[inline]
    pub fn with_capacity(&mut self, capacity: usize) {
        self.db = Some(HashMap::with_capacity(capacity));
    }

    #[inline]
    pub fn is_resized(&self) -> bool {
        self.ix.is_some() && self.db.is_some()
    }

    #[inline]
    pub fn set(&mut self, key: rdb::String, val: rdb::Value, expiry: Option<SystemTime>) {
        if let Some(db) = self.db.as_mut() {
            match expiry {
                // XXX: is now() actually taken per key or at the start of parsing a RDB
                // filter out keys that have already expired
                Some(expiry) if expiry < SystemTime::now() => {}
                expiry => {
                    let _ = db.insert(key, ValueCell::new(val, expiry));
                }
            }
        }
    }

    #[inline]
    pub fn build(self) -> Result<Database> {
        match (self.ix, self.db) {
            (Some(ix), Some(db)) => Ok(Database {
                ix,
                db,
                new: Arc::new(Notify::new()),
            }),
            (ix, _) => bail!("incomplete db={ix:?}"),
        }
    }
}

/// Result alias for X (stream) operations
pub type XResult<T> = Result<Option<T>, Error>;

struct StreamHandle {
    stream: stream::Stream,
    write: Arc<Notify>,
    new: bool,
}

impl StreamHandle {
    /// Determine the lookup range for XREAD.
    ///
    /// If there was no stream before XREAD started, then this implicitly starts from 0-0
    /// (exclusive). Also, it returns `None` if start > end (i.e., trivially empty interval).
    async fn xread_range(&self, start: xread::Id) -> Option<xrange::IdRange> {
        let last_entry = if self.new {
            stream::StreamId::ZERO
        } else {
            self.stream.lock().await.last_entry
        };

        let start = start.unwrap_or(last_entry);

        xrange::Range::lopen(start).into()
    }
}

// NOTE: The store implementation could use some optimization:
//  1. Using a `Mutex` creates a single contention point which is no good. Also, it's quite
//     unfortunate that `get` does a write as well, which prevents using a `RwLock` here.
//  2. Multiple connections accessing _distinct_ keys still contend on a single (root) lock. It'd
//     be more scalable to use some fine-grained locking (e.g., lock per key, sharding or some
//     tree-like structure instead of a `HashMap`).
//  3. Lock-free implementations might turn out to perform better as well.
#[derive(Debug)]
struct StoreInner {
    ix: usize,
    dbs: HashMap<usize, Mutex<Database>>,
}

impl StoreInner {
    async fn db(&self) -> MutexGuard<Database> {
        self.dbs[&self.ix].lock().await
    }
}

impl Default for StoreInner {
    fn default() -> Self {
        let ix = Database::DEFAULT;

        let db = Database {
            ix,
            db: HashMap::default(),
            new: Arc::new(Notify::new()),
        };

        let mut dbs = HashMap::with_capacity(Store::DEFAULT_SIZE);
        dbs.insert(ix, Mutex::new(db));

        Self { ix, dbs }
    }
}

#[derive(Debug, Default)]
pub struct Store(RwLock<StoreInner>);

impl Store {
    /// Default number of databases this store starts with
    pub(crate) const DEFAULT_SIZE: usize = 16;

    // TODO: support patters other than *
    pub async fn keys(&self) -> Vec<rdb::String> {
        let store = self.0.read().await;
        let guard = store.db().await;
        guard.db.keys().cloned().collect()
    }

    pub async fn ty(&self, key: rdb::String) -> Option<rdb::ValueType> {
        let store = self.0.read().await;
        let guard = store.db().await;
        guard.db.get(&key).map(rdb::ValueType::from)
    }

    pub async fn get(&self, key: rdb::String) -> Option<rdb::Value> {
        let store = self.0.read().await;
        let mut guard = store.db().await;
        match guard.db.entry(key) {
            Entry::Occupied(e) => match e.get() {
                ValueCell {
                    data: _,
                    expiry: Some(expiry),
                    ..
                } if *expiry < SystemTime::now() => {
                    let old = e.remove();
                    old.write.notify_waiters();
                    None
                }
                ValueCell { data, .. } => Some(data.clone()),
            },
            Entry::Vacant(_) => None,
        }
    }

    pub async fn set(
        &self,
        key: rdb::String,
        val: rdb::Value,
        set::StoreOptions { exp, cond }: set::StoreOptions,
    ) -> Result<Option<rdb::Value>, ()> {
        use set::{Condition::*, Expiry::*};

        let store = self.0.read().await;
        let mut guard = store.db().await;

        // XXX: not sure at what point Redis takes the timestamp to determine expiration
        let now = SystemTime::now();

        let expiration = |e: &OccupiedEntry<'_, rdb::String, ValueCell>| match exp {
            Some(EX(ttl) | PX(ttl)) => Some(now + ttl),
            Some(EXAT(_) | PXAT(_)) => unimplemented!("SET with EXAT or PXAT isn't supported"),
            Some(KeepTTL) if e.expired_before(now) => None,
            Some(KeepTTL) => e.get().expiry,
            None => None,
        };

        let value = match guard.db.entry(key.clone()) {
            // Overwrite existing entry that has expired before this operation
            Entry::Occupied(mut e) if e.expired_before(now) && matches!(cond, Some(NX) | None) => {
                let expired = e.expired_before(now);
                let expiry = expiration(&e);

                let old = e.get_mut().write(val, expiry);

                Ok(if expired { None } else { Some(old) })
            }

            // Overwrite existing entry that has not expired (or has no expiration set)
            Entry::Occupied(mut e) if matches!(cond, Some(XX) | None) => {
                let expiry = expiration(&e);
                Ok(Some(e.get_mut().write(val, expiry)))
            }

            // Insert new value (note: we know this is a new entry, so could not be an old one)
            Entry::Vacant(e) if matches!(cond, Some(NX) | None) => {
                // TODO: implement EXAT and PXAT
                let expiry = if let Some(EX(ttl) | PX(ttl)) = exp {
                    Some(now + ttl)
                } else {
                    None
                };

                e.insert(ValueCell::new(val, expiry));
                guard.new.notify_waiters();

                Ok(None)
            }

            // Other cases which don't meet given conditions (NX | XX)
            _ => Err(()),
        };

        value
    }

    pub async fn xadd(
        &self,
        key: rdb::String,
        entry: stream::EntryArg,
        ops: xadd::Options,
    ) -> XResult<stream::StreamId> {
        if ops.no_mkstream {
            return Ok(None);
        }

        let store = self.0.read().await;
        let mut guard = store.db().await;

        match guard.db.entry(key) {
            Entry::Occupied(mut e) => {
                let ValueCell { data, write, .. } = e.get_mut();

                let rdb::Value::Stream(s) = data else {
                    return Err(Error::WrongType);
                };

                let mut s = s.lock().await;

                let entry = entry.next(Some(s.last_entry));

                if entry.id <= s.last_entry {
                    let err = "The ID specified in XADD is \
                               equal or smaller than the target stream top item";
                    return Err(Error::err(err));
                }

                s.last_entry = entry.id;
                s.entries.insert(entry.id, entry);
                s.length += 1;
                // TODO: update cgroups?

                // notify all waiting XREAD clients that a new entry has been added
                write.notify_waiters();

                Ok(Some(s.last_entry))
            }

            Entry::Vacant(e) => {
                let entry = entry.next(None);

                let id = entry.id;
                let last_entry = entry.id;

                let mut entries = BTreeMap::new();
                entries.insert(entry.id, entry);

                let cgroups = vec![];

                let stream = stream::Stream::new(entries, 1, last_entry, cgroups);

                e.insert(ValueCell::new(rdb::Value::Stream(stream), None));
                guard.new.notify_waiters();

                Ok(Some(id))
            }
        }
    }

    pub async fn xrange(
        &self,
        key: rdb::String,
        range: xrange::Range,
        count: Option<usize>,
    ) -> XResult<Vec<stream::Entry>> {
        let store = self.0.read().await;
        let guard = store.db().await;

        let Some(ValueCell { data, .. }) = guard.db.get(&key) else {
            return Ok(Some(vec![]));
        };

        let rdb::Value::Stream(s) = data else {
            return Err(Error::WrongType);
        };

        let count = match count {
            Some(0) => return Ok(None),
            Some(c) => c,
            None => usize::MAX,
        };

        // NOTE: returns an error if start > end (i.e., trivially empty interval)
        let Some(range): Option<xrange::IdRange> = range.into() else {
            return Ok(None);
        };

        let s = s.lock().await;

        let entries = s
            .entries
            .range(range)
            .take(count)
            .map(|(_, e)| e.clone())
            .collect();

        Ok(Some(entries))
    }

    async fn get_stream(
        &self,
        key: &rdb::String,
    ) -> Result<(Option<(stream::Stream, Arc<Notify>)>, Arc<Notify>), Error> {
        let store = self.0.read().await;
        let guard = store.db().await;
        let new = Arc::clone(&guard.new);

        let Some(ValueCell { data, write, .. }) = guard.db.get(key) else {
            return Ok((None, new));
        };

        let rdb::Value::Stream(stream) = data else {
            return Err(Error::WrongType);
        };

        let stream = stream.clone();
        let write = Arc::clone(write);

        Ok((Some((stream, write)), new))
    }

    async fn lookup_stream(
        &self,
        key: &rdb::String,
        blocking: bool,
        unblock: &Notify,
    ) -> XResult<StreamHandle> {
        let new = match self.get_stream(key).await? {
            (Some((stream, write)), _) => {
                return Ok(Some(StreamHandle {
                    stream,
                    write,
                    new: false,
                }))
            }
            (_, new) => new,
        };

        loop {
            tokio::select! {
                _ = new.notified(), if blocking => {
                    if let (Some((stream, write)), _) = self.get_stream(key).await? {
                        return Ok(Some(StreamHandle { stream, write, new: true }));
                    }
                },
                _ = unblock.notified(), if blocking => break Ok(None),
                else => break Ok(None),
            }
        }
    }

    pub async fn xread(
        self: Arc<Self>,
        keys: Keys,
        ids: xread::Ids,
        ops: xread::Options,
    ) -> XResult<Vec<(rdb::String, Vec<stream::Entry>)>> {
        if keys.len() != ids.len() {
            let err = "Unbalanced 'xread' list of streams: \
                       for each stream key an ID or '$' must be specified.";
            return Err(Error::err(err));
        }

        let block = ops.block.is_some();
        let (count, timeout) = ops.into();

        // NOTE: IndexMap would be nice here
        let mut streams = vec![None; keys.len()];
        let mut success = false;

        let mut tasks = JoinSet::new();
        let unblock = Arc::new(Notify::new());

        for (k, (key, id)) in keys.iter().cloned().zip(ids.iter().cloned()).enumerate() {
            let store = Arc::clone(&self);
            let unblock = Arc::clone(&unblock);

            tasks.spawn(async move {
                let Some(stream) = store.lookup_stream(&key, block, &unblock).await? else {
                    // NOTE: non-existent keys (streams) are simply filtered out
                    return Ok((k, None));
                };

                // determine the lookup range once ahead of time (important for ID=$)
                let Some(range) = stream.xread_range(id).await else {
                    return Ok((k, None));
                };

                let StreamHandle { stream, write, .. } = stream;

                loop {
                    // re-take the stream lock temporarily, so it won't block writes
                    let entries: Vec<_> = {
                        stream
                            .lock()
                            .await
                            .entries
                            .range(range.clone())
                            .take(count)
                            .map(|(_, entry)| entry.clone())
                            .collect()
                    };

                    if !entries.is_empty() {
                        // there are data to return, so execute the rest sync. (even with BLOCK)
                        unblock.notify_waiters();
                        break Ok((k, Some((key, entries))));
                    }

                    tokio::select! {
                        // wait for additional XADD writes and re-try
                        _ = write.notified(), if block => continue,
                        // ignore BLOCK if some other stream yielded a result
                        _ = unblock.notified(), if block => break Ok((k, None)),
                        // without BLOCK, return after single try
                        else => break Ok((k, None)),
                    }
                }
            });
        }

        let timeout = tokio::time::sleep(timeout);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout, if block => break,
                result = tasks.join_next() => match result {
                    Some(Ok(Ok((k, result)))) => {
                        success |= result.is_some();
                        streams[k] = result;
                    },
                    Some(Ok(Err(e))) => return Err(e),
                    Some(Err(e)) => eprintln!("XREAD failed with {e:?}"),
                    None => break,
                }
            }
        }

        Ok(if success {
            Some(streams.into_iter().flatten().collect())
        } else {
            None
        })
    }

    pub async fn xlen(&self, key: rdb::String) -> Result<usize, Error> {
        let store = self.0.read().await;
        let guard = store.db().await;
        match guard.db.get(&key) {
            Some(ValueCell {
                data: rdb::Value::Stream(s),
                ..
            }) => Ok(s.lock().await.length),
            Some(_) => Err(Error::WrongType),
            None => Ok(0),
        }
    }
}

impl From<rdb::RDB> for Store {
    fn from(rdb::RDB { dbs, .. }: rdb::RDB) -> Self {
        Self(RwLock::new(StoreInner {
            ix: Database::DEFAULT,
            dbs: dbs
                .into_iter()
                .map(|(ix, db)| (ix, Mutex::new(db)))
                .collect(),
        }))
    }
}

trait Expired {
    fn expired_before(&self, t: SystemTime) -> bool;
}

impl Expired for ValueCell {
    #[inline]
    fn expired_before(&self, t: SystemTime) -> bool {
        self.expiry.map_or(false, |expiry| expiry < t)
    }
}

impl Expired for Entry<'_, rdb::String, ValueCell> {
    #[inline]
    fn expired_before(&self, t: SystemTime) -> bool {
        match self {
            Self::Occupied(e) => e.expired_before(t),
            Self::Vacant(e) => e.expired_before(t),
        }
    }
}

impl Expired for OccupiedEntry<'_, rdb::String, ValueCell> {
    #[inline]
    fn expired_before(&self, t: SystemTime) -> bool {
        self.get().expired_before(t)
    }
}

impl Expired for VacantEntry<'_, rdb::String, ValueCell> {
    #[inline]
    fn expired_before(&self, _: SystemTime) -> bool {
        false
    }
}
