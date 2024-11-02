use std::collections::hash_map::{Entry, OccupiedEntry, VacantEntry};
use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock};
use tokio::task::{JoinHandle, JoinSet};

use crate::cmd::{set, xadd, xrange, xread};
use crate::data::Keys;
use crate::rdb::{self, RDBData, RDB};
use crate::{stream, DataReader, Error, ReplState, REDIS_VER, VERSION};

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

// NOTE: old Tokio does not have `JoinHandle::abort_handle()`, so pretend we have one
#[derive(Debug)]
#[repr(transparent)]
struct AbortHandle(JoinHandle<()>);

impl Deref for AbortHandle {
    type Target = JoinHandle<()>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

trait Abortable {
    fn abort_handle(self) -> AbortHandle;
}

impl Abortable for JoinHandle<()> {
    #[inline]
    fn abort_handle(self) -> AbortHandle {
        AbortHandle(self)
    }
}

struct Expiry {
    /// Time at which a key-value pair should expire
    time: SystemTime,
    /// Abort handle to a background expiration task
    task: AbortHandle,
}

impl Expiry {
    // TODO: SystemTime::now() is not really the same as expire_key -> sleep -> Instant::now()
    // TODO: with options like EXAT|PXAT we probably need to extend ttl to an enum
    fn spawn(key: &rdb::String, now: SystemTime, ttl: Duration, handle: &DBHande) -> Self {
        let task = tokio::task::spawn(expire_key(key.clone(), ttl, Arc::clone(handle)));
        Self {
            time: now + ttl,
            task: task.abort_handle(),
        }
    }

    fn spawn_at(key: &rdb::String, now: SystemTime, exat: SystemTime, handle: &DBHande) -> Self {
        let ttl = exat.duration_since(now).unwrap_or_default();
        Self::spawn(key, now, ttl, handle)
    }
}

impl std::fmt::Debug for Expiry {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Expiry")
            .field("time", &self.time)
            .field("task", &"<AbortHandle>")
            .finish()
    }
}

async fn expire_key(key: rdb::String, ttl: Duration, handle: DBHande) {
    if ttl > Duration::ZERO {
        tokio::time::sleep(ttl).await;
    }

    let mut guard = handle.lock().await;

    let Entry::Occupied(entry) = guard.kvstore.entry(key.clone()) else {
        // entry already removed
        return;
    };

    let Some(expiry) = entry.get().expiry else {
        // entry overwritten with a non-expire entry
        return;
    };

    // XXX: add monotonic `version: usize`
    //  - then if version < {entry|expiry}.version then return early
    //  - this also has the benefit that it does not require SystemTime::now() here

    // TODO: this should be comparing Instants
    if expiry > SystemTime::now() {
        // entry overwritten with new expire, let it handle by new task
        return;
    }

    // actually remove expired entry
    let ValueCell { write, .. } = entry.remove();
    let _ = guard.expires.remove(&key);
    guard.expires_size -= 1;
    write.notify_waiters();
}

#[derive(Clone, Debug)]
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

    /// Returns the contained value and expire timestamp.
    ///
    /// Note that this forgets all sync parts, so only call this method on a database snapshot.
    #[inline]
    pub(crate) fn into_inner(self) -> (rdb::Value, Option<SystemTime>) {
        (self.data, self.expiry)
    }

    /// Replace the content of this cell in-place and notify all write waiters.
    ///
    /// Returns the previous [`Value`](rdb::Value) stored in this cell and its expiration time.
    fn write(
        &mut self,
        mut data: rdb::Value,
        mut expiry: Option<SystemTime>,
    ) -> (rdb::Value, Option<SystemTime>) {
        mem::swap(&mut self.data, &mut data);
        mem::swap(&mut self.expiry, &mut expiry);
        self.write.notify_waiters();
        (data, expiry)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Database {
    pub(crate) id: usize,
    pub(crate) kvstore: HashMap<rdb::String, ValueCell>,
    pub(crate) persist_size: usize,
    pub(crate) expires_size: usize,
}

impl Database {
    #[cfg(test)]
    #[inline]
    pub fn new(id: usize) -> Self {
        Self {
            id,
            kvstore: HashMap::default(),
            persist_size: 0,
            expires_size: 0,
        }
    }

    #[inline]
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::default()
    }

    #[cfg(test)]
    pub(crate) fn remove(&mut self, key: &rdb::String) {
        if let Some(ValueCell { write, expiry, .. }) = self.kvstore.remove(key) {
            if expiry.is_some() {
                self.expires_size -= 1;
            } else {
                self.persist_size -= 1;
            }
            write.notify_waiters();
        }
    }
}

#[derive(Debug, Default)]
pub struct DatabaseBuilder {
    id: Option<usize>,
    db: Option<HashMap<rdb::String, ValueCell>>,
    persist_size: usize,
    expires_size: usize,
}

impl DatabaseBuilder {
    #[inline]
    pub fn empty(id: usize) -> Self {
        Self {
            id: Some(id),
            db: Some(HashMap::default()),
            persist_size: 0,
            expires_size: 0,
        }
    }

    #[inline]
    pub fn with_id(&mut self, id: usize) {
        self.id = Some(id);
    }

    #[inline]
    pub fn with_capacity(&mut self, capacity: usize) {
        self.db = Some(HashMap::with_capacity(capacity));
    }

    #[inline]
    pub fn is_resized(&self) -> bool {
        self.id.is_some() && self.db.is_some()
    }

    pub fn set(&mut self, key: rdb::String, val: rdb::Value, expiry: Option<SystemTime>) {
        if let Some(db) = self.db.as_mut() {
            match expiry {
                // XXX: is now() actually taken per key or at the start of parsing a RDB
                // filter out keys that have already expired
                Some(expiry) if expiry < SystemTime::now() => {}
                expiry => {
                    let _ = db.insert(key, ValueCell::new(val, expiry));
                    if expiry.is_some() {
                        self.expires_size += 1;
                    } else {
                        self.persist_size += 1;
                    }
                }
            }
        }
    }

    #[inline]
    pub fn build(self) -> Result<Database> {
        match (self.id, self.db) {
            (Some(id), Some(kvstore)) => Ok(Database {
                id,
                kvstore,
                persist_size: self.persist_size,
                expires_size: self.expires_size,
            }),
            (id, _) => bail!("incomplete db={id:?}"),
        }
    }
}

#[derive(Debug)]
pub struct DB {
    pub(crate) id: usize,
    kvstore: HashMap<rdb::String, ValueCell>,
    expires: HashMap<rdb::String, Expiry>,
    persist_size: usize,
    expires_size: usize,
    // XXX: replace (also in cells) by single watch (issue: no wait_for requires tokio >= 1.37)
    /// Notify subscribers about recently inserted keys
    new: Arc<Notify>,
}

impl DB {
    #[inline]
    pub fn new(id: usize) -> Self {
        Self {
            id,
            kvstore: HashMap::default(),
            expires: HashMap::default(),
            persist_size: 0,
            expires_size: 0,
            new: Arc::new(Notify::new()),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        (self.persist_size + self.expires_size) == 0
    }

    /// Returns a pair of the number of keys with and without expiration
    #[inline]
    pub(crate) fn size(&self) -> (usize, usize) {
        (self.persist_size, self.expires_size)
    }

    #[inline]
    pub(crate) fn snapshot(&self) -> Database {
        Database {
            id: self.id,
            kvstore: self.kvstore.clone(),
            persist_size: self.persist_size,
            expires_size: self.expires_size,
        }
    }
}

pub(crate) type DBHande = Arc<Mutex<DB>>;

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
//
// XXX: clients could access the store just once at connect and hold an Arc to the selected DB
//  - issue: current snapshot relies on an exclusive lock to this inner sore (DB list)
//  - benefit: all Store operations could be moved to Database as clients could reference them
//    directly (no need to pass the `db` param) and we would no longer have to retake the read lock
#[derive(Debug)]
#[repr(transparent)]
struct StoreInner(Vec<DBHande>);

impl StoreInner {
    #[inline]
    fn new(dbnum: usize) -> Self {
        Self(Vec::from_iter(
            (0..dbnum).map(DB::new).map(Mutex::new).map(Arc::new),
        ))
    }

    fn init(dbs: impl IntoIterator<Item = Database>, dbnum: usize) -> Result<Self> {
        let now = SystemTime::now();
        let mut inner = Self::new(dbnum);

        for db in dbs {
            let handle = Arc::clone(&inner[db.id]);

            let mut guard = inner
                .get_mut(db.id)
                .with_context(|| format!("DB id={} exceeds dbnum={dbnum}", db.id))?
                .try_lock()
                .expect("store was never shared");

            guard.kvstore = db.kvstore;
            guard.persist_size = db.persist_size;
            guard.expires_size = db.expires_size;

            // TODO: store expires in Database, so we don't have to iterate all the keys here
            // NOTE: Strings have stable hash
            #[allow(clippy::mutable_key_type)]
            let expires = guard
                .kvstore
                .iter()
                .filter_map(|(k, v)| v.expiry.map(|t| (k, t)))
                .map(|(key, expiry)| (key.clone(), Expiry::spawn_at(key, now, expiry, &handle)))
                .collect();

            guard.expires = expires;
        }

        Ok(inner)
    }

    async fn get(&self, id: usize) -> MutexGuard<DB> {
        self[id].lock().await
    }
}

impl Deref for StoreInner {
    type Target = [DBHande];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StoreInner {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Store(RwLock<StoreInner>);

impl Store {
    #[inline]
    pub(crate) fn new(dbnum: usize) -> Self {
        Self(RwLock::new(StoreInner::new(dbnum)))
    }

    #[inline]
    pub(crate) fn init(RDB { dbs, .. }: RDB, dbnum: usize) -> Result<Self> {
        StoreInner::init(dbs, dbnum).map(RwLock::new).map(Self)
    }

    pub(crate) async fn from_rdb(RDBData(data): RDBData, dbnum: usize) -> Result<Self> {
        let mut reader = DataReader::new(std::io::Cursor::new(data));
        let (rdb, _) = reader.read_rdb_file().await?;
        let inner = StoreInner::init(rdb.dbs, dbnum)?;
        Ok(Self(RwLock::new(inner)))
    }

    /// Clones all the contained databases and creates a [`RDB`]
    pub(crate) async fn snapshot<S>(&self, state: S) -> RDB
    where
        S: FnOnce() -> ReplState,
    {
        use rdb::{Aux, String::*};

        // NOTE: intentionally hold an exclusive lock here to create a consistent snapshot
        let mut store = self.0.write().await;
        let state = state();

        // NOTE: skip empty databases
        let dbs = store
            .iter_mut()
            .map(|db| db.try_lock().expect("store write locked"))
            .filter_map(|db| {
                if db.id > 0 && db.is_empty() {
                    None
                } else {
                    Some(db.snapshot())
                }
            })
            .collect();

        // TODO: set ctime, used_mem and aof_base
        RDB {
            version: VERSION,
            aux: Aux {
                redis_ver: Some(Str(REDIS_VER)),
                #[cfg(target_pointer_width = "64")]
                redis_bits: Some(Int8(64)),
                #[cfg(target_pointer_width = "32")]
                redis_bits: Some(Int8(32)),
                ctime: None,
                used_mem: None,
                repl_stream_db: Some(Int8(0)),
                repl_id: state.repl_id.map(rdb::String::from),
                repl_offset: Some(rdb::String::from(state.repl_offset)),
                aof_base: Some(Int8(0)),
            },
            dbs,
        }
    }

    pub async fn dbsize(&self, db: usize) -> (usize, usize) {
        let store = self.0.read().await;
        let guard = store.get(db).await;
        guard.size()
    }

    // TODO: support patters other than *
    pub async fn keys(&self, db: usize) -> Vec<rdb::String> {
        let now = SystemTime::now();
        let store = self.0.read().await;
        let guard = store.get(db).await;
        guard
            .kvstore
            .iter()
            .filter_map(|(key, val)| {
                if val.expired_before(now) {
                    None
                } else {
                    Some(key.clone())
                }
            })
            .collect()
    }

    pub async fn ty(&self, db: usize, key: rdb::String) -> Option<rdb::ValueType> {
        self.get(db, key).await.as_ref().map(rdb::ValueType::from)
    }

    pub async fn contains(&self, db: usize, key: &rdb::String) -> bool {
        self.get(db, key.clone()).await.is_some()
    }

    pub async fn get(&self, db: usize, key: rdb::String) -> Option<rdb::Value> {
        let store = self.0.read().await;
        let mut guard = store.get(db).await;
        match guard.kvstore.entry(key) {
            Entry::Occupied(e) => match e.get() {
                ValueCell {
                    data: _,
                    expiry: Some(expiry),
                    ..
                } if *expiry < SystemTime::now() => {
                    let (key, old) = e.remove_entry();
                    guard
                        .expires
                        .remove(&key)
                        .inspect(|Expiry { task, .. }| task.abort());
                    guard.expires_size -= 1;
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
        db: usize,
        key: rdb::String,
        val: rdb::Value,
        set::StoreOptions { exp, cond }: set::StoreOptions,
    ) -> Result<Option<rdb::Value>, ()> {
        use set::{Condition::*, Expiry::*};

        let store = self.0.read().await;
        let handle = &store[db];
        let mut guard = handle.lock().await;

        // XXX: not sure at what point Redis takes the timestamp to determine expiration
        let now = SystemTime::now();

        match guard.kvstore.entry(key.clone()) {
            // Overwrite existing entry that has expired before this operation
            Entry::Occupied(mut e) if e.expired_before(now) && matches!(cond, Some(NX) | None) => {
                let new_expiry = match exp {
                    Some(EX(ttl) | PX(ttl)) => Some(now + ttl),
                    Some(EXAT(_) | PXAT(_)) => unimplemented!("SET with EXAT or PXAT"),
                    Some(KeepTTL) | None => None,
                };

                // old value has expired, so discard it
                let (_, old_expiry) = e.get_mut().write(val, new_expiry);

                match (old_expiry, new_expiry) {
                    (Some(_), Some(exat)) => {
                        let expiry = Expiry::spawn_at(&key, now, exat, handle);
                        if let Some(Expiry { task, .. }) = guard.expires.insert(key, expiry) {
                            task.abort();
                        }
                    }
                    (None, Some(exat)) => {
                        guard.persist_size -= 1;
                        guard.expires_size += 1;
                        let expiry = Expiry::spawn_at(&key, now, exat, handle);
                        let old = guard.expires.insert(key, expiry);
                        debug_assert!(old.is_none(), "unexpected old expiry task");
                    }
                    (Some(_), None) => {
                        guard.persist_size += 1;
                        guard.expires_size -= 1;
                        if let Some(Expiry { task, .. }) = guard.expires.remove(&key) {
                            task.abort();
                        }
                    }
                    (None, None) => {}
                }

                Ok(None)
            }

            // Overwrite existing entry that has not expired (or has no expiration set)
            Entry::Occupied(mut e) if matches!(cond, Some(XX) | None) => {
                let new_expiry = match exp {
                    Some(EX(ttl) | PX(ttl)) => Some(now + ttl),
                    Some(EXAT(_) | PXAT(_)) => unimplemented!("SET with EXAT or PXAT"),
                    Some(KeepTTL) => e.get().expiry,
                    None => None,
                };

                let (old_value, old_expiry) = e.get_mut().write(val, new_expiry);

                match (old_expiry, new_expiry) {
                    // don't unnecessarily abort expiry task on KEEPTTL
                    (Some(_), Some(_)) if matches!(exp, Some(KeepTTL)) => {}
                    (Some(_), Some(exat)) => {
                        let expiry = Expiry::spawn_at(&key, now, exat, handle);
                        if let Some(Expiry { task, .. }) = guard.expires.insert(key, expiry) {
                            task.abort();
                        }
                    }
                    (None, Some(exat)) => {
                        guard.persist_size -= 1;
                        guard.expires_size += 1;
                        let expiry = Expiry::spawn_at(&key, now, exat, handle);
                        let old = guard.expires.insert(key, expiry);
                        debug_assert!(old.is_none(), "unexpected old expiry task");
                    }
                    (Some(_), None) => {
                        guard.persist_size += 1;
                        guard.expires_size -= 1;
                        if let Some(Expiry { task, .. }) = guard.expires.remove(&key) {
                            task.abort();
                        }
                    }
                    (None, None) => {}
                }

                Ok(Some(old_value))
            }

            // Insert new value (note: we know this is a new entry, so could not be an old one)
            Entry::Vacant(e) if matches!(cond, Some(NX) | None) => {
                // TODO: implement EXAT and PXAT
                if let Some(EX(ttl) | PX(ttl)) = exp {
                    e.insert(ValueCell::new(val, Some(now + ttl)));
                    guard.expires_size += 1;
                    guard
                        .expires
                        .entry(key)
                        .or_insert_with_key(|key| Expiry::spawn(key, now, ttl, handle));
                } else {
                    e.insert(ValueCell::new(val, None));
                    guard.persist_size += 1;
                }

                guard.new.notify_waiters();

                Ok(None)
            }

            // Other cases which don't meet given conditions (NX | XX)
            _ => Err(()),
        }
    }

    pub async fn incr(&self, db: usize, key: rdb::String) -> Result<i64, Error> {
        let store = self.0.read().await;
        let mut guard = store.get(db).await;
        match guard.kvstore.entry(key.clone()) {
            Entry::Occupied(mut e) => match e.get() {
                // value has expired, so it's effectively vacant
                ValueCell {
                    expiry: Some(expiry),
                    ..
                } if *expiry < SystemTime::now() => {
                    e.get_mut()
                        .write(rdb::Value::String(rdb::String::from(1)), None);

                    guard.persist_size += 1;
                    guard.expires_size -= 1;

                    guard
                        .expires
                        .remove(&key)
                        .inspect(|Expiry { task, .. }| task.abort());

                    Ok(1)
                }

                // update existing value if it contains an integer
                ValueCell {
                    data: rdb::Value::String(s),
                    expiry,
                    ..
                } => {
                    let expiry = *expiry;
                    let n = i64::try_from(s)?;

                    e.get_mut()
                        .write(rdb::Value::String(rdb::String::from(n + 1)), expiry);

                    Ok(n + 1)
                }

                _ => Err(Error::WrongType),
            },

            Entry::Vacant(e) => {
                let data = rdb::Value::String(rdb::String::from(1));
                e.insert(ValueCell::new(data, None));
                guard.persist_size += 1;
                guard.new.notify_waiters();
                Ok(1)
            }
        }
    }

    pub async fn xadd(
        &self,
        db: usize,
        key: rdb::String,
        entry: stream::EntryArg,
        ops: xadd::Options,
    ) -> XResult<stream::StreamId> {
        if entry.id.is_zero() {
            return Err(Error::err(
                "The ID specified in XADD must be greater than 0-0",
            ));
        }

        let store = self.0.read().await;
        let mut guard = store.get(db).await;

        match guard.kvstore.entry(key) {
            Entry::Occupied(mut e) => {
                let ValueCell { data, write, .. } = e.get_mut();

                let rdb::Value::Stream(s) = data else {
                    return Err(Error::WrongType);
                };

                if ops.no_mkstream {
                    return Ok(None);
                }

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

            Entry::Vacant(_) if ops.no_mkstream => Ok(None),

            Entry::Vacant(e) => {
                let entry = entry.next(None);

                let id = entry.id;
                let last_entry = entry.id;

                let mut entries = BTreeMap::new();
                entries.insert(entry.id, entry);

                let cgroups = vec![];

                let stream = stream::Stream::new(entries, 1, last_entry, cgroups);

                e.insert(ValueCell::new(rdb::Value::Stream(stream), None));
                guard.persist_size += 1;
                guard.new.notify_waiters();

                Ok(Some(id))
            }
        }
    }

    pub async fn xrange(
        &self,
        db: usize,
        key: rdb::String,
        range: xrange::Range,
        count: Option<usize>,
    ) -> XResult<Vec<stream::Entry>> {
        let store = self.0.read().await;
        let guard = store.get(db).await;

        let Some(ValueCell { data, .. }) = guard.kvstore.get(&key) else {
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
        db: usize,
        key: &rdb::String,
    ) -> Result<(Option<(stream::Stream, Arc<Notify>)>, Arc<Notify>), Error> {
        let store = self.0.read().await;
        let guard = store.get(db).await;
        let new = Arc::clone(&guard.new);

        let Some(ValueCell { data, write, .. }) = guard.kvstore.get(key) else {
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
        db: usize,
        key: &rdb::String,
        blocking: bool,
        unblock: &Notify,
    ) -> XResult<StreamHandle> {
        let new = match self.get_stream(db, key).await? {
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
                    if let (Some((stream, write)), _) = self.get_stream(db, key).await? {
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
        db: usize,
        keys: Keys,
        ids: xread::Ids,
        ops: xread::Options,
    ) -> XResult<Vec<(rdb::String, Vec<stream::Entry>)>> {
        if keys.len() != ids.len() {
            let err = "Unbalanced 'xread' list of streams: \
                       for each stream key an ID or '$' must be specified.";
            return Err(Error::err(err));
        }

        let count = ops.count.unwrap_or(usize::MAX);
        let blocking = ops.block.is_some();
        let timeout = ops.block.unwrap_or_default();

        // NOTE: IndexMap would be nice here
        let mut streams = vec![None; keys.len()];
        let mut success = false;

        let mut tasks = JoinSet::new();
        let unblock = Arc::new(Notify::new());

        for (k, (key, id)) in keys.iter().cloned().zip(ids.iter().cloned()).enumerate() {
            let store = Arc::clone(&self);
            let unblock = Arc::clone(&unblock);

            tasks.spawn(async move {
                let Some(stream) = store.lookup_stream(db, &key, blocking, &unblock).await? else {
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
                        _ = write.notified(), if blocking => continue,
                        // ignore BLOCK if some other stream yielded a result
                        _ = unblock.notified(), if blocking => break Ok((k, None)),
                        // without BLOCK, return after single try
                        else => break Ok((k, None)),
                    }
                }
            });
        }

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);

        loop {
            tokio::select! {
                _ = &mut sleep, if !timeout.is_zero() => break,
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

    pub async fn xlen(&self, db: usize, key: rdb::String) -> Result<usize, Error> {
        let store = self.0.read().await;
        let guard = store.get(db).await;
        match guard.kvstore.get(&key) {
            Some(ValueCell {
                data: rdb::Value::Stream(s),
                ..
            }) => Ok(s.lock().await.length),
            Some(_) => Err(Error::WrongType),
            None => Ok(0),
        }
    }
}

#[cfg(test)]
impl Default for Store {
    #[inline]
    fn default() -> Self {
        Self::new(crate::config::DBNUM)
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

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use super::*;

    use bytes::Bytes;

    use rdb::{String::*, Value};

    const DB: usize = 0;

    const FOO: Bytes = Bytes::from_static(b"foo");
    const BAR: Bytes = Bytes::from_static(b"bar");
    const BAZ: Bytes = Bytes::from_static(b"baz");

    #[tokio::test]
    async fn simple_set_get() {
        let store = Store::default();

        let value = store.get(DB, Str(FOO)).await;
        assert!(value.is_none(), "cannot get a value from an empty store");

        let value = Value::String(Str(BAR));
        let expected = Some(value.clone());

        let old = store
            .set(DB, Str(FOO), value, set::StoreOptions::default())
            .await
            .expect("new value was set");

        assert!(old.is_none(), "the should be no previous value");

        let acutal = store.get(DB, Str(FOO)).await;
        assert_eq!(expected, acutal);

        assert_eq!((1, 0), store.dbsize(DB).await);
    }

    // TODO: add test for KEEPTTL
    #[tokio::test]
    async fn set_get_with_options() {
        let store = Store::default();

        let value = store.get(DB, Str(FOO)).await;
        assert!(value.is_none(), "cannot get a value from an empty store");

        let value = Value::String(Str(BAR));
        let expected = Some(value.clone());

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::EX(Duration::from_secs(10))),
            cond: Some(set::Condition::NX),
        };

        let old = store
            .set(DB, Str(FOO), value, ops)
            .await
            .expect("new value was set");

        assert!(old.is_none(), "the should be no previous value");

        let acutal = store.get(DB, Str(FOO)).await;
        assert_eq!(expected, acutal);

        let value = Value::String(Str(BAZ));
        let expected = Some(value.clone());

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::PX(Duration::from_millis(200))),
            cond: Some(set::Condition::NX),
        };

        let old = store
            .set(DB, Str(BAR), value, ops)
            .await
            .expect("new value was set");

        assert!(old.is_none(), "the should be no previous value");

        let acutal = store.get(DB, Str(BAR)).await;
        assert_eq!(expected, acutal);

        assert_eq!((0, 2), store.dbsize(DB).await);

        tokio::time::sleep(Duration::from_millis(400)).await;

        let value = store.get(DB, Str(FOO)).await;
        assert!(value.is_some(), "foo has not expired yet");

        assert_eq!((0, 1), store.dbsize(DB).await);

        let value = store.get(DB, Str(BAR)).await;
        assert!(value.is_none(), "bar has expired");

        assert_eq!((0, 1), store.dbsize(DB).await);

        // set bar again on an already expired key - should fail with XX and succeed with NX
        let value = Value::String(Str(BAZ));
        let expected = value.clone();

        let ops = set::StoreOptions {
            exp: None,
            cond: Some(set::Condition::XX),
        };

        store
            .set(DB, Str(BAR), value.clone(), ops)
            .await
            .expect_err("SET with XX should not succeed on an already expired key");

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::PX(Duration::from_millis(200))),
            cond: Some(set::Condition::NX),
        };

        let old = store
            .set(DB, Str(BAR), value.clone(), ops)
            .await
            .expect("new value was set");

        assert!(old.is_none(), "store should not expose expired values");

        let actual = store
            .get(DB, Str(BAR))
            .await
            .expect("SET with NX should succeed on an already expired key");

        assert_eq!(expected, actual);

        assert_eq!((0, 2), store.dbsize(DB).await);

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::KeepTTL),
            cond: Some(set::Condition::XX),
        };

        let actual = store
            .set(DB, Str(BAR), value.clone(), ops)
            .await
            .expect("SET with XX on an existing key should succeed");

        assert_eq!(Some(expected), actual);

        assert_eq!((0, 2), store.dbsize(DB).await);
    }
}
