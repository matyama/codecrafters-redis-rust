use std::collections::hash_map::{Entry, OccupiedEntry, VacantEntry};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::SystemTime;
use std::{mem, usize};

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::sync::{Mutex, MutexGuard, Notify, RwLock};
use tokio::task::JoinSet;

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

    // XXX: or take &self and clone both components so we don't have to drop the notify
    #[inline]
    pub(crate) fn into_inner(self) -> (rdb::Value, Option<SystemTime>) {
        (self.data, self.expiry)
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

#[derive(Clone, Debug)]
pub struct Database {
    pub(crate) ix: usize,
    db: HashMap<rdb::String, ValueCell>,
    persist_size: usize,
    expire_size: usize,
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

    /// Returns a pair of the number of keys with and without expiration
    #[inline]
    pub(crate) fn size(&self) -> (usize, usize) {
        (self.persist_size, self.expire_size)
    }

    #[cfg(test)]
    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&rdb::String, &ValueCell)> {
        self.db.iter()
    }

    #[inline]
    pub(crate) fn into_inner(self) -> (usize, HashMap<rdb::String, ValueCell>, usize, usize) {
        (self.ix, self.db, self.persist_size, self.expire_size)
    }

    #[cfg(test)]
    pub(crate) fn remove(&mut self, key: &rdb::String) {
        if let Some(ValueCell { write, expiry, .. }) = self.db.remove(key) {
            if expiry.is_some() {
                self.expire_size -= 1;
            } else {
                self.persist_size -= 1;
            }
            write.notify_waiters();
        }
    }
}

#[derive(Debug, Default)]
pub struct DatabaseBuilder {
    ix: Option<usize>,
    db: Option<HashMap<rdb::String, ValueCell>>,
    size: usize,
    expire_size: usize,
}

impl DatabaseBuilder {
    #[inline]
    pub fn empty() -> Self {
        Self {
            ix: Some(Database::DEFAULT),
            db: Some(HashMap::default()),
            size: 0,
            expire_size: 0,
        }
    }

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
                    if expiry.is_some() {
                        self.expire_size += 1;
                    } else {
                        self.size += 1;
                    }
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
                persist_size: self.size,
                expire_size: self.expire_size,
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
    // FIXME: This is not how Redis handles namespaces (databases).
    //
    // The `SELECT ix` command applies to that particular connection - i.e., the `ix` is a property
    // of the connection and not the whole server. Also, it's defined to be 0-indexed, so a Vec
    // might be more appropriate than a HashMap. Note that currently it's not a big deal as we
    // don't actually support SELECT, so the only database in use is the default one.
    //
    // Where SELECT might get tricky is its atomicity. From the point of view of one client, it
    // should be atomic to swap the selected namespace. So any ongoing operations must see this
    // change immediately. Currently this should be fine, because no write operation re-takes the
    // store lock and commands within the same connection are sequential.
    //
    // XXX: Implementation of this could be that the instance registers a handle for each conn.
    // This handle stores an atomic ptr to the currently selected database and all the store
    // operations go through that handle. The instance than owns the store, which might no longer
    // need a RwLock. Alternatively, the handle could store just the database index.
    ix: usize,
    dbs: HashMap<usize, Mutex<Database>>,
}

impl StoreInner {
    #[inline]
    fn new(dbs: HashMap<usize, Database>) -> Self {
        Self {
            ix: Database::DEFAULT,
            dbs: dbs
                .into_iter()
                .map(|(ix, db)| (ix, Mutex::new(db)))
                .collect(),
        }
    }

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
            persist_size: 0,
            expire_size: 0,
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

    pub(crate) async fn from_rdb(RDBData(data): RDBData) -> Result<Self> {
        let mut reader = DataReader::new(std::io::Cursor::new(data));
        let (rdb, _) = reader.read_rdb_file().await?;
        Ok(Self(RwLock::new(StoreInner::new(rdb.dbs))))
    }

    /// Clones all the contained databases and creates a [`RDB`]
    pub(crate) async fn snapshot<S>(&self, state: S) -> RDB
    where
        S: FnOnce() -> ReplState,
    {
        use rdb::{Aux, String::*};

        // NOTE: intentionally hold an exclusive lock here to create a consistent snapshot
        let store = self.0.write().await;
        let state = state();

        let mut dbs = HashMap::with_capacity(store.dbs.len());

        for db in store.dbs.values() {
            let db = db
                .try_lock()
                .expect("hold an exclusive lock to the whole store");
            dbs.insert(db.ix, db.clone());
        }

        let checksum = Bytes::from_static(b"\xf0n;\xfe\xc0\xffZ\xa2");

        // TODO: set ctime, used_mem and aof_base, and compute checksum
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
                repl_stream_db: Some(Int8(Database::DEFAULT as i8)),
                repl_id: state.repl_id.map(rdb::String::from),
                repl_offset: Some(rdb::String::from(state.repl_offset)),
                aof_base: Some(Int8(0)),
            },
            dbs,
            checksum: Some(checksum),
        }
    }

    // FIXME: keys expire
    pub async fn dbsize(&self) -> (usize, usize) {
        let store = self.0.read().await;
        let guard = store.db().await;
        guard.size()
    }

    // FIXME: keys expire
    // TODO: support patters other than *
    pub async fn keys(&self) -> Vec<rdb::String> {
        let store = self.0.read().await;
        let guard = store.db().await;
        guard.db.keys().cloned().collect()
    }

    // FIXME: keys expire (use get)
    pub async fn ty(&self, key: rdb::String) -> Option<rdb::ValueType> {
        let store = self.0.read().await;
        let guard = store.db().await;
        guard.db.get(&key).map(rdb::ValueType::from)
    }

    // FIXME: keys expire (use get)
    pub async fn contains(&self, key: &rdb::String) -> bool {
        let store = self.0.read().await;
        let guard = store.db().await;
        guard.db.contains_key(key)
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
                    guard.expire_size -= 1;
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

        match guard.db.entry(key) {
            // Overwrite existing entry that has expired before this operation
            Entry::Occupied(mut e) if e.expired_before(now) && matches!(cond, Some(NX) | None) => {
                let expired = e.expired_before(now);
                let expiry = expiration(&e);

                let had_expiry = e.get().expiry.is_some();
                let has_expiry = expiry.is_some();

                let old = e.get_mut().write(val, expiry);

                if had_expiry && !has_expiry {
                    guard.persist_size += 1;
                    guard.expire_size -= 1;
                }

                if !had_expiry && has_expiry {
                    guard.persist_size -= 1;
                    guard.expire_size += 1;
                }

                Ok(if expired { None } else { Some(old) })
            }

            // Overwrite existing entry that has not expired (or has no expiration set)
            Entry::Occupied(mut e) if matches!(cond, Some(XX) | None) => {
                let expiry = expiration(&e);

                let had_expiry = e.get().expiry.is_some();
                let has_expiry = expiry.is_some();

                let value = Some(e.get_mut().write(val, expiry));

                if had_expiry && !has_expiry {
                    guard.persist_size += 1;
                    guard.expire_size -= 1;
                }

                if !had_expiry && has_expiry {
                    guard.persist_size -= 1;
                    guard.expire_size += 1;
                }

                Ok(value)
            }

            // Insert new value (note: we know this is a new entry, so could not be an old one)
            Entry::Vacant(e) if matches!(cond, Some(NX) | None) => {
                // TODO: implement EXAT and PXAT
                if let Some(EX(ttl) | PX(ttl)) = exp {
                    e.insert(ValueCell::new(val, Some(now + ttl)));
                    guard.expire_size += 1;
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

    pub async fn xadd(
        &self,
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
        let mut guard = store.db().await;

        match guard.db.entry(key) {
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
                let Some(stream) = store.lookup_stream(&key, blocking, &unblock).await? else {
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

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use super::*;

    use bytes::Bytes;

    use rdb::{String::*, Value};

    const FOO: Bytes = Bytes::from_static(b"foo");
    const BAR: Bytes = Bytes::from_static(b"bar");
    const BAZ: Bytes = Bytes::from_static(b"baz");

    #[tokio::test]
    async fn simple_set_get() {
        let store = Store::default();

        let value = store.get(Str(FOO)).await;
        assert!(value.is_none(), "cannot get a value from an empty store");

        let value = Value::String(Str(BAR));
        let expected = Some(value.clone());

        let old = store
            .set(Str(FOO), value, set::StoreOptions::default())
            .await
            .expect("new value was set");

        assert!(old.is_none(), "the should be no previous value");

        let acutal = store.get(Str(FOO)).await;
        assert_eq!(expected, acutal);

        assert_eq!((1, 0), store.dbsize().await);
    }

    // TODO: add test for KEEPTTL
    #[tokio::test]
    async fn set_get_with_options() {
        let store = Store::default();

        let value = store.get(Str(FOO)).await;
        assert!(value.is_none(), "cannot get a value from an empty store");

        let value = Value::String(Str(BAR));
        let expected = Some(value.clone());

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::EX(Duration::from_secs(10))),
            cond: Some(set::Condition::NX),
        };

        let old = store
            .set(Str(FOO), value, ops)
            .await
            .expect("new value was set");

        assert!(old.is_none(), "the should be no previous value");

        let acutal = store.get(Str(FOO)).await;
        assert_eq!(expected, acutal);

        let value = Value::String(Str(BAZ));
        let expected = Some(value.clone());

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::PX(Duration::from_millis(200))),
            cond: Some(set::Condition::NX),
        };

        let old = store
            .set(Str(BAR), value, ops)
            .await
            .expect("new value was set");

        assert!(old.is_none(), "the should be no previous value");

        let acutal = store.get(Str(BAR)).await;
        assert_eq!(expected, acutal);

        assert_eq!((0, 2), store.dbsize().await);

        tokio::time::sleep(Duration::from_millis(400)).await;

        let value = store.get(Str(FOO)).await;
        assert!(value.is_some(), "foo has not expired yet");

        // FIXME: before GET bar, its key is still accounted for the expire size
        // assert_eq!((0, 1), store.dbsize().await);

        let value = store.get(Str(BAR)).await;
        assert!(value.is_none(), "bar has expired");

        assert_eq!((0, 1), store.dbsize().await);

        // set bar again on an already expired key - should fail with XX and succeed with NX
        let value = Value::String(Str(BAZ));
        let expected = value.clone();

        let ops = set::StoreOptions {
            exp: None,
            cond: Some(set::Condition::XX),
        };

        store
            .set(Str(BAR), value.clone(), ops)
            .await
            .expect_err("SET with XX should not succeed on an already expired key");

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::PX(Duration::from_millis(200))),
            cond: Some(set::Condition::NX),
        };

        let old = store
            .set(Str(BAR), value.clone(), ops)
            .await
            .expect("new value was set");

        assert!(old.is_none(), "store should not expose expired values");

        let actual = store
            .get(Str(BAR))
            .await
            .expect("SET with NX should succeed on an already expired key");

        assert_eq!(expected, actual);

        assert_eq!((0, 2), store.dbsize().await);

        let ops = set::StoreOptions {
            exp: Some(set::Expiry::KeepTTL),
            cond: Some(set::Condition::XX),
        };

        let actual = store
            .set(Str(BAR), value.clone(), ops)
            .await
            .expect("SET with XX on an existing key should succeed");

        assert_eq!(Some(expected), actual);

        assert_eq!((0, 2), store.dbsize().await);
    }
}
