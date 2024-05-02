use std::collections::hash_map::{Entry, OccupiedEntry, VacantEntry};
use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::{bail, Result};
use tokio::sync::{Mutex, MutexGuard, RwLock};

use crate::cmd::set;
use crate::rdb;

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
    expiry: Option<SystemTime>,
}

#[derive(Debug)]
pub struct Database {
    pub(crate) ix: usize,
    db: HashMap<rdb::String, ValueCell>,
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
                    let _ = db.insert(key, ValueCell { data: val, expiry });
                }
            }
        }
    }

    #[inline]
    pub fn build(self) -> Result<Database> {
        match (self.ix, self.db) {
            (Some(ix), Some(db)) => Ok(Database { ix, db }),
            (ix, _) => bail!("incomplete db={ix:?}"),
        }
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
                } if *expiry < SystemTime::now() => {
                    e.remove();
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

        let value = match guard.db.entry(key) {
            // Overwrite existing entry that has expired before this operation
            Entry::Occupied(mut e) if e.expired_before(now) && matches!(cond, Some(NX) | None) => {
                let expiry = expiration(&e);
                match e.insert(ValueCell { data: val, expiry }) {
                    value if value.expired_before(now) => Ok(None),
                    ValueCell { data, .. } => Ok(Some(data)),
                }
            }

            // Overwrite existing entry that has not expired (or has no expiration set)
            Entry::Occupied(mut e) if matches!(cond, Some(XX) | None) => {
                let expiry = expiration(&e);
                let ValueCell { data, .. } = e.insert(ValueCell { data: val, expiry });
                Ok(Some(data))
            }

            // Insert new value (note: we know this is a new entry, so could not be an old one)
            Entry::Vacant(e) if matches!(cond, Some(NX) | None) => {
                // TODO: implement EXAT and PXAT
                let expiry = if let Some(EX(ttl) | PX(ttl)) = exp {
                    Some(now + ttl)
                } else {
                    None
                };

                e.insert(ValueCell { data: val, expiry });

                Ok(None)
            }

            // Other cases which don't meet given conditions (NX | XX)
            _ => Err(()),
        };

        value
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
