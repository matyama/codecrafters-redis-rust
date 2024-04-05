use std::collections::hash_map::{Entry, OccupiedEntry, VacantEntry};
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::cmd::set;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct Key(pub(crate) Bytes);

impl From<Bytes> for Key {
    #[inline]
    fn from(key: Bytes) -> Self {
        Self(key)
    }
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct Value(pub(crate) Bytes);

impl From<Bytes> for Value {
    #[inline]
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<StoredValue> for Value {
    #[inline]
    fn from(StoredValue { data, .. }: StoredValue) -> Self {
        Self(data)
    }
}

#[derive(Debug)]
struct StoredValue {
    data: Bytes,
    expiry: Option<Instant>,
}

// NOTE: The store implementation could use some optimization:
//  1. Using a `Mutex` creates a single contention point which is no good. Also, it's quite
//     unfortunate that `get` does a write as well, which prevents using a `RwLock` here.
//  2. Multiple connections accessing _distinct_ keys still contend on a single (root) lock. It'd
//     be more scalable to use some fine-grained locking (e.g., lock per key, sharding or some
//     tree-like structure instead of a `HashMap`).
//  3. Lock-free implementations might turn out to perform better as well.
#[derive(Debug, Default)]
pub struct Store(Mutex<HashMap<Key, StoredValue>>);

impl Store {
    pub async fn get(self: Arc<Self>, key: Key) -> Option<Value> {
        let mut store = self.0.lock().await;
        let value = match store.entry(key) {
            Entry::Occupied(e) => match e.get() {
                StoredValue {
                    data: _,
                    expiry: Some(expiry),
                } if *expiry < Instant::now() => {
                    e.remove();
                    None
                }
                StoredValue { data, .. } => Some(Value(data.clone())),
            },
            Entry::Vacant(_) => None,
        };
        drop(store);
        value
    }

    pub async fn set(
        self: Arc<Self>,
        key: Key,
        Value(data): Value,
        set::StoreOptions { exp, cond }: set::StoreOptions,
    ) -> Result<Option<Value>, ()> {
        use set::{Condition::*, Expiry::*};

        // XXX: not sure at what point Redis takes the timestamp to determine expiration
        let mut store = self.0.lock().await;
        let now = Instant::now();

        let expiration = |e: &OccupiedEntry<'_, Key, StoredValue>| match exp {
            Some(EX(ttl) | PX(ttl)) => Some(now + ttl),
            Some(EXAT(_) | PXAT(_)) => unimplemented!("SET with EXAT or PXAT isn't supported"),
            Some(KeepTTL) if e.expired_before(now) => None,
            Some(KeepTTL) => e.get().expiry,
            None => None,
        };

        let value = match store.entry(key) {
            // Overwrite existing entry that has expired before this operation
            Entry::Occupied(mut e) if e.expired_before(now) && matches!(cond, Some(NX) | None) => {
                let expiry = expiration(&e);
                match e.insert(StoredValue { data, expiry }) {
                    value if value.expired_before(now) => Ok(None),
                    StoredValue { data, .. } => Ok(Some(data.into())),
                }
            }

            // Overwrite existing entry that has not expired (or has no expiration set)
            Entry::Occupied(mut e) if matches!(cond, Some(XX) | None) => {
                let expiry = expiration(&e);
                let StoredValue { data, .. } = e.insert(StoredValue { data, expiry });
                Ok(Some(data.into()))
            }

            // Insert new value (note: we know this is a new entry, so could not be an old one)
            Entry::Vacant(e) if matches!(cond, Some(NX) | None) => {
                // TODO: implement EXAT and PXAT
                let expiry = if let Some(EX(ttl) | PX(ttl)) = exp {
                    Some(now + ttl)
                } else {
                    None
                };

                e.insert(StoredValue { data, expiry });

                Ok(None)
            }

            // Other cases which don't meet given conditions (NX | XX)
            _ => Err(()),
        };

        drop(store);
        value
    }
}

trait Expired {
    fn expired_before(&self, t: Instant) -> bool;
}

impl Expired for StoredValue {
    #[inline]
    fn expired_before(&self, t: Instant) -> bool {
        self.expiry.map_or(false, |expiry| expiry < t)
    }
}

impl Expired for Entry<'_, Key, StoredValue> {
    #[inline]
    fn expired_before(&self, t: Instant) -> bool {
        match self {
            Self::Occupied(e) => e.expired_before(t),
            Self::Vacant(e) => e.expired_before(t),
        }
    }
}

impl Expired for OccupiedEntry<'_, Key, StoredValue> {
    #[inline]
    fn expired_before(&self, t: Instant) -> bool {
        self.get().expired_before(t)
    }
}

impl Expired for VacantEntry<'_, Key, StoredValue> {
    #[inline]
    fn expired_before(&self, _: Instant) -> bool {
        false
    }
}
