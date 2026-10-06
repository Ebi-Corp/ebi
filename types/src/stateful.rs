use chrono::{DateTime, Utc};
use im::HashMap;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::ops::Deref;
use std::pin::Pin;

use crate::SharedRef;

pub type SwapRef<T> = SharedRef<T, ()>;

//[!] "Wall Time" may not be trustworthy, maybe use a Hybrid Logical Clock ?? 
pub type Revision = DateTime<Utc>;

// A value with a last-edit timestamp, used for LWW 
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Stamped<T> {
    value: T,
    rev: Revision,
}

impl<T> Stamped<T> {
    pub fn new(value: T, at: Revision) -> Self {

        Stamped { value, rev: at }
    }

    pub fn get(&self) -> &T {
        &self.value
    }

    pub fn rev(&self) -> Revision {
        self.rev
    }
}

impl<T: PartialEq> Stamped<T> {
    pub fn set(&self, value: T, at: Revision) -> Self {
        let rev = if value != self.value { at } else { self.rev };
        Stamped { value, rev }
    }
}

impl<T: Clone + Ord> Stamped<T> {
    pub fn merge(&self, other: &Self) -> Self {
        match other.rev.cmp(&self.rev) {
            std::cmp::Ordering::Greater => other.clone(),
            std::cmp::Ordering::Less => self.clone(),
            std::cmp::Ordering::Equal => {
                if other.value > self.value {
                    other.clone()
                } else {
                    self.clone()
                }
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct StatefulMap<K, V>
where
    K: Hash + std::cmp::Eq + Clone,
    V: Clone,
{
    map: HashMap<K, V>,
    state: SwapRef<()>, // rateless bloom filter
}

impl<K, V> Deref for StatefulMap<K, V>
where
    K: Hash + std::cmp::Eq + Clone,
    V: Clone,
{
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, V> StatefulMap<K, V>
where
    K: Hash + std::cmp::Eq + Clone,
    V: Clone,
{
    pub fn new(state: SwapRef<()>) -> Self {
        StatefulMap {
            map: HashMap::new(),
            state,
        }
    }
    pub fn from_hmap(map: HashMap<K, V>, state: SwapRef<()>) -> Self {
        StatefulMap { map, state }
    }

    pub fn insert(&self, key: K, value: V) -> (Self, Pin<Box<dyn Future<Output = ()> + Send>>) {
        let mut updated_map = self.map.clone();
        updated_map.insert(key, value);

        let update_state = Box::pin(async move {});
        // [TODO] prepare future for INSERT filter

        let map = StatefulMap {
            map: updated_map,
            state: self.state.clone(),
        };

        (map, update_state)
    }

    pub fn remove(&self, key: &K) -> (Self, Pin<Box<dyn Future<Output = ()> + Send>>) {
        let mut updated_map = self.map.clone();
        updated_map.remove(key);

        let update_state = Box::pin(async move {});
        // [TODO] update REMOVE filter

        let map = StatefulMap {
            map: updated_map,
            state: self.state.clone(),
        };

        (map, update_state)
    }
}
