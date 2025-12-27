use chrono::{DateTime, Utc};
use im::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::{any::Any, hash::Hash};

use crate::{Ref, SharedRef};

pub type SwapRef<T> = SharedRef<T, ()>;

pub trait GenericValue: Any + Send + Sync + Debug {
    fn clone_box(&self) -> Box<dyn GenericValue>;
}

impl<T> GenericValue for T
where
    T: Any + Send + Sync + Debug + Clone + 'static,
{
    fn clone_box(&self) -> Box<dyn GenericValue> {
        Box::new(self.clone())
    }
}

type TimedFieldValue = (Arc<dyn GenericValue>, DateTime<Utc>);

#[derive(Debug, Clone)]
pub struct InfoState<K>
where
    K: Clone + Send + Sync + Hash + Eq,
{
    state: SharedRef<HashMap<K, TimedFieldValue>, ()>,
}

impl<K> InfoState<K>
where
    K: Clone + Send + Sync + Hash + Eq,
{
    pub fn new() -> Self {
        InfoState {
            state: SharedRef::new_ref((), HashMap::new()),
        }
    }

    pub fn rcu<F>(&self, mut f: F) -> Arc<HashMap<K, TimedFieldValue>>
    where
        F: FnMut(&HashMap<K, TimedFieldValue>) -> HashMap<K, TimedFieldValue>,
    {
        self.state.rcu(|curr| f(&**curr))
    }

    pub fn entries(&self) -> Vec<(K, TimedFieldValue)> {
        self.state
            .load()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl<K> Default for InfoState<K>
where
    K: Clone + Send + Sync + Hash + Eq,
{
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct StatefulField<K, V>
where
    K: Clone + Send + Sync + Hash + Eq,
    V: Clone + Send + Sync + Default,
{
    field: Arc<K>,
    value: V,
    state: InfoState<K>,
}

impl<K, V> Deref for StatefulField<K, V>
where
    K: Clone + Send + Sync + Hash + Eq,
    V: Clone + Send + Sync + Default,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<K, V> StatefulField<K, V>
where
    K: Clone + Send + Sync + Hash + Eq,
    V: Clone + Send + Sync + Default + GenericValue,
{
    pub fn new(field: K, state: InfoState<K>) -> Self {
        StatefulField {
            field: Arc::new(field),
            value: V::default(),
            state: state.clone(),
        }
    }

    pub fn set<'a>(&self, value: &V) -> (Self, Pin<Box<dyn Future<Output = ()> + Send + 'a>>)
    where
        K: 'a,
        V: 'a,
    {
        let field = self.field.clone();
        let state = self.state.clone();
        let updated_value = value.clone();

        let update_state = Box::pin(async move {
            state.rcu(|update_map| {
                let mut update_map = update_map.clone();
                update_map.insert(
                    (*field).clone(),
                    (Arc::new(updated_value.clone()), Utc::now()),
                );
                update_map
            });
        });

        let updated_field = StatefulField {
            field: self.field.clone(),
            value: value.clone(),
            state: self.state.clone(),
        };

        (updated_field, update_state)
    }

    pub fn get(&self) -> V {
        self.value.clone()
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
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
