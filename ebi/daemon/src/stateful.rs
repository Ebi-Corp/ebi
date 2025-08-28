use std::{hash::Hash, pin::Pin, sync::Arc};
use im::HashMap;
use std::future::Future;
use std::ops::Deref;


pub trait StatefulUpdate<T>
where
{
    type Insert;
    type Remove;
    type Response;
    type Future: Future<Output = Self::Response>;
    fn s_insert(&self, u: Self::Insert) -> (T, Self::Future);
    fn s_remove(&self, r: &Self::Remove) -> (T, Self::Future);
}


#[derive(Debug, Clone, Default)]
pub struct StatefulMap<K, V> where
K: Hash + std::cmp::Eq + Clone,
V: Clone,
{
    map: HashMap<K, V>,
    updater: Arc<()>, // rateless bloom filter
}

impl<K, V> Deref for StatefulMap<K, V> where
K: Hash + std::cmp::Eq + Clone,
V: Clone,
{
    type Target = HashMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K, V> StatefulMap<K, V> where
K: Hash + std::cmp::Eq + Clone,
V: Clone,
{
    pub fn new(updater: Arc<()>) -> Self {
        StatefulMap {
            map: HashMap::new(),
            updater
        }
    }
}


impl<K, V> StatefulUpdate<Self> for StatefulMap<K, V> where
K: Hash + std::cmp::Eq + Clone,
V: Clone,
{
    type Insert = (K, V);
    type Remove = K;
    type Response = ();
    type Future = Pin<Box<dyn Future<Output = Self::Response> + Send>>;

    fn s_insert(&self, u: (K, V)) -> (Self, Self::Future) {
        let u_map = self.map.update(u.0, u.1);
        let u_state = Box::pin(async move {()});
        // [TODO] prepare future for INSERT filter
        let u_s = StatefulMap {
            map: u_map,
            updater: self.updater.clone()
        };
        (u_s, u_state)
    }

    fn s_remove(&self, k: &Self::Remove) -> (Self, Self::Future)
    {
        let u_map = self.map.without(&k);
        let u_filter = Box::pin(async move {()}); 
        // [TODO] update REMOVE filter
        let u_s = StatefulMap {
            map: u_map,
            updater: self.updater.clone()
        };
        (u_s, u_filter)
    }
}
