use arc_swap::{ArcSwap, AsRaw, Guard};
use tokio::sync::RwLock;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use uuid::Uuid;
use std::future::Future;

pub trait Ref<T> {
    fn new_ref(data: T) -> Self;
    fn new_ref_id(id: Uuid, data: T) -> Self;
}

pub type ImmutRef<T> = Arc<Inner<T>>;
pub type SharedRef<T> = Arc<Inner<ArcSwap<T>>>;

impl<T> Ref<T> for ImmutRef<T> {
    fn new_ref(data: T) -> Self {
        let id = Uuid::new_v4();
        Arc::new(Inner { id, data })
    }
    fn new_ref_id(id: Uuid, data: T) -> Self {
        Arc::new(Inner { id, data })
    }
}

impl<T> Ref<T> for SharedRef<T> {
    fn new_ref(data: T) -> Self {
        let id = Uuid::new_v4();
        let data = ArcSwap::new(Arc::new(data));
        Arc::new(Inner { id, data })
    }
    fn new_ref_id(id: Uuid, data: T) -> Self {
        let data = ArcSwap::new(Arc::new(data));
        Arc::new(Inner { id, data })
    }
}

#[derive(Debug, Clone)]
pub struct Inner<T> where 
{
    pub id: Uuid,
    data: T
}

impl<T> PartialEq for Inner<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Hash for Inner<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T> Eq for Inner<T> {}

impl<T> Deref for Inner<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub struct History<T> where
{
    pub staged: StatefulRef<T>,
    pub synced: Option<StatefulRef<T>>,
    pub hist: VecDeque<StatefulRef<T>>
}

const HIST_L: usize = 3;

// Lock type
type L = ();

impl<T> History<T> {
    pub fn new(val: T, lock: Arc<RwLock<L>>) -> Self {
        let first = StatefulRef::new(val, lock);
        History {
            staged: first,
            synced: None,
            hist: VecDeque::new()
        }
    }

    pub fn next(&self) -> Self {
        let mut hist = self.hist.clone();
        if hist.len() == 3 {
            hist.pop_back();
        }
        if let Some(synced) = &self.synced {
            hist.push_front(synced.clone());
        }
        History {
            staged: StatefulRef {
                id: Uuid::new_v4(),
                data: ArcSwap::new(self.staged.load_full()),
                s_lock: self.staged.s_lock.clone()
            },
            synced: Some(self.staged.clone()),
            hist
        }
    }
}

#[derive(Debug)]
pub struct StatefulRef<T> where
{
    pub id: Uuid,
    data: ArcSwap<T>,
    s_lock: Arc<RwLock<L>>,
}
impl<T> Clone for StatefulRef<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            data: ArcSwap::new(self.data.load_full()),
            s_lock: self.s_lock.clone()
        }
    }
}


impl<T> StatefulRef<T> {
    pub fn new(val: T, s_lock: Arc<RwLock<L>>) -> Self {
        StatefulRef {
            id: Uuid::new_v4(),
            data: ArcSwap::new(Arc::new(val)),
            s_lock
        }
    }

    pub fn new_with_id(id: Uuid, val: T, s_lock: Arc<RwLock<L>>) -> Self {
        StatefulRef {
            id,
            data: ArcSwap::new(Arc::new(val)),
            s_lock,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn id_ref(&self) -> &Uuid {
        &self.id
    }

    pub fn load(&self) -> Guard<Arc<T>> {
        self.data.load()
    }
    pub fn load_full(&self) -> Arc<T> {
        self.data.load_full()
    }
    pub async fn stateful_rcu<R, F, O>(&self, mut f: F) -> Arc<T>
    where
        F: FnMut(&Arc<T>) -> (R, Pin<Box<dyn Future<Output = O> + Send>>),
        R: Into<Arc<T>>,
    {
        let mut cur = self.data.load();
        loop {
            let (new, update_state) = f(&cur);
            let new = new.into();
            let prev = self.data.compare_and_swap(&*cur, new);
            let swapped = ptr_eq(&*cur, &*prev);
            if swapped {
                update_state.await;
                return Guard::into_inner(prev);
            } else {
                cur = prev;
            }
        }
    }
}

fn ptr_eq<Base, A, B>(a: A, b: B) -> bool
where
    A: AsRaw<Base>,
    B: AsRaw<Base>,
{
    let a = a.as_raw();
    let b = b.as_raw();
    ptr::eq(a, b)
}

impl<T> PartialEq for StatefulRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Hash for StatefulRef<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T> Eq for StatefulRef<T> {}
