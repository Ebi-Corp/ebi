use crate::uuid::Uuid;
use arc_swap::{ArcSwap, AsRaw, Guard};
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::{future::Future, ops::Deref, pin::Pin, ptr, sync::Arc};
use tokio::sync::RwLock;

pub type ImmutRef<T, I = Uuid> = Arc<Inner<T, I>>;
pub type SharedRef<T, I = Uuid> = Arc<Inner<ArcSwap<T>, I>>;

pub trait Ref<T, I> {
    fn new_ref(data: T) -> Self;
    fn new_ref_id(id: I, data: T) -> Self;
}

impl<T, I> Ref<T, I> for ImmutRef<T, I>
where
    I: Copy + Default,
{
    fn new_ref(data: T) -> Self {
        let id = I::default();
        Arc::new(Inner { id, data })
    }
    fn new_ref_id(id: I, data: T) -> Self {
        Arc::new(Inner { id, data })
    }
}

impl<T, I> Ref<T, I> for SharedRef<T, I>
where
    I: Copy + Default,
{
    fn new_ref(data: T) -> Self {
        let id = I::default();
        let data = ArcSwap::new(Arc::new(data));
        Arc::new(Inner { id, data })
    }
    fn new_ref_id(id: I, data: T) -> Self {
        let data = ArcSwap::new(Arc::new(data));
        Arc::new(Inner { id, data })
    }
}

#[derive(Debug, Clone)]
pub struct Inner<T, I> {
    pub id: I,
    data: T,
}

impl<T, I> PartialEq for Inner<T, I>
where
    I: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T, I> Hash for Inner<T, I>
where
    I: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T, I> Eq for Inner<T, I> where I: PartialEq {}

impl<T, I> Deref for Inner<T, I> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug)]
pub struct History<T, I = Uuid> {
    pub staged: StatefulRef<T, I>,
    pub synced: Option<StatefulRef<T, I>>,
    pub hist: VecDeque<StatefulRef<T, I>>,
}

const HIST_L: usize = 3;

// Lock type
type L = ();

impl<T, I> History<T, I>
where
    I: Copy + Default,
{
    pub fn new(val: T, lock: Arc<RwLock<L>>) -> Self {
        let first = StatefulRef::new_ref(val, lock);
        History {
            staged: first,
            synced: None,
            hist: VecDeque::new(),
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
                id: I::default(),
                data: ArcSwap::new(self.staged.load_full()),
                s_lock: self.staged.s_lock.clone(),
            },
            synced: Some(self.staged.clone()),
            hist,
        }
    }
}

#[derive(Debug)]
pub struct StatefulRef<T, I = Uuid> {
    pub id: I,
    data: ArcSwap<T>,
    s_lock: Arc<RwLock<L>>,
}
impl<T, I> Clone for StatefulRef<T, I>
where
    I: Copy,
{
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            data: ArcSwap::new(self.data.load_full()),
            s_lock: self.s_lock.clone(),
        }
    }
}

impl<T, I> StatefulRef<T, I>
where
    I: Copy + Default,
{
    pub fn new_ref(val: T, s_lock: Arc<RwLock<L>>) -> Self {
        StatefulRef {
            id: I::default(),
            data: ArcSwap::new(Arc::new(val)),
            s_lock,
        }
    }

    pub fn new_ref_id(id: I, val: T, s_lock: Arc<RwLock<L>>) -> Self {
        StatefulRef {
            id,
            data: ArcSwap::new(Arc::new(val)),
            s_lock,
        }
    }

    pub fn id(&self) -> I
    where
        I: Copy,
    {
        self.id
    }
    pub fn id_ref(&self) -> &I {
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

impl<T, I> PartialEq for StatefulRef<T, I>
where
    I: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T, I> Hash for StatefulRef<T, I>
where
    I: Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl<T, I> Eq for StatefulRef<T, I> where I: PartialEq {}
