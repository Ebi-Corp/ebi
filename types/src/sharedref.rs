use crate::Uuid;
use arc_swap::{ArcSwap, AsRaw};
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Weak;
use std::{future::Future, ops::Deref, pin::Pin, ptr, sync::Arc};

pub type WeakRef<T, I = Uuid> = Inner<Weak<T>, I>;
pub type ImmutRef<T, I = Uuid> = Inner<Arc<T>, I>;
pub type SharedRef<T, I = Uuid> = Inner<Arc<ArcSwap<T>>, I>;
pub type StatefulRef<T, I = Uuid> = Inner<ArcSwap<T>, I>;

pub trait Ref<T, I> {
    fn new_ref(data: T) -> Self;
    fn new_ref_id(id: I, data: T) -> Self;
    fn inner_ptr(&self) -> *const T;
}

impl<T> Ref<T, Uuid> for ImmutRef<T, Uuid> {
    fn new_ref(data: T) -> Self {
        let id = Uuid::new_v4();
        Inner {
            id,
            data: Arc::new(data),
        }
    }
    fn new_ref_id(id: Uuid, data: T) -> Self {
        Inner {
            id,
            data: Arc::new(data),
        }
    }

    fn inner_ptr(&self) -> *const T {
        Arc::as_ptr(&self.data)
    }
}

impl<T> Ref<T, Uuid> for StatefulRef<T, Uuid> {
    fn new_ref(data: T) -> Self {
        let id = Uuid::new_v4();
        Inner {
            id,
            data: ArcSwap::new(Arc::new(data)),
        }
    }
    fn new_ref_id(id: Uuid, data: T) -> Self {
        Inner {
            id,
            data: ArcSwap::new(Arc::new(data)),
        }
    }

    fn inner_ptr(&self) -> *const T {
        Arc::as_ptr(&self.data.load_full())
    }
}

impl<T> Ref<T, Uuid> for SharedRef<T, Uuid> {
    fn new_ref(data: T) -> Self {
        let id = Uuid::new_v4();
        let data = ArcSwap::new(Arc::new(data));
        Inner {
            id,
            data: Arc::new(data),
        }
    }
    fn new_ref_id(id: Uuid, data: T) -> Self {
        let data = ArcSwap::new(Arc::new(data));
        Inner {
            id,
            data: Arc::new(data),
        }
    }
    fn inner_ptr(&self) -> *const T {
        Arc::as_ptr(&self.data.load())
    }
}

impl<T> Ref<T, ()> for SharedRef<T, ()> {
    fn new_ref(data: T) -> Self {
        let data = ArcSwap::new(Arc::new(data));
        Inner {
            id: (),
            data: Arc::new(data),
        }
    }
    fn new_ref_id(id: (), data: T) -> Self {
        let data = ArcSwap::new(Arc::new(data));
        Inner {
            id,
            data: Arc::new(data),
        }
    }

    fn inner_ptr(&self) -> *const T {
        Arc::as_ptr(&self.data.load_full())
    }
}

impl<T, I: Copy> ImmutRef<T, I> {
    pub fn downgrade(&self) -> WeakRef<T, I> {
        Inner {
            id: self.id,
            data: Arc::downgrade(&self.data),
        }
    }
}

impl<T, I: Copy> WeakRef<T, I> {
    pub fn to_upgraded(&self) -> Option<ImmutRef<T, I>> {
        let data = self.data.upgrade()?;
        Some(Inner { id: self.id, data })
    }
}

#[derive(Debug, Clone)]
pub struct Inner<T, I> {
    pub id: I,
    data: T,
}

impl<T, I: Copy> StatefulRef<T, I> {
    pub fn clone_inner(&self) -> Self {
        Self {
            id: self.id,
            data: ArcSwap::new(self.data.load_full()),
        }
    }
    pub fn from_arcswap(id: I, data: ArcSwap<T>) -> Self {
        Self {
            id,
            data
        }
    }
}

impl<T, I> Inner<T, I> {
    pub fn new(id: I, data: T) -> Self {
        Inner { id, data }
    }
    pub fn data_ref(&self) -> &T {
        &self.data
    }
}

impl<T, I> Borrow<I> for ImmutRef<T, I> {
    fn borrow(&self) -> &I {
        &self.id
    }
}

impl<T, I> Borrow<I> for WeakRef<T, I> {
    fn borrow(&self) -> &I {
        &self.id
    }
}

pub trait WithPath {
    fn path(&self) -> PathBuf;
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

impl<T> StatefulRef<T> {
    pub fn stateful_rcu<R, F, O>(&self, mut f: F) -> Pin<Box<dyn Future<Output = O> + Send>>
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
                return update_state;
            } else {
                cur = prev;
            }
        }
    }
}

pub fn ptr_eq<Base, A, B>(a: A, b: B) -> bool
where
    A: AsRaw<Base>,
    B: AsRaw<Base>,
{
    let a = a.as_raw();
    let b = b.as_raw();
    ptr::eq(a, b)
}
