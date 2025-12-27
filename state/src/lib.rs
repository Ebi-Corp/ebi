pub mod cache;
pub mod redb;
pub mod service;

use ebi_filesystem::shelf::TagFilter;
use ebi_types::shelf::*;
use ebi_types::workspace::WorkspaceId;
use ebi_types::{Uuid, sharedref::*, stateful::*};
use std::collections::VecDeque;
use std::sync::Arc;

pub type Workspace = ebi_types::workspace::Workspace<TagFilter>;
pub type Shelf = ebi_types::shelf::Shelf<TagFilter>;
pub type ShelfRef = <ImmutRef<Shelf> as Ref<Shelf, Uuid>>::Weak;

#[derive(Debug)]
pub struct StateView {
    pub workspaces: StatefulMap<WorkspaceId, Arc<StatefulRef<Workspace>>>,
    pub shelves: StatefulMap<ShelfId, ShelfRef>,
}

impl Default for StateView {
    fn default() -> Self {
        Self::new()
    }
}

impl StateView {
    pub fn new() -> Self {
        Self {
            workspaces: StatefulMap::new(SwapRef::new_ref((), ())),
            shelves: StatefulMap::new(SwapRef::new_ref((), ())),
        }
    }
}

// [TODO] dummy struct, replace with IBLT/CRDT type
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CRDT;

#[derive(Debug)]
pub struct StateChain {
    pub staged: StatefulRef<StateView>, // Locally modified state - contains uncommitted changes
    pub committed: CRDT, // Committed ops (Broadcasted) but not yet approved by OpChain
    pub received: CRDT,  // Received ops (as broadcast) but not yet approved by OpChain
    pub synced: VecDeque<StatefulRef<StateView>>, // Approved states by OpChain, not seen by all - Clears once every Daemon views the changes
}

const HIST_L: usize = 3;

impl StateChain {
    pub fn new(new_state: StateView) -> Self {
        let first = StatefulRef::new_ref(Uuid::new_v4(), new_state);
        StateChain {
            staged: first,
            committed: CRDT,
            received: CRDT,
            synced: VecDeque::new(),
        }
    }

    pub fn next(&self) -> (Self, Option<Uuid>) {
        let mut past: VecDeque<StatefulRef<StateView>> =
            self.synced.iter().map(|r| r.clone_inner()).collect();
        let mut removed_state = None;
        if past.len() == HIST_L {
            removed_state = Some(past.pop_back().unwrap().id);
        }
        // [TODO] handle committed, received
        past.push_front(self.staged.clone_inner());

        let new_synced = StateChain {
            staged: StatefulRef::from_arcswap(Uuid::new_v4(), self.staged.load_full().into()),
            committed: CRDT,
            received: CRDT,
            synced: past,
        };
        (new_synced, removed_state)
    }

    pub fn from(
        staged: StatefulRef<StateView>,
        committed: CRDT,
        received: CRDT,
        synced: VecDeque<StatefulRef<StateView>>,
    ) -> Self {
        StateChain {
            staged,
            committed,
            received,
            synced,
        }
    }
}
