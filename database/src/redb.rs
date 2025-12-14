use std::collections::HashMap;

use crate::service::state::GroupState;
use crate::{Shelf, Tag, Workspace};
use ebi_types::Uuid;
use ebi_types::redb::*;
use ebi_types::tag::TagId;
use ebi_types::{ImmutRef, SharedRef, StatefulRef};
use redb::{self, TableDefinition};
use serde::{Deserialize, Serialize};

pub type EntityId = Uuid; // does not correspond to ShelfId or WorkspaceId, but db-only Id
pub type GroupStateId = Uuid;

pub const T_WKSPC: TableDefinition<EntityId, Bincode<StatefulRef<Workspace>>> =
    TableDefinition::new("workspace");
pub const T_SHELF: TableDefinition<EntityId, Bincode<ImmutRef<Shelf>>> =
    TableDefinition::new("shelf");
pub const T_TAG: TableDefinition<TagId, Bincode<SharedRef<Tag>>> = TableDefinition::new("tag");
pub const T_HISTORY: TableDefinition<u8, Bincode<GroupState>> = TableDefinition::new("history");

// needed for edit. find table stored workspace or shelf by (state_id, entityId) -> db_workspace_id
pub const T_ENTITY_STATE: TableDefinition<GroupStateId, Bincode<HashMap<EntityId, (Uuid, bool)>>> =
    TableDefinition::new("entity_state");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStateStorable {
    workspaces: Vec<Uuid>,
    shelves: Vec<Uuid>,
}

impl Storable for GroupState {
    type Storable = GroupStateStorable;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(GroupStateStorable {
            workspaces: Vec::new(),
            shelves: Vec::new(),
        })
    }
}
