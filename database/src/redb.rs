use crate::state::GroupState;
use crate::{Shelf, Tag, Workspace};
use ebi_types::StatefulRef;
use ebi_types::Uuid;
use ebi_types::redb::*;
use im::HashMap;
use redb::{
    self, Database, Key, ReadableDatabase, ReadableTable, TableDefinition, TypeName, Value,
};
use serde::{Deserialize, Serialize};

const WKSPC_TABLE: TableDefinition<u128, Bincode<StatefulRef<Workspace>>> =
    TableDefinition::new("workspace");
const ASSGN_TABLE: TableDefinition<Uuid, Uuid> = TableDefinition::new("shelf_assignment");
const SHELF_TABLE: TableDefinition<Uuid, Bincode<StatefulRef<Shelf>>> =
    TableDefinition::new("shelf");
const TAG_TABLE: TableDefinition<Uuid, Bincode<Tag>> = TableDefinition::new("tag");
const HISTORY_TABLE: TableDefinition<u8, Bincode<GroupState>> = TableDefinition::new("history");

// needed for edit. find table stored workspace by (state_id, workspace_id) -> db_workspace_id
const WKSPC_STATE_TABLE: TableDefinition<(u8, Uuid), u128> =
    TableDefinition::new("workspace_state");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupStateStorable {
    workspaces: Vec<u128>,
    shelf_assignment: HashMap<u128, Vec<u128>>,
}

impl Storable for GroupState {
    type Storable = GroupStateStorable;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(GroupStateStorable {
            workspaces: Vec::new(),
            shelf_assignment: HashMap::new(),
        })
    }
}
