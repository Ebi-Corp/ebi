use crate::{Shelf, Tag, Workspace};
use ebi_types::Uuid;
use ebi_types::redb::*;
use redb::{
    self, Database, Key, ReadableDatabase, ReadableTable, TableDefinition, TypeName, Value,
};
use serde::{Deserialize, Serialize};

const WKSPC_TABLE: TableDefinition<Uuid, Bincode<Workspace>> =
    TableDefinition::new("workspace_state");
const ASSGN_TABLE: TableDefinition<Uuid, Uuid> = TableDefinition::new("shelf_assignment");
const SHELF_TABLE: TableDefinition<Uuid, Bincode<Shelf>> = TableDefinition::new("shelf_state");
const TAG_TABLE: TableDefinition<Uuid, Bincode<Tag>> = TableDefinition::new("tag_state");
const HISTORY_TABLE: TableDefinition<u8, Bincode<HistoryState>> =
    TableDefinition::new("history_state");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HistoryState {}

impl Storable for HistoryState {
    type Storable = HistoryState;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(self.clone())
    }
}
