use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::ops::Operations;
use crate::service::StateService;
use crate::{Shelf, SyncState, Workspace};
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::*;
use ebi_types::shelf::ShelfInfo;
use ebi_types::tag::{Tag, TagId};
use ebi_types::workspace::WorkspaceInfo;
use ebi_types::{ImmutRef, SharedRef, StatefulRef};
use ebi_types::{Ref, StatefulMap, SwapRef, Uuid};
use redb::{self, Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};

pub type EntityId = Uuid;
pub type SyncStateId = Uuid;
pub type WorkStateId = Uuid;

// note: sync_state_id is equivalent to the permanent id of a workspace
// sync_state_id -> list of workspace id + status
pub const T_SYNC_STATE: TableDefinition<SyncStateId, Vec<Bincode<WorkspaceState>>> =
    TableDefinition::new("version_states");

// workspace state id -> workspace
pub const T_WORKSPACE: TableDefinition<WorkStateId, Bincode<StatefulRef<Workspace>>> =
    TableDefinition::new("workspace");

// shelf entity id -> shelf
pub const T_SHELF: TableDefinition<EntityId, Bincode<ImmutRef<Shelf>>> =
    TableDefinition::new("shelf");

// tag entity id -> tag
pub const T_TAG: TableDefinition<EntityId, Bincode<SharedRef<Tag>>> = TableDefinition::new("tag");

// workspace state id -> list of owned entities
pub const T_WORKSPACE_ENTITIES: TableDefinition<WorkStateId, Vec<Bincode<Entity>>> =
    TableDefinition::new("workspace_entities");

// database entity id -> list of workspace state ids
pub const T_ENTITY_WORKSPACES: TableDefinition<EntityId, Vec<WorkStateId>> =
    TableDefinition::new("entity_workspaces");

// permanent id -> database entity
pub const T_ENTITY: TableDefinition<Uuid, Vec<Bincode<Entity>>> = TableDefinition::new("entity");

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub kind: EntityKind,
    pub seq: u64, // Sequence counter, incremented for every edit
}

impl Entity {
    pub fn next(id: EntityId, kind: EntityKind, history: &[Bincode<Entity>]) -> Self {
        let seq = history.iter().map(|e| e.0.seq).max().map_or(0, |m| m + 1);
        Entity { id, kind, seq }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum EntityKind {
    Shelf,
    Tag,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum StateStatus {
    Staged,
    Synced,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceState {
    pub id: WorkStateId,
    pub status: StateStatus,
}

impl WorkspaceState {
    pub fn new(id: Uuid, status: StateStatus) -> Self {
        WorkspaceState { id, status }
    }
}

impl Storable for WorkspaceState {
    type Storable = Self;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(self.clone())
    }
}

impl Storable for Entity {
    type Storable = Self;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(self.clone())
    }
}

impl StateService {
    pub fn full_load(db_path: &PathBuf) -> Result<Self, ReturnCode> {
        let db = Database::create(db_path).map_err(|_| ReturnCode::DbOpenError)?;
        let read_txn = db.begin_read().unwrap();
        let tables = match (
            read_txn.open_table(T_SYNC_STATE),
            read_txn.open_table(T_WORKSPACE),
            read_txn.open_table(T_WORKSPACE_ENTITIES),
        ) {
            (Ok(sync_state_t), Ok(workspaces_t), Ok(workspace_entities)) => {
                Some((sync_state_t, workspaces_t, workspace_entities))
            }
            (
                Err(redb::TableError::TableDoesNotExist(_)),
                Err(redb::TableError::TableDoesNotExist(_)),
                Err(redb::TableError::TableDoesNotExist(_)),
            ) => None,
            _ => return Err(ReturnCode::DbTableOpenError),
        };
        let Some((sync_state_t, workspaces_t, workspace_entities)) = tables else {
            return Ok(Self {
                sync_states: Arc::new(papaya::HashSet::new()),
                shelves: Arc::new(papaya::HashSet::new()),
                db: Arc::new(db),
            });
        };

        let mut raw_tags = HashMap::new();
        // A workspace without tags (e.g., a new one) has no T_TAG table
        match read_txn.open_table(T_TAG) {
            Ok(tags_t) => {
                for (k, v) in (tags_t
                    .range::<Uuid>(..)
                    .map_err(|_| ReturnCode::InternalStateError)?)
                .flatten()
                {
                    let v = v.value().0; // access TagStorable
                    let k = k.value();
                    raw_tags.insert(k, v);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(_) => return Err(ReturnCode::DbTableOpenError),
        }

        let mut raw_shelves = HashMap::new();
        // A workspace without shelves (e.g., a new one) has no T_SHELF table
        match read_txn.open_table(T_SHELF) {
            Ok(shelves_t) => {
                for (k, v) in (shelves_t
                    .range::<Uuid>(..)
                    .map_err(|_| ReturnCode::InternalStateError)?)
                .flatten()
                {
                    let v = v.value().0;
                    let k = k.value();
                    raw_shelves.insert(k, v);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(_) => return Err(ReturnCode::DbTableOpenError),
        }

        let mut raw_workspaces = HashMap::new();
        for (k, v) in (workspaces_t
            .range::<Uuid>(..)
            .map_err(|_| ReturnCode::InternalStateError)?)
        .flatten()
        {
            let v = v.value().0;
            let k = k.value();
            raw_workspaces.insert(k, v);
        }

        let shelf_refs = papaya::HashSet::new();
        let sync_states = papaya::HashSet::new();
        for (st_id, wks) in (sync_state_t
            .range::<Uuid>(..)
            .map_err(|_| ReturnCode::InternalStateError)?)
        .flatten()
        {
            let st_id = st_id.value();
            let wks = wks.value();
            let mut synced: Option<ImmutRef<Workspace>> = None;
            let mut staged: Option<StatefulRef<Workspace>> = None;

            for wk in wks.iter() {
                let wk = &wk.0;
                let wk_s_id = wk.id;
                let wk_status = &wk.status;

                let raw_wk = raw_workspaces.get(&wk_s_id).unwrap();

                let mut tags = im::HashMap::new();

                let mut shelves = im::HashMap::new();

                let entities = workspace_entities.get(wk_s_id).unwrap().unwrap().value();

                let mut newest: HashMap<Uuid, &Entity> = HashMap::new();
                for entity in entities.iter() {
                    let entity = &entity.0;
                    let permanent_id = match entity.kind {
                        EntityKind::Shelf => raw_shelves.get(&entity.id).unwrap().id,
                        EntityKind::Tag => raw_tags.get(&entity.id).unwrap().id,
                    };
                    newest
                        .entry(permanent_id)
                        .and_modify(|cur| {
                            if entity.seq > cur.seq {
                                *cur = entity;
                            }
                        })
                        .or_insert(entity);
                }

                for (permanent_id, entity) in newest.iter() {
                    match entity.kind {
                        EntityKind::Shelf => {
                            let raw_shelf = raw_shelves.get(&entity.id).unwrap().clone();
                            let shelf = Shelf::new(
                                raw_shelf.root,
                                raw_shelf.name.get().clone(),
                                raw_shelf.shelf_type,
                                raw_shelf.shelf_owner,
                                Some(raw_shelf.config),
                                raw_shelf.description.get().clone(),
                            );
                            shelf.info.rcu(|info| ShelfInfo {
                                name: raw_shelf.name.clone(),
                                description: raw_shelf.description.clone(),
                                root: info.root.clone(),
                            });
                            shelf
                                .filter_tags
                                .store(raw_shelf.filter_tags.clone().into());
                            let shelf: ImmutRef<Shelf> = ImmutRef::new_ref(*permanent_id, shelf);
                            shelf_refs.pin().insert(shelf.downgraded());
                            shelves.insert(*permanent_id, shelf);
                        }
                        EntityKind::Tag => {
                            let raw_tag = raw_tags.get(&entity.id).unwrap().clone();
                            let tag = Tag {
                                name: raw_tag.name,
                                priority: raw_tag.priority,
                                parent: raw_tag.parent,
                            };
                            tags.insert(*permanent_id, SharedRef::new_ref(*permanent_id, tag));
                        }
                    }
                }

                let lookup: im::HashMap<String, TagId> = raw_wk.lookup.clone().into();
                let w_info = WorkspaceInfo {
                    name: raw_wk.name.clone(),
                    description: raw_wk.description.clone(),
                };
                let workspace = Workspace {
                    info: StatefulRef::new_ref((), w_info),
                    shelves: StatefulMap::from_hmap(shelves, SwapRef::new_ref((), ())),
                    tags: StatefulMap::from_hmap(tags, SwapRef::new_ref((), ())),
                    lookup: StatefulMap::from_hmap(lookup, SwapRef::new_ref((), ())),
                };

                if *wk_status == StateStatus::Staged {
                    let workspace = StatefulRef::new_ref(wk.id, workspace);
                    staged = Some(workspace)
                } else {
                    let workspace = ImmutRef::new_ref(wk.id, workspace);
                    synced = Some(workspace)
                }
            }

            let sync_state = SyncState {
                staged: staged.unwrap(),
                committed: SharedRef::new_ref((), Operations::new()),
                synced,
            };

            sync_states
                .pin()
                .insert(SharedRef::new_ref(st_id, sync_state));
        }

        Ok(Self {
            sync_states: Arc::new(sync_states),
            shelves: Arc::new(shelf_refs),
            db: Arc::new(db),
        })
    }
}

#[cfg(test)]
mod tests {
    use ebi_types::sharedref::*;
    use std::str::FromStr;
    use std::sync::Arc;

    use ebi_types::NodeId;
    use papaya::HashSet;

    use crate::{SyncState, Workspace, service::StateService};

    const TEST_PKEY: &str = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6";

    fn assert_eq_sync_states(
        ss_left: &Arc<HashSet<SharedRef<SyncState<Workspace>>>>,
        ss_right: &Arc<HashSet<SharedRef<SyncState<Workspace>>>>,
    ) {
        let ss_left_pin = ss_left.pin();
        let ss_right_pin = ss_right.pin();
        let mut left_sync_states: Vec<_> = ss_left_pin.iter().collect();
        let mut right_sync_states: Vec<_> = ss_right_pin.iter().collect();
        left_sync_states.sort_by_key(|s| s.id);
        right_sync_states.sort_by_key(|s| s.id);

        assert_eq!(left_sync_states, right_sync_states);
        let sync_states_zipped = left_sync_states.iter().zip(right_sync_states.iter());

        for (ss_l, ss_r) in sync_states_zipped {
            assert_eq!(ss_l.id, ss_r.id);
            let staged_w_l = ss_l.load().staged.load();
            let staged_w_r = ss_r.load().staged.load();
            assert_eq_workspace(&staged_w_r, &staged_w_l);
            let synced_w_l = &ss_l.load().synced;
            let synced_w_r = &ss_r.load().synced;
            if let Some(synced_w_l) = synced_w_l
                && let Some(synced_w_r) = synced_w_r
            {
                assert_eq_workspace(synced_w_l, synced_w_r);
            } else {
                assert_eq!(*synced_w_l, None);
                assert_eq!(*synced_w_r, None);
            }
        }
    }

    fn assert_eq_workspace(w_l: &Workspace, w_r: &Workspace) {
        assert_eq!(w_l.info.load_full(), w_r.info.load_full());
        // first assert keys only (statefulref eq impl)
        assert_eq!(w_l.lookup, w_r.lookup);
        assert_eq!(w_l.shelves, w_r.shelves);
        assert_eq!(w_l.tags, w_r.tags);

        let mut l_shelves: Vec<_> = w_l.shelves.iter().collect();
        let mut r_shelves: Vec<_> = w_r.shelves.iter().collect();
        l_shelves.sort_by_key(|(id, _)| *id);
        r_shelves.sort_by_key(|(id, _)| *id);
        for (s_l, s_r) in l_shelves.iter().zip(r_shelves.iter()) {
            let s_l = s_l.1;
            let s_r = s_r.1;
            assert_eq!(s_l.shelf_type, s_r.shelf_type);
            assert_eq!(s_l.shelf_owner, s_r.shelf_owner);
            // filter tags is not checked, as it is simply (de)serialized
            // and encoded to bytes
            assert_eq!(s_l.config, s_r.config);
            assert_eq!(s_l.info.load_full(), s_r.info.load_full());
        }
        let mut l_tags: Vec<_> = w_l.tags.iter().collect();
        let mut r_tags: Vec<_> = w_r.tags.iter().collect();
        l_tags.sort_by_key(|(id, _)| *id);
        r_tags.sort_by_key(|(id, _)| *id);
        for (t_l, t_r) in l_tags.iter().zip(r_tags.iter()) {
            let (t_l, t_r) = (t_l.1.load_full(), t_r.1.load_full());
            assert_eq!(t_l.parent, t_r.parent);
            assert_eq!(t_l.name, t_r.name);
            assert_eq!(t_l.priority, t_r.priority);
        }
    }

    #[tokio::test]
    async fn save_restore_db() {
        let test_path = std::env::temp_dir().join("ebi-state");
        let test_path = test_path.join("save-restore-db");
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let _ = std::fs::remove_file(&db_path);
        let state_service = StateService::new(&db_path).unwrap();

        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let wk_desc = "none".to_string();

        //[/] Empty Database (No Workspaces)
        let saved_sync_states = state_service.sync_states.clone();
        drop(state_service);

        let mut state_service = StateService::full_load(&db_path).unwrap();
        let loaded_sync_states = state_service.sync_states.clone();

        assert_eq_sync_states(&saved_sync_states, &loaded_sync_states);

        //[/] Empty Workspace (No Tags & No Shelves) 
        let wk_e_name = "workspace_empty".to_string();
        let _ = state_service
            .create_workspace(wk_e_name, wk_desc.clone())
            .await
            .unwrap();

        let saved_sync_states = state_service.sync_states.clone();
        drop(state_service);

        let mut state_service = StateService::full_load(&db_path).unwrap();
        let loaded_sync_states = state_service.sync_states.clone();

        assert_eq_sync_states(&saved_sync_states, &loaded_sync_states);

        //[/] Nominal Case 
        let wk_0_name = "workspace0".to_string();
        let wk_0_id = state_service
            .create_workspace(wk_0_name, wk_desc.clone())
            .await
            .unwrap();
        let wk_1_name = "workspace1".to_string();
        let wk_1_id = state_service
            .create_workspace(wk_1_name, wk_desc)
            .await
            .unwrap();
        let mut wk_0 = state_service.workspace(wk_0_id);
        let mut wk_1 = state_service.workspace(wk_1_id);

        let s_0_id = wk_0
            .assign_shelf(test_path.clone(), node_id, false, None, None)
            .await
            .unwrap();
        let _s_1_id = wk_1
            .assign_shelf(
                test_path.parent().unwrap().to_path_buf(),
                node_id,
                true,
                None,
                None,
            )
            .await
            .unwrap();

        let tag_name = "tag_name".to_string();
        let t_1 = wk_0.create_tag(12, tag_name.clone(), None).await.unwrap();
        let _ = wk_0
            .create_tag(2, "child_tag_name".to_string(), Some(t_1))
            .await
            .unwrap();
        let _ = wk_1.create_tag(0, tag_name.clone(), None).await.unwrap();

        let saved_sync_states = state_service.sync_states.clone();
        drop(state_service);
        drop(wk_0);
        drop(wk_1);

        let mut state_service = StateService::full_load(&db_path).unwrap();
        let loaded_sync_states = state_service.sync_states.clone();

        assert_eq_sync_states(&saved_sync_states, &loaded_sync_states);

        //[/] Edited Workspace 
        //state_service.sync_state(crate::CRDT).await;
        state_service.remove_workspace(wk_1_id).await.unwrap();
        state_service
            .workspace(wk_0_id)
            .unassign_shelf(s_0_id)
            .await
            .unwrap();
        state_service
            .workspace(wk_0_id)
            .edit_workspace_info("new_name".to_string(), "new_desc".to_string())
            .await
            .unwrap();

        let saved_sync_states = state_service.sync_states.clone();
        drop(state_service);

        let state_service = StateService::full_load(&db_path).unwrap();
        let loaded_sync_states = state_service.sync_states.clone();

        assert_eq_sync_states(&saved_sync_states, &loaded_sync_states);

        //[/] No-Op 
        //state_service.sync_state(crate::CRDT).await;
        let saved_sync_states = state_service.sync_states.clone();

        drop(state_service);

        let state_service = StateService::full_load(&db_path).unwrap();
        let loaded_sync_states = state_service.sync_states.clone();

        assert_eq_sync_states(&saved_sync_states, &loaded_sync_states);

        let _ = std::fs::remove_file(&db_path);
    }
}
