use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::service::state::{GroupState, StateDatabase};
use crate::{Shelf, Tag, Workspace};
use ebi_proto::rpc::ReturnCode;
use ebi_types::redb::*;
use ebi_types::tag::TagId;
use ebi_types::workspace::WorkspaceInfo;
use ebi_types::{History, Ref, StatefulMap, SwapRef, Uuid};
use ebi_types::{ImmutRef, SharedRef, StatefulRef};
use redb::{self, Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

pub type EntityId = Uuid; // does not correspond to ShelfId or WorkspaceId, but db-only Id
pub type GroupStateId = Uuid;

pub const T_WKSPC: TableDefinition<EntityId, Bincode<StatefulRef<Workspace>>> =
    TableDefinition::new("workspace");
pub const T_SHELF: TableDefinition<EntityId, Bincode<ImmutRef<Shelf>>> =
    TableDefinition::new("shelf");
pub const T_TAG: TableDefinition<TagId, Bincode<SharedRef<Tag>>> = TableDefinition::new("tag");
pub const T_STATE_STATUS: TableDefinition<GroupStateId, Bincode<StateStatus>> =
    TableDefinition::new("state_status");

// needed for edit. find table stored workspace or shelf by (state_id, entityId) -> db_workspace_id
pub const T_ENTITY_STATE: TableDefinition<GroupStateId, Bincode<HashMap<EntityId, (Uuid, bool)>>> =
    TableDefinition::new("entity_state");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateStatus {
    Staged,
    Synced,
    History(u64),
}

impl Storable for StateStatus {
    type Storable = Self;

    fn to_storable(&self) -> Bincode<Self> {
        Bincode(self.clone())
    }
}

fn setup_tags(raw_tags: HashMap<Uuid, TagStorable>) -> HashMap<Uuid, SharedRef<Tag>> {
    let mut tag_refs = HashMap::new();
    fn setup_tag(
        raw_tags: &HashMap<Uuid, TagStorable>,
        tag_refs: &mut HashMap<Uuid, SharedRef<Tag>>,
        id: Uuid,
        tag_raw: TagStorable,
    ) -> SharedRef<Tag> {
        let parent = if let Some(p_id) = tag_raw.parent {
            match tag_refs.get(&p_id) {
                Some(p) => Some(p.clone()),
                None => {
                    let tag_raw = raw_tags.get(&p_id).unwrap().clone();
                    Some(setup_tag(raw_tags, tag_refs, p_id, tag_raw))
                }
            }
        } else {
            None
        };
        let tag = Tag {
            name: tag_raw.name,
            priority: tag_raw.priority,
            parent,
        };
        let s_ref = SharedRef::new_ref_id(id, tag);
        tag_refs.insert(id, s_ref.clone());
        s_ref
    }
    for (id, tag_raw) in raw_tags.iter() {
        setup_tag(&raw_tags, &mut tag_refs, *id, tag_raw.clone());
    }
    tag_refs
}

impl StateDatabase {
    pub fn full_load(db_path: &PathBuf) -> Result<Self, ReturnCode> {
        let db = Database::create(db_path).map_err(|_| ReturnCode::DbOpenError)?;
        let read_txn = db.begin_read().unwrap();
        let state_t = read_txn
            .open_table(T_STATE_STATUS)
            .map_err(|_| ReturnCode::DbTableOpenError)?;
        let entitity_t = read_txn
            .open_table(T_ENTITY_STATE)
            .map_err(|_| ReturnCode::DbTableOpenError)?;
        let shelves_t = read_txn
            .open_table(T_SHELF)
            .map_err(|_| ReturnCode::DbTableOpenError)?;
        let workspaces_t = read_txn
            .open_table(T_WKSPC)
            .map_err(|_| ReturnCode::DbTableOpenError)?;
        let tags_t = read_txn
            .open_table(T_TAG)
            .map_err(|_| ReturnCode::DbTableOpenError)?;
        let mut raw_tags = HashMap::new();
        let mut state_ids: Vec<Uuid> = Vec::new();
        for entry in state_t.iter().unwrap() {
            let (s_id, _) = entry.unwrap();
            state_ids.push(s_id.value());
        }

        for (k, v) in (tags_t
            .range::<Uuid>(..)
            .map_err(|_| ReturnCode::InternalStateError)?)
        .flatten()
        {
            let v = v.value().0; // access TagStorable
            let k = k.value();
            raw_tags.insert(k, v);
        }
        let all_tags = setup_tags(raw_tags);

        let mut raw_shelves = HashMap::new();
        for (k, v) in (shelves_t
            .range::<Uuid>(..)
            .map_err(|_| ReturnCode::InternalStateError)?)
        .flatten()
        {
            let v = v.value().0;
            let k = k.value();
            raw_shelves.insert(k, v);
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

        let mut states = HashMap::new();
        for st_id in state_ids.iter() {
            let mut workspaces = im::HashMap::new();
            let mut shelf_refs = im::HashMap::new();
            for (id, wk) in raw_workspaces.iter() {
                let s_map = entitity_t.get(st_id).unwrap().unwrap().value().0;
                if let Some((db_id, _)) = s_map.get(&wk.id)
                    && db_id == id
                {
                    let mut shelves = im::HashMap::new();
                    for s_id in wk.shelves.iter() {
                        let s_db_id = s_map.get(s_id).unwrap().0;
                        let raw_shelf = raw_shelves.get(&s_db_id).unwrap().clone();
                        let shelf = Shelf::new(
                            raw_shelf.root,
                            raw_shelf.name,
                            raw_shelf.shelf_type,
                            raw_shelf.shelf_owner,
                            Some(raw_shelf.config),
                            raw_shelf.description,
                        );
                        shelf
                            .filter_tags
                            .store(raw_shelf.filter_tags.clone().into());
                        let shelf: ImmutRef<Shelf> = ImmutRef::new_ref_id(raw_shelf.id, shelf);
                        shelf_refs.insert(raw_shelf.id, shelf.downgrade());
                        shelves.insert(raw_shelf.id, shelf);
                    }
                    let mut tags = im::HashMap::new();
                    for tag_id in wk.tags.iter() {
                        let tag = all_tags.get(tag_id).unwrap();
                        tags.insert(*tag_id, tag.clone());
                    }
                    let lookup: im::HashMap<String, TagId> = wk.lookup.clone().into();
                    let w_info =
                        WorkspaceInfo::new(Some(wk.name.clone()), Some(wk.description.clone()));
                    let workspace = Workspace {
                        info: StatefulRef::new_ref(w_info),
                        shelves: StatefulMap::from_hmap(shelves, SwapRef::new_ref(())),
                        tags: StatefulMap::from_hmap(tags, SwapRef::new_ref(())),
                        lookup: StatefulMap::from_hmap(lookup, SwapRef::new_ref(())),
                    };
                    let workspace = StatefulRef::new_ref_id(wk.id, workspace);
                    workspaces.insert(wk.id, workspace.clone_inner().into());
                }
            }
            let group_state = GroupState {
                workspaces: StatefulMap::from_hmap(workspaces, SwapRef::new_ref(())),
                shelves: StatefulMap::from_hmap(shelf_refs, SwapRef::new_ref(())),
            };
            let group_state = StatefulRef::new_ref_id(*st_id, group_state);
            states.insert(st_id, group_state);
        }

        let mut hist = History::new(GroupState::new());
        let mut ord = Vec::<(u64, StatefulRef<GroupState>)>::new();

        for entry in state_t.iter().unwrap() {
            let (s_id, s_type) = entry.unwrap();
            let s_id = s_id.value();
            match s_type.value().0 {
                StateStatus::Staged => hist.staged = states.get(&s_id).unwrap().clone_inner(),
                StateStatus::Synced => hist.synced = Some(states.get(&s_id).unwrap().clone_inner()),
                StateStatus::History(v) => ord.push((v, states.get(&s_id).unwrap().clone_inner())),
            }
        }
        ord.sort_by_key(|(v, _)| *v);
        for (_, s) in ord {
            hist.hist.push_front(s);
        }

        Ok(Self {
            state: Arc::new(hist),
            lock: Arc::new(RwLock::new(())),
            db: Arc::new(db),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ebi_types::NodeId;

    use crate::service::state::{GroupState, StateDatabase};

    const TEST_PKEY: &str = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6";

    fn equal_state(gs_left: &GroupState, gs_right: &GroupState) {
        let mut left_wkspcs: Vec<_> = gs_left.workspaces.values().into_iter().collect();
        let mut right_wkspcs: Vec<_> = gs_right.workspaces.values().into_iter().collect();
        left_wkspcs.sort_by_key(|w| w.id);
        right_wkspcs.sort_by_key(|w| w.id);

        let wkspc_zipped = left_wkspcs.iter().zip(right_wkspcs.iter());

        for (w_l, w_r) in wkspc_zipped {
            let w_l = w_l.load();
            let w_r = w_r.load();
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
    }

    #[tokio::test]
    async fn save_restore_db() {
        let test_path = std::env::temp_dir().join("ebi-database");
        let test_path = test_path.join("save-restore-db");
        let _ = std::fs::create_dir_all(test_path.clone());
        let db_path = test_path.join("database.redb");
        let _ = std::fs::remove_file(&db_path);
        let mut state_db = StateDatabase::new(&db_path).unwrap();

        let node_id = NodeId::from_str(TEST_PKEY).unwrap();
        let wk0_name = "workspace0".to_string();
        let wk_desc = "none".to_string();

        let w_0 = state_db
            .create_workspace(wk0_name, wk_desc.clone())
            .await
            .unwrap();
        let wk1_name = "workspace1".to_string();
        let w_1 = state_db.create_workspace(wk1_name, wk_desc).await.unwrap();
        let mut w_0 = state_db.workspace(w_0);
        let mut w_1 = state_db.workspace(w_1);

        let _ = w_0
            .assign_shelf(test_path.clone(), node_id, false, None, None)
            .await
            .unwrap();
        let _ = w_1
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
        let t_1 = w_0.create_tag(12, tag_name.clone(), None).await.unwrap();
        let _ = w_0
            .create_tag(2, tag_name.clone(), Some(t_1))
            .await
            .unwrap();
        let _ = w_1.create_tag(0, tag_name.clone(), None).await.unwrap();

        let hist = state_db.state.clone();
        drop(state_db);
        drop(w_0);
        drop(w_1);

        let loaded_state_db = StateDatabase::full_load(&db_path).unwrap();

        equal_state(
            loaded_state_db.state.staged.load().as_ref(),
            hist.staged.load().as_ref(),
        );

        let _ = std::fs::remove_file(&db_path);
    }
}
