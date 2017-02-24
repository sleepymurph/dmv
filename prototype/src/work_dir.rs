//! Working Directory: Files checked out from an ObjectStore

use constants::HARDCODED_BRANCH;
use constants::HIDDEN_DIR_NAME;
use dag::Commit;
use dag::ObjectKey;
use disk_backed::DiskBacked;
use error::*;
use find_repo::RepoLayout;
use fs_transfer::ObjectFsTransfer;
use objectstore::ObjectStore;
use rustc_serialize::json;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug,Clone,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable,Default)]
pub struct WorkDirState {
    parents: Vec<ObjectKey>,
    branch: Option<String>,
}

pub struct WorkDir {
    fs_transfer: ObjectFsTransfer,
    path: PathBuf,
    state: DiskBacked<WorkDirState>,
}

fn work_dir_state_path(wd_path: &Path) -> PathBuf {
    wd_path.join(HIDDEN_DIR_NAME).join("work_dir_state")
}


impl WorkDir {
    pub fn init(layout: RepoLayout) -> Result<Self> {
        let os = ObjectStore::init(layout.osd)?;
        let state = DiskBacked::new("work dir state",
                                    work_dir_state_path(&layout.wd));
        Ok(WorkDir {
            fs_transfer: ObjectFsTransfer::with_object_store(os),
            path: layout.wd,
            state: state,
        })
    }
    pub fn open(layout: RepoLayout) -> Result<Self> {
        Ok(WorkDir {
            fs_transfer: ObjectFsTransfer::with_repo_path(layout.osd)?,
            state:
                DiskBacked::read_or_default("work dir state",
                                            work_dir_state_path(&layout.wd))?,
            path: layout.wd,
        })
    }

    pub fn fs_transfer(&mut self) -> &mut ObjectFsTransfer {
        &mut self.fs_transfer
    }
    pub fn object_store(&mut self) -> &mut ObjectStore {
        &mut self.fs_transfer.object_store
    }
    pub fn path(&self) -> &Path { &self.path }

    pub fn commit(&mut self, message: String) -> Result<(&str, ObjectKey)> {
        let parents = match self.object_store()
            .try_find_ref(HARDCODED_BRANCH) {
            Ok(Some(hash)) => vec![hash],
            Ok(None) => vec![],
            Err(e) => bail!(e),
        };
        debug!("Current branch: {}. Parents: {}",
               HARDCODED_BRANCH,
               parents.iter()
                   .map(|h| h.to_short())
                   .collect::<Vec<String>>()
                   .join(","));

        let path = self.path().to_owned();
        let tree_hash = self.fs_transfer().hash_path(&path)?;
        let commit = Commit {
            tree: tree_hash,
            parents: parents,
            message: message,
        };
        let hash = self.object_store().store_object(&commit)?;
        Ok((HARDCODED_BRANCH, hash))
    }
}

mod test {
    use rustc_serialize::json;
    use super::*;

    #[test]
    fn test_serialize_work_dir_state() {
        let mut obj = WorkDirState::default();

        let encoded = json::encode(&obj).unwrap();
        // assert_eq!(encoded, "see encoded");
        let decoded: WorkDirState = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
