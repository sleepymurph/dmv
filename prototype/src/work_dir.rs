//! Working Directory: Files checked out from an ObjectStore

use constants::DEFAULT_BRANCH_NAME;
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

#[derive(Debug,Clone,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
pub struct WorkDirState {
    parents: Vec<ObjectKey>,
    branch: Option<String>,
}

impl Default for WorkDirState {
    fn default() -> Self {
        WorkDirState {
            parents: Vec::new(),
            branch: Some(DEFAULT_BRANCH_NAME.to_owned()),
        }
    }
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

    pub fn path(&self) -> &Path { &self.path }

    pub fn branch(&self) -> Option<&str> {
        self.state.branch.as_ref().map(|s| s.as_str())
    }

    pub fn parents(&self) -> &Vec<ObjectKey> { &self.state.parents }

    fn parents_short_hashes(&self) -> Vec<String> {
        self.state
            .parents
            .iter()
            .map(|h| h.to_short())
            .collect::<Vec<String>>()
    }

    pub fn commit(&mut self,
                  message: String)
                  -> Result<(Option<&str>, ObjectKey)> {
        debug!("Current branch: {}. Parents: {}",
               self.branch().unwrap_or("<detached head>"),
               self.parents_short_hashes().join(","));

        let path = self.path().to_owned();
        let tree_hash = self.hash_path(&path)?;
        let commit = Commit {
            tree: tree_hash,
            parents: self.parents().to_owned(),
            message: message,
        };
        let hash = self.store_object(&commit)?;
        self.state.parents = vec![hash];
        if let Some(branch) = self.state.branch.clone() {
            self.update_ref(branch, hash)?;
        }
        self.state.flush()?;
        Ok((self.branch(), hash))
    }

    pub fn update_ref_to_head(&mut self, ref_name: &str) -> Result<ObjectKey> {
        match self.parents().len() {
            0 => {
                bail!("Asked to set ref {} to head, but no current head \
                       (no initial commit)", ref_name)
            }
            1 => {
                let head = self.parents()[0].to_owned();
                self.update_ref(ref_name, head)?;
                Ok(head)
            }
            _ => {
                bail!("Asked to set ref {} to head, but too many parents \
                      (mid-merge). Please select a parent: {}",
                      ref_name, self.parents_short_hashes().join(","))
            }
        }
    }
}

impl_deref_mut!(WorkDir => ObjectFsTransfer, fs_transfer);

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
