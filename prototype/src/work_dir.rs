//! Working Directory: Files checked out from an ObjectStore

use constants::HARDCODED_BRANCH;
use dag::Commit;
use dag::ObjectKey;
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

impl WorkDirState {
    fn new() -> Self {
        WorkDirState {
            parents: Vec::new(),
            branch: None,
        }
    }
}

pub struct WorkDir {
    fs_transfer: ObjectFsTransfer,
    path: PathBuf,
    state: WorkDirState,
    disk_state: WorkDirState,
}

impl WorkDir {
    pub fn init(layout: RepoLayout) -> Result<Self> {
        let os = ObjectStore::init(layout.osd)?;
        let state = WorkDirState::new();
        let mut work_dir = WorkDir {
            fs_transfer: ObjectFsTransfer::with_object_store(os),
            path: layout.wd,
            state: WorkDirState::new(),
            disk_state: WorkDirState::new(),
        };
        work_dir.write_state()?;
        Ok(work_dir)
    }
    pub fn open(layout: RepoLayout) -> Result<Self> {
        let mut work_dir = WorkDir {
            fs_transfer: ObjectFsTransfer::with_repo_path(layout.osd)?,
            path: layout.wd,
            state: WorkDirState::new(),
            disk_state: WorkDirState::new(),
        };
        work_dir.read_state()?;
        Ok(work_dir)
    }

    fn state_file_path(&self) -> PathBuf {
        self.fs_transfer.object_store.path().join(".work_dir_state")
    }

    /// Write state to disk, if it has been updated
    fn flush_state(&mut self) -> Result<()> {
        if self.state == self.disk_state {
            debug!("WorkDir state unchanged");
            Ok(())
        } else {
            self.write_state()
        }
    }

    /// Write state to disk, updated or not
    fn write_state(&mut self) -> Result<()> {
        let path = self.state_file_path();
        debug!("Writing WorkDir state: {}", path.display());
        OpenOptions::new().write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .and_then(|mut file| {
                writeln!(file, "{}", json::as_pretty_json(&self.state))
            })
            .chain_err(|| {
                format!("Could not write WorkDir state: {}", path.display())
            })?;

        self.disk_state = self.state.clone();
        Ok(())
    }

    /// Read state from disk
    fn read_state(&mut self) -> Result<()> {
        let path = self.state_file_path();
        debug!("Reading WorkDir state: {}", path.display());
        let state = OpenOptions::new().read(true)
            .open(&path)
            .map_err(|e| Error::from(e))
            .and_then(|mut file| {
                let mut json = String::new();
                file.read_to_string(&mut json)
                    .and(Ok(json))
                    .map_err(|e| e.into())
            })
            .and_then(|json| {
                json::decode::<WorkDirState>(&json).map_err(|e| e.into())
            })
            .chain_err(|| {
                format!("Could not read WorkDir state: {}", path.display())
            })?;

        self.disk_state = state.clone();
        self.state = state;
        Ok(())
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

impl Drop for WorkDir {
    fn drop(&mut self) {
        self.flush_state().expect("Could not flush hash file")
    }
}

mod test {
    use rustc_serialize::json;
    use super::*;

    #[test]
    fn test_serialize_work_dir_state() {
        let mut obj = WorkDirState::new();

        let encoded = json::encode(&obj).unwrap();
        // assert_eq!(encoded, "see encoded");
        let decoded: WorkDirState = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
