//! Working Directory: Files checked out from an ObjectStore

use constants::HARDCODED_BRANCH;
use dag::Commit;
use dag::ObjectKey;
use error::*;
use find_repo::RepoLayout;
use fs_transfer::ObjectFsTransfer;
use objectstore::ObjectStore;
use std::path::Path;
use std::path::PathBuf;

pub struct WorkDir {
    fs_transfer: ObjectFsTransfer,
    path: PathBuf,
}

impl WorkDir {
    pub fn init(layout: RepoLayout) -> Result<Self> {
        let os = ObjectStore::init(layout.osd)?;
        Ok(WorkDir {
            fs_transfer: ObjectFsTransfer::with_object_store(os),
            path: layout.wd,
        })
    }
    pub fn open(layout: RepoLayout) -> Result<Self> {
        Ok(WorkDir {
            fs_transfer: ObjectFsTransfer::with_repo_path(layout.osd)?,
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
