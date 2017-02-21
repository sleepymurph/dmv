//! Working Directory: Files checked out from an ObjectStore

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
}
