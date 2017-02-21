//! Working Directory: Files checked out from an ObjectStore

use constants::HIDDEN_DIR_NAME;
use error::*;
use fs_transfer::ObjectFsTransfer;
use objectstore::ObjectStore;
use std::path::Path;
use std::path::PathBuf;

pub struct WorkDir {
    fs_transfer: ObjectFsTransfer,
    path: PathBuf,
}

impl WorkDir {
    pub fn open(wd: PathBuf) -> Result<Self> {
        let osd = wd.join(HIDDEN_DIR_NAME);
        Ok(WorkDir {
            fs_transfer: ObjectFsTransfer::with_repo_path(osd)?,
            path: wd,
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
