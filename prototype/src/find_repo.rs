//! Functionality for finding WorkDir and/or ObjectStore at startup

use constants::HIDDEN_DIR_NAME;
use error::*;
use fs_transfer::FsTransfer;
use fsutil::up_from;
use objectstore::ObjectStore;
use std::env::current_dir;
use std::path::Path;
use std::path::PathBuf;
use work_dir::WorkDir;

/// A repository layout with a WorkDir and hidden directory
pub struct RepoLayout {
    /// ObjectStore directory: the hidden directory inside the WorkDir
    pub osd: PathBuf,
    /// WorkDir: the working directory
    pub wd: PathBuf,
}

impl RepoLayout {
    pub fn in_work_dir(wd: PathBuf) -> Self {
        RepoLayout {
            osd: wd.join(HIDDEN_DIR_NAME),
            wd: wd,
        }
    }
}

/// Find the repository layout
fn find_repo(start_path: &Path) -> Result<RepoLayout> {
    for path in up_from(&start_path) {
        let hidden_path = path.join(HIDDEN_DIR_NAME);
        trace!("Looking for repo: {} ?", hidden_path.display());
        if hidden_path.is_dir() {
            debug!("Found repo: {}", hidden_path.display());
            return Ok(RepoLayout {
                osd: hidden_path,
                wd: path.to_owned(),
            });
        }
    }
    bail!("Could not find repository directory in \"{}\" or its parents",
          start_path.display())
}

/// Find just an ObjectStore
pub fn find_object_store() -> Result<ObjectStore> {
    let start_dir = current_dir()?;
    find_repo(&start_dir).and_then(|layout| ObjectStore::open(layout.osd))
}

/// Find ObjectStore and create an FsTransfer around it
pub fn find_fs_transfer() -> Result<FsTransfer> {
    find_object_store().map(|os| FsTransfer::with_object_store(os))
}

/// Find entire WorkDir
pub fn find_work_dir() -> Result<WorkDir> {
    let start_dir = current_dir()?;
    find_repo(&start_dir).and_then(|layout| WorkDir::open(layout))
}
