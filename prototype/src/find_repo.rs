//! Functionality for finding the repository at startup

use constants::HIDDEN_DIR_NAME;
use error::*;
use fs_transfer::ObjectFsTransfer;
use fsutil::up_from;
use objectstore::ObjectStore;
use std::env::current_dir;
use std::path::Path;
use std::path::PathBuf;
use work_dir::WorkDir;

struct Layout {
    osd: PathBuf,
    wd: PathBuf,
}

impl Layout {
    fn find_repo(start_path: &Path) -> Result<Self> {
        for path in up_from(&start_path) {
            let hidden_path = path.join(HIDDEN_DIR_NAME);
            if hidden_path.metadata()?.is_dir() {
                return Ok(Layout {
                    osd: hidden_path,
                    wd: path.to_owned(),
                });
            }
        }
        bail!("Could not find repository directory, in \"{}\" or its parents",
              start_path.display())
    }
}

pub fn find_object_store() -> Result<ObjectStore> {
    let start_dir = current_dir()?;
    Layout::find_repo(&start_dir)
        .and_then(|layout| ObjectStore::open(layout.osd))
}

pub fn find_fs_transfer() -> Result<ObjectFsTransfer> {
    find_object_store().map(|os| ObjectFsTransfer::with_object_store(os))
}

pub fn find_work_dir() -> Result<WorkDir> {
    let start_dir = current_dir()?;
    Layout::find_repo(&start_dir).and_then(|layout| WorkDir::open(layout.wd))
}
