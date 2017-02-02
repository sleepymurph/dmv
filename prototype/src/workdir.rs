use constants;
use dag;
use error::*;
use fsutil;
use objectstore;
use status;
use std::env;
use std::fs;
use std::path;

pub struct WorkDir {
    path: path::PathBuf,
    current_branch: Option<dag::ObjectKey>,
    pub object_store: objectstore::ObjectStore,
}

impl WorkDir {
    /// Initialize the given directory as a working directory
    pub fn init(wd_path: path::PathBuf) -> Result<Self> {

        let os_path = wd_path.join(constants::HIDDEN_DIR_NAME);
        let os = try!(objectstore::ObjectStore::init(os_path));

        let wd = WorkDir {
            path: wd_path,
            current_branch: None,
            object_store: os,
        };

        Ok(wd)
    }

    /// Load a working directory that has already been initialized
    pub fn open(wd_path: path::PathBuf) -> Result<Self> {
        let os_path = wd_path.join(constants::HIDDEN_DIR_NAME);
        let os = try!(objectstore::ObjectStore::open(os_path));

        let wd = WorkDir {
            path: wd_path,
            current_branch: None,
            object_store: os,
        };

        Ok(wd)
    }

    /// Search the given path and its parents for a working directory
    pub fn find<P: AsRef<path::Path>>(start_path: &P) -> Result<Self> {
        let start_path = start_path.as_ref();
        for path in fsutil::up_from(&start_path) {
            let hidden_path = path.join(constants::HIDDEN_DIR_NAME);
            if hidden_path.is_dir() {
                return Self::open(path.to_owned()).chain_err(|| {
                    format!("Found working directory {} but could not open it",
                            hidden_path.display())
                });
            }
        }
        Err(format!("Could not find working directory in \"{}\" or its \
                     parents",
                    start_path.display())
            .into())
    }

    /// Search for a working directory, starting with the current dir
    pub fn find_from_current_dir() -> Result<Self> {
        Self::find(&env::current_dir()?)
    }

    pub fn path(&self) -> &path::Path { &self.path }

    pub fn check_status(&self) -> Result<status::DirStatus> {
        let meta = try!(self.path.metadata());
        let status = try!(self.check_status_path(self.path(),
                                                 &meta,
                                                 self.current_branch.as_ref()));
        match status {
            status::PathStatus::ModifiedDir { status, .. } => Ok(status),
            _ => {
                panic!("Working directory is not a directory: {:?}", &self.path)
            }
        }
    }

    pub fn check_status_path(&self,
                             path: &path::Path,
                             meta: &fs::Metadata,
                             _expected_hash: Option<&dag::ObjectKey>)
                             -> Result<status::PathStatus> {

        if meta.is_dir() {

            let mut dirstatus = status::DirStatus::new();

            for child in try!(fs::read_dir(path)) {
                let child = child?;
                let subpath = child.path();

                if subpath == self.object_store.path() {
                    continue;
                }

                let filename = path::PathBuf::from(&child.file_name());
                let submeta = child.metadata()?;

                let childstatus =
                    self.check_status_path(&subpath, &submeta, None)?;
                dirstatus.insert(filename, childstatus);
            }

            Ok(status::PathStatus::ModifiedDir { status: dirstatus })

        } else if meta.is_file() {

            Ok(status::PathStatus::UncachedFile { size: meta.len() })

        } else {
            unimplemented!()
        }
    }
}

#[cfg(test)]
mod test {

    use error::*;
    use fsutil;
    use rollinghash;
    use std::fs;
    use super::*;
    use tempdir::TempDir;
    use testutil;

    fn create_temp_repository() -> Result<(TempDir, WorkDir)> {
        let wd_temp = try!(testutil::in_mem_tempdir("test_repository"));
        let wd_path = wd_temp.path().to_path_buf();
        try!(fs::create_dir_all(&wd_path));

        let wd = try!(WorkDir::init(wd_path));
        Ok((wd_temp, wd))
    }

    #[test]
    fn test_check_status_no_cache() {
        let (_temp, workdir) = create_temp_repository().unwrap();
        let mut rng = testutil::RandBytes::new();

        let wd_path = workdir.path();

        testutil::write_str_file(&wd_path.join("foo"), "foo").unwrap();
        testutil::write_str_file(&wd_path.join("bar"), "bar").unwrap();

        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;
        rng.write_file(&wd_path.join("baz"), filesize).unwrap();

        testutil::write_str_file(&wd_path.join("sub/x"), "new x").unwrap();
        testutil::write_str_file(&wd_path.join("sub/y"), "new y").unwrap();

        let status = workdir.check_status().unwrap();

        assert!(status.is_modified());
        assert_eq!(status.to_hash_total_size(), 46096);

        // Uncomment to examine status value
        // panic!("{:?}", status);
    }

    #[test]
    fn test_find_workdir() {
        let (_temp, workdir) = create_temp_repository().unwrap();
        let child = workdir.path().join("a/b/c/d");
        fsutil::create_parents(&child).expect("could not create child dir");
        let found = WorkDir::find(&child).expect("could not find workdir");
        assert_eq!(found.path(), workdir.path());
    }

    // TODO: Commit and create cache
    // TODO: Status after commit
    // TODO: Deleted values
}
