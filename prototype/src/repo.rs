use cache;
use dag;
use objectstore;
use rollinghash;
use status;
use std::fs;
use std::io;
use std::ops;
use std::path;

pub struct WorkDir {
    path: path::PathBuf,
    current_branch: Option<dag::ObjectKey>,
    cache: cache::FileCache,
}

pub struct Repo {
    pub objectstore: objectstore::ObjectStore,
    pub workdir: WorkDir,
}

impl ops::Deref for WorkDir {
    type Target = path::Path;
    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl Repo {
    pub fn check_status(&self) -> io::Result<status::DirStatus> {
        let meta = try!(self.workdir.metadata());
        let status =
            try!(self.check_status_path(&self.workdir,
                                        &meta,
                                        self.workdir.current_branch.as_ref()));
        match status {
            status::PathStatus::ModifiedDir { status, .. } => Ok(status),
            _ => {
                panic!("Working directory is not a directory: {:?}",
                       &self.workdir.path)
            }
        }
    }

    pub fn check_status_path(&self,
                             path: &path::Path,
                             meta: &fs::Metadata,
                             _expected_hash: Option<&dag::ObjectKey>)
                             -> io::Result<status::PathStatus> {

        if meta.is_dir() {

            let mut dirstatus = status::DirStatus::new();

            for child in try!(fs::read_dir(path)) {
                let child = child?;
                let subpath = child.path();

                if subpath == self.objectstore.path() {
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

    pub fn store_object<O: dag::Object>(&mut self,
                                        obj: &O)
                                        -> io::Result<dag::ObjectKey> {
        let mut incoming = try!(self.objectstore.new_object());
        let key = try!(obj.write_to(&mut incoming));
        try!(self.objectstore.save_object(key, incoming));
        Ok(key)
    }

    pub fn store_file(&mut self,
                      path: &path::Path)
                      -> io::Result<dag::ObjectKey> {

        let file = try!(fs::File::open(path));
        let file = io::BufReader::new(file);

        let mut chunker = rollinghash::ChunkReader::wrap(file);
        let chunk1 = chunker.next();
        let chunk2 = chunker.next();

        match (chunk1, chunk2) {
            (None, None) => {
                // Empty file
                let blob = dag::Blob::from_vec(vec![0u8;0]);
                self.store_object(&blob)
            }
            (Some(v1), None) => {
                // File only one-chunk long
                let blob = dag::Blob::from_vec(v1?);
                self.store_object(&blob)
            }
            (Some(v1), Some(v2)) => {
                // Multiple chunks
                let mut chunkedblob = dag::ChunkedBlob::new();

                for chunk in vec![v1, v2].into_iter().chain(chunker) {
                    let blob = dag::Blob::from_vec(chunk?);
                    let key = try!(self.store_object(&blob));
                    chunkedblob.add_chunk(blob.size(), key);
                }

                self.store_object(&chunkedblob)
            }
            (None, Some(_)) => unreachable!(),
        }
    }


    pub fn store_directory<P: AsRef<path::Path>>
        (&mut self,
         relpath: &P)
         -> io::Result<dag::ObjectKey> {

        let abspath = self.workdir.path.join(relpath);
        let mut tree = dag::Tree::new();

        for entry in try!(fs::read_dir(&abspath)) {

            let entry = try!(entry);

            let subpath = entry.path();
            if subpath == self.objectstore.path() {
                continue;
            }

            let name = try!(subpath.strip_prefix(&abspath)
                .map_err(|spe| io::Error::new(io::ErrorKind::Other, spe)));

            let key;
            if subpath.is_dir() {
                key = try!(self.store_directory(&subpath));
            } else if subpath.is_file() {
                key = try!(self.store_file(&subpath));
            } else {
                unimplemented!()
            };

            tree.insert(name.to_owned(), key);
        }
        self.store_object(&tree)
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;


    use cache;
    use dag;
    use dag::Object;
    use objectstore;
    use rollinghash;
    use std::fs;
    use std::io;

    use super::*;
    use testutil;

    fn create_temp_repository() -> io::Result<(tempdir::TempDir, Repo)> {
        let wd_temp = try!(tempdir::TempDir::new_in("/dev/shm",
                                                    "test_repository"));
        let wd_path = wd_temp.path().to_path_buf();
        try!(fs::create_dir_all(&wd_path));
        let wd = WorkDir {
            path: wd_path.clone(),
            current_branch: None,
            cache: cache::FileCache::new(),
        };

        let os_path = wd_path.join(".prototype");
        let os = objectstore::ObjectStore::new(&os_path);
        try!(os.init());

        let repo = Repo {
            objectstore: os,
            workdir: wd,
        };

        Ok((wd_temp, repo))
    }

    #[test]
    fn test_store_file_empty() {
        let (_temp, mut repo) = create_temp_repository().unwrap();
        let filepath = repo.workdir.join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = repo.store_file(&filepath).unwrap();

        let obj = repo.objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = dag::Blob::read_from(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_store_file_small() {
        let (_temp, mut repo) = create_temp_repository().unwrap();
        let filepath = repo.workdir.join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = repo.store_file(&filepath).unwrap();

        let obj = repo.objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);

        let blob = dag::Blob::read_from(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_store_file_chunked() {
        let (_temp, mut repo) = create_temp_repository().unwrap();
        let filepath = repo.workdir.join("foo");
        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = repo.store_file(&filepath).unwrap();

        let obj = repo.objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);
        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, dag::ObjectType::ChunkedBlob);

        let chunked = dag::ChunkedBlob::read_from(&mut obj).unwrap();
        assert_eq!(chunked.total_size, filesize);
        assert_eq!(chunked.chunks.len(), 5);

        for chunkrecord in chunked.chunks {
            let obj = repo.objectstore.read_object(&chunkrecord.hash).unwrap();
            let mut obj = io::BufReader::new(obj);
            let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, dag::ObjectType::Blob);

            let blob = dag::Blob::read_from(&mut obj).unwrap();
            assert_eq!(blob.size(), chunkrecord.size);
        }
    }


    #[test]
    fn test_store_directory() {
        let (_temp, mut repo) = create_temp_repository().unwrap();
        let mut rng = testutil::RandBytes::new();

        let wd_path = repo.workdir.to_path_buf();

        testutil::write_str_file(&wd_path.join("foo"), "foo").unwrap();
        testutil::write_str_file(&wd_path.join("bar"), "bar").unwrap();

        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;
        rng.write_file(&wd_path.join("baz"), filesize).unwrap();

        let hash = repo.store_directory(&".").unwrap();

        let obj = repo.objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);
        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, dag::ObjectType::Tree);

        let tree = dag::Tree::read_from(&mut obj).unwrap();
        // assert_eq!(tree, dag::Tree::new());
        assert_eq!(tree.len(), 3);

        // TODO: nested directories
        // TODO: consistent sort order
    }

    #[test]
    fn test_check_status_no_cache() {
        let (_temp, repo) = create_temp_repository().unwrap();
        let mut rng = testutil::RandBytes::new();

        testutil::write_str_file(&repo.workdir.join("foo"), "foo").unwrap();
        testutil::write_str_file(&repo.workdir.join("bar"), "bar").unwrap();

        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;
        rng.write_file(&repo.workdir.join("baz"), filesize).unwrap();

        testutil::write_str_file(&repo.workdir.join("sub/x"), "new x").unwrap();
        testutil::write_str_file(&repo.workdir.join("sub/y"), "new y").unwrap();

        let status = repo.check_status().unwrap();

        assert!(status.is_modified());
        assert_eq!(status.to_hash_total_size(), 46096);

        // Uncomment to examine status value
        // panic!("{:?}", status);
    }

    // TODO: Commit and create cache
    // TODO: Status after commit
    // TODO: Deleted values
}
