//! Functionality for transfering files between filesystem and object store

use cache::AllCaches;
use cache::FileStats;
use dag::HashedOrNot;
use dag::ObjectHandle;
use dag::ObjectKey;
use dag::PartialTree;
use dag::UnhashedPath;
use error::*;
use ignore::IgnoreList;
use objectstore::ObjectStore;
use rollinghash::read_file_objects;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::create_dir;
use std::fs::read_dir;
use std::fs::remove_file;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

pub enum OverwritePolicy {
    /// Overwrite all
    OverwriteAll,
    /// Keep modified files and directories
    KeepModified,
}

impl Default for OverwritePolicy {
    fn default() -> Self { OverwritePolicy::KeepModified }
}

impl OverwritePolicy {
    pub fn should_overwite(&self, target: &HashedOrNot) -> bool {
        use self::OverwritePolicy::*;
        use dag::HashedOrNot::*;
        match (self, target) {
            (&OverwriteAll, _) => true,
            (&KeepModified, &Hashed(_)) => true,
            (&KeepModified, &Dir(ref partial)) => partial.unhashed_size() == 0,
            (&KeepModified, &UnhashedFile(_)) => false,
        }
    }
}

pub struct ObjectFsTransfer {
    pub object_store: ObjectStore,
    pub cache: AllCaches,
    pub ignored: IgnoreList,
}

impl ObjectFsTransfer {
    pub fn with_object_store(object_store: ObjectStore) -> Self {
        let mut ignored = IgnoreList::default();
        ignored.insert(object_store.path());

        ObjectFsTransfer {
            object_store: object_store,
            ignored: ignored,
            cache: AllCaches::new(),
        }
    }

    pub fn with_repo_path(repo_path: PathBuf) -> Result<Self> {
        Ok(ObjectFsTransfer::with_object_store(ObjectStore::open(repo_path)?))
    }

    pub fn hash_object(&mut self,
                       path: &Path,
                       status: HashedOrNot)
                       -> Result<ObjectKey> {
        use dag::HashedOrNot::*;
        match status {
            Hashed(hash) => Ok(hash),
            UnhashedFile(_) => self.hash_file(path.to_owned()),
            Dir(partial) => self.hash_partial_tree(&path, partial),
        }
    }

    pub fn hash_file(&mut self, file_path: PathBuf) -> Result<ObjectKey> {
        let file = try!(File::open(&file_path));
        let file_stats = FileStats::from(file.metadata()?);
        let file = BufReader::new(file);

        return_if_cached!(self.cache, &file_path, &file_stats);
        info!("Hashing {}", file_path.display());

        let mut last_hash = ObjectKey::zero();
        for object in read_file_objects(file) {
            last_hash = try!(self.object_store.store_object(&object?));
        }

        try!(self.cache.insert(file_path, file_stats, last_hash.clone()));

        Ok(last_hash)
    }


    /// Read filesystem to construct a PartialTree
    pub fn dir_to_partial_tree(&mut self,
                               dir_path: &Path)
                               -> Result<PartialTree> {

        match self.check_hashed_status(&dir_path)? {
            HashedOrNot::Dir(partial) => Ok(partial),
            _ => bail!(ErrorKind::NotADirectory(dir_path.to_owned())),
        }
    }


    pub fn check_hashed_status(&mut self, path: &Path) -> Result<HashedOrNot> {

        let path_meta = path.metadata()
            .chain_err(|| format!("getting metadata for {}", path.display()))?;
        let hashed_or_not;

        if path_meta.is_file() {

            let cache_status = self.cache
                .check_with(&path, &path_meta.into())?;
            hashed_or_not = HashedOrNot::from(cache_status);

        } else if path_meta.is_dir() {

            let mut partial = PartialTree::new();

            for entry in try!(read_dir(path)) {
                let entry = try!(entry);
                let ch_path = entry.path();

                if self.ignored.ignores(&ch_path) {
                    continue;
                }

                let hashed_status = self.check_hashed_status(&ch_path)?;
                let is_empty = match &hashed_status {
                    &HashedOrNot::Dir(ref subdir) if subdir.is_empty() => true,
                    _ => false,
                };
                if !is_empty {
                    let ch_name = PathBuf::from(ch_path.file_name_or_err()?);
                    partial.insert(ch_name, hashed_status);
                }

            }
            hashed_or_not = HashedOrNot::from(partial);

        } else {
            bail!("Path {} was neither file nor directory", path.display());
        }

        Ok(hashed_or_not)
    }


    pub fn hash_partial_tree(&mut self,
                             dir_path: &Path,
                             mut partial: PartialTree)
                             -> Result<ObjectKey> {

        for (ch_name, unknown) in partial.unhashed().clone() {
            let ch_path = dir_path.join(&ch_name);

            let hash = match unknown {
                UnhashedPath::File(_) => self.hash_file(ch_path),
                UnhashedPath::Dir(partial) => {
                    self.hash_partial_tree(&ch_path, partial)
                }
            };
            partial.insert(ch_name, hash?);
        }

        assert!(partial.is_complete());
        self.object_store.store_object(partial.tree())
    }



    pub fn extract_object(&mut self,
                          hash: &ObjectKey,
                          path: &Path)
                          -> Result<()> {

        let handle = self.object_store
            .open_object(hash)
            .chain_err(|| {
                format!("Could not extract {} to {}", hash, path.display())
            })?;

        match handle {
            ObjectHandle::Blob(_) |
            ObjectHandle::ChunkedBlob(_) => {
                self.extract_file_open(handle, hash, path)
            }
            ObjectHandle::Tree(_) |
            ObjectHandle::Commit(_) => {
                self.extract_tree_open(handle, hash, path)
            }
        }
    }

    fn extract_tree_open(&mut self,
                         handle: ObjectHandle,
                         hash: &ObjectKey,
                         dir_path: &Path)
                         -> Result<()> {

        match handle {
            ObjectHandle::Commit(_) => {
                debug!("Extracting commit {}", hash);
                unimplemented!()
            }
            ObjectHandle::Tree(tree) => {
                debug!("Extracting tree {} to {}", hash, dir_path.display());

                if !dir_path.is_dir() {
                    if dir_path.exists() {
                        remove_file(&dir_path)?;
                    }
                    create_dir(&dir_path)?;
                }

                let tree = tree.read_content()?;

                for (ref name, ref hash) in tree.iter() {
                    self.extract_object(hash, &dir_path.join(name))?;
                }
                Ok(())
            }
            _ => bail!("Expected a Tree or Commit, got: {:?}", handle),
        }
    }

    fn extract_file_open(&mut self,
                         handle: ObjectHandle,
                         hash: &ObjectKey,
                         file_path: &Path)
                         -> Result<()> {

        return_if_cache_matches!(self.cache, file_path, hash);

        debug!("Extracting file {} to {}", hash, file_path.display());

        if file_path.is_dir() {
            bail!(ErrorKind::WouldClobberDirectory(file_path.to_owned()));
        }

        let mut out_file = OpenOptions::new().write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;

        self.copy_blob_content_open(handle, hash, &mut out_file)?;

        out_file.flush()?;
        let file_stats = FileStats::from(out_file.metadata()?);
        self.cache.insert(file_path.to_owned(), file_stats, hash.to_owned())?;

        Ok(())
    }

    fn copy_blob_content_open(&mut self,
                              handle: ObjectHandle,
                              hash: &ObjectKey,
                              writer: &mut Write)
                              -> Result<()> {
        match handle {
            ObjectHandle::Blob(blob) => {
                debug!("Extracting blob {}", hash);
                blob.copy_content(writer)?;
            }
            ObjectHandle::ChunkedBlob(index) => {
                debug!("Reading ChunkedBlob {}", hash);
                let index = index.read_content()?;
                for offset in index.chunks {
                    debug!("{}", offset);
                    let ch_handle = self.object_store
                        .open_object(&offset.hash)?;
                    self.copy_blob_content_open(ch_handle,
                                                &offset.hash,
                                                writer)?;
                }
            }
            _ => bail!("Expected a Blob or ChunkedBlob, got: {:?}", handle),
        };
        Ok(())
    }
}



#[cfg(test)]
mod test {
    use cache::CacheStatus;
    use dag::Blob;
    use dag::HashedOrNot;
    use dag::Object;
    use dag::ObjectCommon;
    use dag::ObjectKey;
    use dag::ObjectType;
    use rollinghash::CHUNK_TARGET_SIZE;
    use rollinghash::read_file_objects;
    use std::fs::create_dir;
    use std::fs::create_dir_all;
    use std::io::Cursor;
    use std::io::Read;
    use super::*;
    use testutil;

    #[test]
    fn test_hash_file_empty() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let filepath = temp.path().join("foo");
        testutil::write_file(&filepath, "").unwrap();

        let hash = fs_transfer.hash_file(filepath).unwrap();

        let obj = fs_transfer.object_store.open_object(&hash).unwrap();
        let obj = obj.read_content().unwrap();

        assert_eq!(Object::Blob(Blob::empty()), obj);
    }

    #[test]
    fn test_hash_file_small() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let filepath = temp.path().join("foo");

        testutil::write_file(&filepath, "foo").unwrap();

        let hash = fs_transfer.hash_file(filepath).unwrap();

        let obj = fs_transfer.object_store.open_object(&hash).unwrap();
        let obj = obj.read_content().unwrap();

        assert_eq!(Object::Blob(Blob::from("foo")), obj);
    }

    #[test]
    fn test_hash_file_chunked() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let filepath = temp.path().join("foo");
        let filesize = 3 * CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::TestRand::default();
        testutil::write_file(&filepath, rng.take(filesize)).unwrap();

        let hash = fs_transfer.hash_file(filepath).unwrap();

        let obj = fs_transfer.object_store.open_object(&hash).unwrap();
        let obj = obj.read_content().unwrap();

        if let Object::ChunkedBlob(chunked) = obj {
            assert_eq!(chunked.total_size, filesize);
            assert_eq!(chunked.chunks.len(), 5);

            for chunkrecord in chunked.chunks {
                let obj = fs_transfer.object_store
                    .open_object(&chunkrecord.hash)
                    .unwrap();
                let obj = obj.read_content().unwrap();
                assert_eq!(obj.object_type(), ObjectType::Blob);
                assert_eq!(obj.content_size(), chunkrecord.size);
            }

        } else {
            panic!("Not a ChunkedBlob: {:?}", obj);
        }

    }

    #[test]
    fn test_extract_object_object_not_found() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let out_file = temp.path().join("foo");
        let hash = Blob::from("12345").calculate_hash();

        let result = fs_transfer.extract_object(&hash, &out_file);
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_object_single_blob() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let blob = Blob::from("12345");
        let hash = fs_transfer.object_store.store_object(&blob).unwrap();

        let out_file = temp.path().join("foo");
        fs_transfer.extract_object(&hash, &out_file).unwrap();

        let out_content = testutil::read_file_to_string(&out_file).unwrap();
        assert_eq!(out_content, "12345");

        assert_eq!(fs_transfer.cache.check(&out_file).unwrap(),
                   CacheStatus::Cached { hash: hash },
                   "Cache should be primed with extracted file's hash");
    }

    #[test]
    fn test_extract_object_multi_chunks() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let mut rng = testutil::TestRand::default();
        let filesize = 3 * CHUNK_TARGET_SIZE as u64;
        let mut in_file = Vec::new();
        rng.take(filesize).read_to_end(&mut in_file).unwrap();

        let mut hash = ObjectKey::zero();
        for object in read_file_objects(Cursor::new(&in_file)) {
            hash = fs_transfer.object_store
                .store_object(&object.unwrap())
                .unwrap();
        }

        let out_file = temp.path().join("foo");
        fs_transfer.extract_object(&hash, &out_file).unwrap();

        assert_eq!(out_file.metadata().unwrap().len(), filesize);

        let out_content = testutil::read_file_to_end(&out_file).unwrap();
        assert!(out_content == in_file);

        assert_eq!(fs_transfer.cache.check(&out_file).unwrap(),
                   CacheStatus::Cached { hash: hash },
                   "Cache should be primed with extracted file's hash");
    }

    #[test]
    fn test_extract_object_clobber_existing_file() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let blob = Blob::from("12345");
        let hash = fs_transfer.object_store.store_object(&blob).unwrap();

        let out_file = temp.path().join("foo");
        testutil::write_file(&out_file, "Existing content. To be clobbered.")
            .unwrap();

        fs_transfer.extract_object(&hash, &out_file).unwrap();

        let out_content = testutil::read_file_to_string(&out_file).unwrap();
        assert_eq!(out_content, "12345");

        assert_eq!(fs_transfer.cache.check(&out_file).unwrap(),
                   CacheStatus::Cached { hash: hash },
                   "Cache should be primed with extracted file's hash");
    }

    #[test]
    fn test_extract_object_abort_on_existing_directory() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let blob = Blob::from("12345");
        let hash = fs_transfer.object_store.store_object(&blob).unwrap();

        let out_file = temp.path().join("foo");
        create_dir(&out_file).unwrap();

        let result = fs_transfer.extract_object(&hash, &out_file);

        match result {
            Err(Error(ErrorKind::WouldClobberDirectory(p), _)) => {
                assert_eq!(p, out_file)
            }
            _ => panic!("Got incorrect error: {:?}", result),
        }
    }

    use dag::Tree;
    fn do_store_directory_test<WF>(workdir_name: &str,
                                   write_files: WF,
                                   expected_partial: PartialTree,
                                   expected_tree: Tree,
                                   expected_cached_partial: PartialTree)
        where WF: FnOnce(&Path)
    {

        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let wd_path = temp.path().join(workdir_name);

        write_files(&wd_path);

        // Build partial tree

        let partial = fs_transfer.dir_to_partial_tree(&wd_path).unwrap();
        assert_eq!(partial, expected_partial);

        // Hash and store files

        let hash = fs_transfer.hash_partial_tree(&wd_path, partial).unwrap();

        let obj = fs_transfer.object_store.open_object(&hash).unwrap();
        let obj = obj.read_content().unwrap();

        assert_eq!(obj, Object::Tree(expected_tree.clone()));

        // Flush cache files
        fs_transfer.cache.flush();

        // Build partial tree again -- make sure it doesn't pick up cache files

        let partial = fs_transfer.dir_to_partial_tree(&wd_path).unwrap();
        assert_eq!(partial, expected_cached_partial);

        // Extract and compare
        let extract_path = temp.path().join("extract_dir");
        fs_transfer.extract_object(&hash, &extract_path).unwrap();

        let extract_partial = fs_transfer.dir_to_partial_tree(&extract_path)
            .unwrap();
        assert_eq!(extract_partial, expected_cached_partial);
    }

    #[test]
    fn test_store_directory_shallow() {

        let write_files = |wd_path: &Path| {
            write_files!{
                wd_path;
                "foo" => "123",
                "bar" => "1234",
                "baz" => "12345",
            };
        };

        let expected_partial = partial_tree!{
            "foo" => HashedOrNot::UnhashedFile(3),
            "bar" => HashedOrNot::UnhashedFile(4),
            "baz" => HashedOrNot::UnhashedFile(5),
        };

        let expected_tree = tree_object!{
            "foo" => Blob::from("123").calculate_hash(),
            "bar" => Blob::from("1234").calculate_hash(),
            "baz" => Blob::from("12345").calculate_hash(),
        };

        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
            "bar" => Blob::from("1234").calculate_hash(),
            "baz" => Blob::from("12345").calculate_hash(),
        };

        do_store_directory_test("work_dir",
                                write_files,
                                expected_partial,
                                expected_tree.clone(),
                                expected_cached_partial);
    }

    #[test]
    fn test_store_directory_recursive() {

        let write_files = |wd_path: &Path| {
            write_files!{
                wd_path;
                "foo" => "123",
                "level1/bar" => "1234",
                "level1/level2/baz" => "12345",
            };
        };

        let expected_partial = partial_tree!{
            "foo" => HashedOrNot::UnhashedFile(3),
            "level1" => partial_tree!{
                "bar" => HashedOrNot::UnhashedFile(4),
                "level2" => partial_tree!{
                    "baz" => HashedOrNot::UnhashedFile(5),
                },
            },
        };

        let expected_tree = tree_object!{
            "foo" => Blob::from("123").calculate_hash(),
            "level1" => tree_object!{
                "bar" => Blob::from("1234").calculate_hash(),
                "level2" => tree_object!{
                    "baz" => Blob::from("12345").calculate_hash(),
                }.calculate_hash(),
            }.calculate_hash(),
        };

        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
            "level1" => partial_tree!{
                "bar" => Blob::from("1234").calculate_hash(),
                "level2" => partial_tree!{
                    "baz" => Blob::from("12345").calculate_hash(),
                },
            },
        };

        do_store_directory_test("work_dir",
                                write_files,
                                expected_partial,
                                expected_tree,
                                expected_cached_partial);
    }

    #[test]
    fn test_store_directory_ignore_objectstore_dir() {
        let write_files = |wd_path: &Path| {
            write_files!{
                wd_path;
                "foo" => "123",
                "level1/bar" => "1234",
                "level1/level2/baz" => "12345",
            };
        };

        let expected_partial = partial_tree!{
            "foo" => HashedOrNot::UnhashedFile(3),
            "level1" => partial_tree!{
                "bar" => HashedOrNot::UnhashedFile(4),
                "level2" => partial_tree!{
                    "baz" => HashedOrNot::UnhashedFile(5),
                },
            },
        };

        let expected_tree = tree_object!{
            "foo" => Blob::from("123").calculate_hash(),
            "level1" => tree_object!{
                "bar" => Blob::from("1234").calculate_hash(),
                "level2" => tree_object!{
                    "baz" => Blob::from("12345").calculate_hash(),
                }.calculate_hash(),
            }.calculate_hash(),
        };

        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
            "level1" => partial_tree!{
                "bar" => Blob::from("1234").calculate_hash(),
                "level2" => partial_tree!{
                    "baz" => Blob::from("12345").calculate_hash(),
                },
            },
        };

        do_store_directory_test("",
                                write_files,
                                expected_partial,
                                expected_tree,
                                expected_cached_partial);
    }

    #[test]
    fn test_store_directory_ignore_empty_dirs() {
        let write_files = |wd_path: &Path| {
            write_files!{
                wd_path;
                "foo" => "123",
            };
            create_dir_all(wd_path.join("empty1/empty2/empty3")).unwrap();
        };

        let expected_partial = partial_tree!{
            "foo" => HashedOrNot::UnhashedFile(3),
        };

        let expected_tree = tree_object!{
            "foo" => Blob::from("123").calculate_hash(),
        };

        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
        };

        do_store_directory_test("work_dir",
                                write_files,
                                expected_partial,
                                expected_tree,
                                expected_cached_partial);
    }

    #[test]
    fn test_extract_directory_clobber_file() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let wd_path = temp.path().join("work_dir");

        write_files!{ wd_path; "foo" => "123", };
        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
        };

        // Hash and store files

        let partial = fs_transfer.dir_to_partial_tree(&wd_path).unwrap();
        let hash = fs_transfer.hash_partial_tree(&wd_path, partial).unwrap();

        // Extract path is an existing file
        let extract_path = temp.path().join("extract_dir");
        testutil::write_file(&extract_path, "Existing file").unwrap();

        // Extract and compare
        fs_transfer.extract_object(&hash, &extract_path).unwrap();

        let extract_partial = fs_transfer.dir_to_partial_tree(&extract_path)
            .unwrap();
        assert_eq!(extract_partial, expected_cached_partial);
    }

    #[test]
    fn test_extract_directory_ok_with_existing_dir() {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join("object_store");
        let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)
            .unwrap();

        let wd_path = temp.path().join("work_dir");

        write_files!{ wd_path; "foo" => "123", };
        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
        };

        // Hash and store files

        let partial = fs_transfer.dir_to_partial_tree(&wd_path).unwrap();
        let hash = fs_transfer.hash_partial_tree(&wd_path, partial).unwrap();

        // Extract path is an existing directory
        let extract_path = temp.path().join("extract_dir");
        write_files!{ extract_path; "foo" => "Exiting file", };

        // Extract and compare
        fs_transfer.extract_object(&hash, &extract_path).unwrap();

        let extract_partial = fs_transfer.dir_to_partial_tree(&extract_path)
            .unwrap();
        assert_eq!(extract_partial, expected_cached_partial);
    }

}
