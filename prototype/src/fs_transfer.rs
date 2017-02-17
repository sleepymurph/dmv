//! Functionality for transfering files between filesystem and object store

use cache::AllCaches;
use cache::FileStats;
use dag::HashedOrNot;
use dag::ObjectHandle;
use dag::ObjectKey;
use dag::PartialTree;
use dag::Tree;
use dag::UnhashedPath;
use error::*;
use ignore::IgnoreList;
use objectstore::ObjectStore;
use rollinghash::read_file_objects;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::create_dir;
use std::fs::read_dir;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;


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


    pub fn check_status(&mut self, path: &Path) -> Result<HashedOrNot> {

        let path_meta = path.metadata()
            .chain_err(|| format!("getting metadata for {}", path.display()))?;

        let hashed_or_not: HashedOrNot;

        if path_meta.is_file() {

            hashed_or_not =
                self.cache.check_with(&path, &path_meta.into())?.into();

        } else if path_meta.is_dir() {

            let mut partial = PartialTree::new();

            for entry in try!(read_dir(path)) {
                let entry = try!(entry);
                let ch_path = entry.path();

                if self.ignored.ignores(&ch_path) {
                    continue;
                }

                let ch_name = PathBuf::from(ch_path.file_name_or_err()?);
                let hashed_status = self.check_status(&ch_path)?;
                partial.insert(ch_name, hashed_status);
            }
            hashed_or_not = partial.into();

        } else {
            bail!("Path {} was neither file nor directory", path.display());
        }

        Ok(hashed_or_not)
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

    fn hash_file(&mut self, file_path: PathBuf) -> Result<ObjectKey> {
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

    fn hash_partial_tree(&mut self,
                         dir_path: &Path,
                         mut partial: PartialTree)
                         -> Result<ObjectKey> {

        if partial.is_empty() {
            bail!("Refusing to hash empty directory: {}", dir_path.display());
        }

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

        self.object_store
            .open_object(hash)
            .and_then(|handle| self.extract_object_open(handle, hash, path))
            .chain_err(|| {
                format!("Could not extract {} to {}", hash, path.display())
            })
    }

    pub fn extract_object_open(&mut self,
                               handle: ObjectHandle,
                               hash: &ObjectKey,
                               path: &Path)
                               -> Result<()> {
        match handle {
            ObjectHandle::Blob(_) |
            ObjectHandle::ChunkedBlob(_) => {
                debug!("Extracting file {} to {}", hash, path.display());
                self.extract_file_open(handle, hash, path)
            }
            ObjectHandle::Tree(tree) => {
                debug!("Extracting tree {} to {}", hash, path.display());
                let tree = tree.read_content()?;
                self.extract_tree_open(tree, path)
            }
            ObjectHandle::Commit(_) => unimplemented!(),
        }
    }

    fn extract_tree_open(&mut self, tree: Tree, dir_path: &Path) -> Result<()> {

        if !dir_path.is_dir() {
            if dir_path.exists() {
                remove_file(&dir_path)?;
            }
            create_dir(&dir_path)?;
        }

        for (ref name, ref hash) in tree.iter() {
            self.extract_object(hash, &dir_path.join(name))?;
        }

        Ok(())
    }

    fn extract_file_open(&mut self,
                         handle: ObjectHandle,
                         hash: &ObjectKey,
                         path: &Path)
                         -> Result<()> {
        return_if_cache_matches!(self.cache, path, hash);

        if path.is_dir() {
            remove_dir_all(path)?;
        }

        let mut out_file = OpenOptions::new().write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        self.copy_blob_content_open(handle, hash, &mut out_file)?;

        out_file.flush()?;
        let file_stats = FileStats::from(out_file.metadata()?);
        self.cache.insert(path.to_owned(), file_stats, hash.to_owned())?;

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
    use dag::ObjectType;
    use hamcrest::prelude::*;
    use rollinghash::CHUNK_TARGET_SIZE;
    use std::fs::create_dir_all;
    use super::*;
    use testutil;
    use testutil::tempdir::TempDir;

    fn create_temp_repo(dir_name: &str) -> (TempDir, ObjectFsTransfer) {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join(dir_name);
        let fs_transfer = ObjectFsTransfer::with_repo_path(repo_path).unwrap();
        (temp, fs_transfer)
    }

    fn do_store_single_file_test(in_file: &[u8],
                                 expected_object_type: ObjectType) {

        let (temp, mut fs_transfer) = create_temp_repo("object_store");

        // Write input file to disk
        let filepath = temp.path().join("foo");
        testutil::write_file(&filepath, in_file).unwrap();

        // Hash input file
        let status = fs_transfer.check_status(&filepath).unwrap();
        let hash = fs_transfer.hash_object(&filepath, status).unwrap();

        // Check the object type
        let obj = fs_transfer.object_store.open_object(&hash).unwrap();
        assert_eq!(obj.header().object_type, expected_object_type);

        // Extract the object
        let out_file = temp.path().join("bar");
        fs_transfer.extract_object(&hash, &out_file).unwrap();

        // Compare input and output
        assert_eq!(out_file.metadata().unwrap().len(), in_file.len() as u64);
        let out_content = testutil::read_file_to_end(&out_file).unwrap();
        assert!(out_content.as_slice() == in_file, "file contents differ");

        // Make sure the output is cached
        assert_eq!(fs_transfer.cache.check(&out_file).unwrap(),
                   CacheStatus::Cached { hash: hash },
                   "Cache should be primed with extracted file's hash");
    }

    #[test]
    fn test_hash_file_empty() {
        do_store_single_file_test(&Vec::new(), ObjectType::Blob);
    }

    #[test]
    fn test_hash_file_small() {
        do_store_single_file_test("foo".as_bytes(), ObjectType::Blob);
    }

    #[test]
    fn test_hash_file_chunked() {
        let filesize = 3 * CHUNK_TARGET_SIZE;
        let in_file = testutil::TestRand::default().gen_byte_vec(filesize);
        do_store_single_file_test(&in_file, ObjectType::ChunkedBlob);
    }

    #[test]
    fn test_extract_object_object_not_found() {
        let (temp, mut fs_transfer) = create_temp_repo("object_store");

        let out_file = temp.path().join("foo");
        let hash = Blob::from("12345").calculate_hash();

        let result = fs_transfer.extract_object(&hash, &out_file);
        assert!(result.is_err());
    }

    use dag::Tree;
    fn do_store_directory_test<WF>(workdir_name: &str,
                                   write_files: WF,
                                   expected_partial: PartialTree,
                                   expected_tree: Tree,
                                   expected_cached_partial: PartialTree)
        where WF: FnOnce(&Path)
    {

        let (temp, mut fs_transfer) = create_temp_repo("object_store");

        let wd_path = temp.path().join(workdir_name);
        write_files(&wd_path);

        // Build partial tree

        let partial = fs_transfer.check_status(&wd_path).unwrap();
        assert_eq!(partial, HashedOrNot::Dir(expected_partial));

        // Hash and store files

        let hash = fs_transfer.hash_object(&wd_path, partial).unwrap();

        let obj = fs_transfer.object_store.open_object(&hash).unwrap();
        let obj = obj.read_content().unwrap();

        assert_eq!(obj, Object::Tree(expected_tree.clone()));

        // Flush cache files
        fs_transfer.cache.flush();

        // Build partial tree again -- make sure it doesn't pick up cache files

        let partial = fs_transfer.check_status(&wd_path).unwrap();
        assert_eq!(partial, HashedOrNot::Dir(expected_cached_partial.clone()));

        // Extract and compare
        let extract_path = temp.path().join("extract_dir");
        fs_transfer.extract_object(&hash, &extract_path).unwrap();

        let extract_partial = fs_transfer.check_status(&extract_path)
            .unwrap();
        assert_eq!(extract_partial, HashedOrNot::Dir(expected_cached_partial));
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
    fn test_store_directory_error_if_empty() {
        let (temp, mut fs_transfer) = create_temp_repo("object_store");
        let wd_path = temp.path().join("work_dir");
        create_dir_all(wd_path.join("empty1/empty2/empty3")).unwrap();

        // Build partial tree

        let partial = fs_transfer.check_status(&wd_path).unwrap();
        assert_eq!(partial, HashedOrNot::Dir(PartialTree::new()));

        // Hash and store files

        let hash = fs_transfer.hash_object(&wd_path, partial);
        assert!(hash.is_err());
        // hash.unwrap();
    }


    #[test]
    fn test_default_overwrite_policy() {
        let (temp, mut fs_transfer) = create_temp_repo("object_store");
        let wd_path = temp.path().join("work_dir");

        let source = wd_path.join("in_file");
        testutil::write_file(&source, "in_file content").unwrap();
        let hash = fs_transfer.hash_file(source.clone()).unwrap();


        // File vs cached file
        let target = wd_path.join("cached_file");
        testutil::write_file(&target, "cached_file content").unwrap();
        fs_transfer.hash_file(target.clone()).unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        let content = testutil::read_file_to_string(&target).unwrap();
        assert_that!(&content, equal_to("in_file content"));


        // File vs uncached file
        let target = wd_path.join("uncached_file");
        testutil::write_file(&target, "uncached_file content").unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        let content = testutil::read_file_to_string(&target).unwrap();
        assert_that!(&content, equal_to("in_file content"));


        // File vs empty dir
        let target = wd_path.join("empty_dir");
        create_dir_all(&target).unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        let content = testutil::read_file_to_string(&target).unwrap();
        assert_that!(&content, equal_to("in_file content"));


        // File vs non-empty dir
        let target = wd_path.join("dir");
        write_files!{
            &target;
            "dir_file" => "dir_file content",
        };

        fs_transfer.extract_object(&hash, &target).unwrap();
        let content = testutil::read_file_to_string(&target).unwrap();
        assert_that!(&content, equal_to("in_file content"));
    }

    #[test]
    fn test_extract_directory_clobber_file() {
        let (temp, mut fs_transfer) = create_temp_repo("object_store");
        let wd_path = temp.path().join("work_dir");

        let source = wd_path.join("in_dir");
        write_files!{
                source;
                "file1" => "dir/file1 content",
                "file2" => "dir/file2 content",
        };

        let status = fs_transfer.check_status(&source).unwrap();
        let hash = fs_transfer.hash_object(&source, status).unwrap();

        // Dir vs cached file
        let target = wd_path.join("cached_file");
        testutil::write_file(&target, "cached_file content").unwrap();
        fs_transfer.hash_file(target.clone()).unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        assert_that!(&target, existing_dir());


        // Dir vs uncached file
        let target = wd_path.join("uncached_file");
        testutil::write_file(&target, "uncached_file content").unwrap();
        fs_transfer.hash_file(target.clone()).unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        assert_that!(&target, existing_dir());


        // Dir vs empty dir
        let target = wd_path.join("empty_dir");
        create_dir_all(&target).unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        assert_that!(&target, existing_dir());
        assert_that!(&target.join("file1"), existing_file());


        // Dir vs non-empty dir
        let target = wd_path.join("non_empty_dir");
        write_files!{
            target;
            "target_file1" => "target_file1 content",
        };

        fs_transfer.extract_object(&hash, &target).unwrap();
        assert_that!(&target, existing_dir());
        assert_that!(&target.join("file1"), existing_file());
        assert_that!(&target.join("file2"), existing_file());
        assert_that!(&target.join("target_file1"), existing_file());
    }
}
