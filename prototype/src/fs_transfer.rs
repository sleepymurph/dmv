//! Functionality for transfering files between filesystem and object store

use cache::AllCaches;
use cache::FileStats;
use dag::ObjectHandle;
use dag::ObjectKey;
use dag::Tree;
use error::*;
use human_readable::human_bytes;
use ignore::IgnoreList;
use item::HashedOrNot;
use item::PartialItem;
use item::PartialTree;
use object_store::ObjectStore;
use rolling_hash::read_file_objects;
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


pub struct FsTransfer {
    pub object_store: ObjectStore,
    pub cache: AllCaches,
    pub ignored: IgnoreList,
}

impl FsTransfer {
    pub fn with_object_store(object_store: ObjectStore) -> Self {
        let mut ignored = IgnoreList::default();
        ignored.insert(object_store.path());

        FsTransfer {
            object_store: object_store,
            ignored: ignored,
            cache: AllCaches::new(),
        }
    }

    pub fn with_repo_path(repo_path: PathBuf) -> Result<Self> {
        Ok(FsTransfer::with_object_store(ObjectStore::open(repo_path)?))
    }

    /// Check, hash, and store a file or directory
    pub fn hash_path(&mut self, path: &Path) -> Result<ObjectKey> {
        let status = self.check_status(&path)?;
        if status.unhashed_size() > 0 {
            stderrln!("{} to hash. Hashing...",
                      human_bytes(status.unhashed_size()));
        }
        self.hash_object(&path, &status)
    }

    pub fn check_status(&mut self, path: &Path) -> Result<PartialItem> {

        let mark_ignore = self.ignored.ignores(path);
        let path_meta = path.metadata()
            .chain_err(|| format!("getting metadata for {}", path.display()))?;

        let mut hashed_or_not: PartialItem;

        if path_meta.is_file() {

            hashed_or_not =
                self.cache.check_with(&path, &path_meta.into())?.into();

        } else if path_meta.is_dir() {

            let mut partial = PartialTree::new();

            if !mark_ignore {
                // Don't descend into ignored directory
                for entry in try!(read_dir(path)) {
                    let entry = try!(entry);
                    let ch_path = entry.path();
                    let ch_name = PathBuf::from(ch_path.file_name_or_err()?);
                    let hashed_status = self.check_status(&ch_path)?;
                    partial.insert(ch_name, hashed_status);
                }
            }
            hashed_or_not = partial.into();

        } else {
            bail!("Path {} was neither file nor directory", path.display());
        }

        hashed_or_not.mark_ignore = mark_ignore;
        Ok(hashed_or_not)
    }


    pub fn hash_object(&mut self,
                       path: &Path,
                       status: &PartialItem)
                       -> Result<ObjectKey> {
        use item::HashedOrNot::*;
        let status = status.prune_vacant();
        match status.hon() {
            Hashed(hash) => Ok(hash.to_owned()),
            UnhashedFile(_) => self.hash_file(path.to_owned()),
            Dir(partial) => self.hash_partial_tree(&path, partial),
        }
    }

    fn hash_file(&mut self, file_path: PathBuf) -> Result<ObjectKey> {
        let file = File::open(&file_path)?;
        let file_stats = FileStats::from(file.metadata()?);
        let file = BufReader::new(file);

        return_if_cached!(self.cache, &file_path, &file_stats);
        debug!("Hashing {}", file_path.display());

        let mut last_hash = None;
        for object in read_file_objects(file) {
            let object = object?;
            self.store_object(&object)?;
            last_hash = Some(object.hash().to_owned());
        }
        let last_hash = last_hash.expect("Iterator always emits objects");

        self.cache.insert(file_path, file_stats, last_hash.to_owned())?;

        Ok(last_hash)
    }

    fn hash_partial_tree(&mut self,
                         dir_path: &Path,
                         partial: &PartialTree)
                         -> Result<ObjectKey> {

        if partial.is_vacant() {
            bail!("No children to hash (all empty dirs or ignored) in \
                   directory: {}",
                  dir_path.display());
        }

        let mut tree = Tree::new();

        for (ch_name, unknown) in partial.iter() {
            let ch_path = dir_path.join(&ch_name);

            let hash = match unknown.hon() {
                HashedOrNot::UnhashedFile(_) => self.hash_file(ch_path)?,
                HashedOrNot::Dir(partial) => {
                    if partial.is_vacant() {
                        continue;
                    }
                    self.hash_partial_tree(&ch_path, partial)?
                }
                HashedOrNot::Hashed(hash) => hash.to_owned(),
            };
            tree.insert(ch_name, hash);
        }

        self.store_object(&tree)
    }


    pub fn extract_object(&mut self,
                          hash: &ObjectKey,
                          path: &Path)
                          -> Result<()> {

        self.open_object(hash)
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
            ObjectHandle::Tree(raw) => {
                debug!("Extracting tree {} to {}", hash, path.display());
                let tree = raw.read_content()?;
                self.extract_tree_open(tree, path)
            }
            ObjectHandle::Commit(raw) => {
                debug!("Extracting commit {} to {}", hash, path.display());
                let tree = raw.read_content()
                    .and_then(|commit| self.open_tree(&commit.tree))?;
                self.extract_tree_open(tree, path)
            }
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
                trace!("Extracting blob {}", hash);
                blob.copy_content(writer)?;
            }
            ObjectHandle::ChunkedBlob(index) => {
                debug!("Reading ChunkedBlob {}", hash);
                let index = index.read_content()?;
                for offset in index.chunks {
                    debug!("{}", offset);
                    let ch_handle = self.open_object(&offset.hash)?;
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

impl_deref_mut!(FsTransfer => ObjectStore, object_store);


#[cfg(test)]
mod test {
    use cache::CacheStatus;
    use dag::Blob;
    use dag::Object;
    use dag::ObjectCommon;
    use dag::ObjectType;
    use hamcrest::prelude::*;
    use rolling_hash::CHUNK_TARGET_SIZE;
    use std::fs::create_dir_all;
    use super::*;
    use testutil;
    use testutil::tempdir::TempDir;

    fn create_temp_repo(dir_name: &str) -> (TempDir, FsTransfer) {
        let temp = in_mem_tempdir!();
        let repo_path = temp.path().join(dir_name);
        let fs_transfer = FsTransfer::with_repo_path(repo_path).unwrap();
        (temp, fs_transfer)
    }

    fn do_store_single_file_test(in_file: &[u8],
                                 expected_object_type: ObjectType) {

        let (temp, mut fs_transfer) = create_temp_repo("object_store");

        // Write input file to disk
        let filepath = temp.path().join("foo");
        testutil::write_file(&filepath, in_file).unwrap();

        // Hash input file
        let hash = fs_transfer.hash_path(&filepath).unwrap();

        // Check the object type
        let obj = fs_transfer.open_object(&hash).unwrap();
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
        assert_eq!(partial, PartialItem::from(expected_partial));

        // Hash and store files

        let hash = fs_transfer.hash_object(&wd_path, &partial).unwrap();

        let obj = fs_transfer.open_object(&hash).unwrap();
        let obj = obj.read_content().unwrap();

        assert_eq!(obj, Object::Tree(expected_tree.clone()));

        // Flush cache files
        fs_transfer.cache.flush();

        // Build partial tree again -- check how it handles the cache files

        let partial = fs_transfer.check_status(&wd_path).unwrap();
        assert_eq!(partial.prune_vacant(),
                   PartialItem::from(expected_cached_partial.prune_vacant()));
        let rehash = fs_transfer.hash_object(&wd_path, &partial).unwrap();
        assert_eq!(rehash, hash);

        // Extract and compare
        let extract_path = temp.path().join("extract_dir");
        fs_transfer.extract_object(&hash, &extract_path).unwrap();

        let extract_partial = fs_transfer.check_status(&extract_path).unwrap();
        assert_eq!(extract_partial.prune_vacant(),
                   PartialItem::from(expected_cached_partial.prune_vacant()));

        let rehash = fs_transfer.hash_object(&extract_path, &extract_partial)
            .unwrap();
        assert_eq!(rehash, hash);
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
            "foo" => PartialItem::unhashed_file(3),
            "bar" => PartialItem::unhashed_file(4),
            "baz" => PartialItem::unhashed_file(5),
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
            "foo" => PartialItem::unhashed_file(3),
            "level1" => partial_tree!{
                "bar" => PartialItem::unhashed_file(4),
                "level2" => partial_tree!{
                    "baz" => PartialItem::unhashed_file(5),
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
            "foo" => PartialItem::unhashed_file(3),
            "level1" => partial_tree!{
                "bar" => PartialItem::unhashed_file(4),
                "level2" => partial_tree!{
                    "baz" => PartialItem::unhashed_file(5),
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
            "foo" => PartialItem::unhashed_file(3),
            "empty1" => PartialItem::from(partial_tree!{
                "empty2" => PartialItem::from(partial_tree!{
                    "empty3" => PartialItem::from(PartialTree::new()),
                }),
            }),
        };

        let expected_tree = tree_object!{
            "foo" => Blob::from("123").calculate_hash(),
        };

        let expected_cached_partial = partial_tree!{
            "foo" => Blob::from("123").calculate_hash(),
            "empty1" => PartialItem::from(partial_tree!{
                "empty2" => PartialItem::from(partial_tree!{
                    "empty3" => PartialItem::from(PartialTree::new()),
                }),
            }),
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

        // Hash and store files

        let hash = fs_transfer.hash_object(&wd_path, &partial);
        assert!(hash.is_err(), "Should refuse to hash an empty directory");
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

        let hash = fs_transfer.hash_path(&source).unwrap();

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
