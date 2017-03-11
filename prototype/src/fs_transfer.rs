//! Functionality for transfering files between filesystem and object store

use cache::AllCaches;
use cache::CacheStatus;
use cache::FileStats;
use dag::ObjectHandle;
use dag::ObjectKey;
use dag::ObjectSize;
use dag::Tree;
use error::*;
use human_readable::human_bytes;
use ignore::IgnoreList;
use object_store::ObjectStore;
use object_store::ObjectWalkNode;
use rolling_hash::read_file_objects;
use std::collections::BTreeMap;
use std::fs::File;
use std::fs::Metadata;
use std::fs::OpenOptions;
use std::fs::create_dir;
use std::fs::read_dir;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use walker::*;


pub struct FsTransfer {
    pub object_store: ObjectStore,
    pub cache: AllCaches,
    pub ignored: IgnoreList,
}

impl_deref_mut!(FsTransfer => ObjectStore, object_store);

/// Constructors and high-level methods
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
        debug!("Hashing object, with framework");
        let hash_plan =
            self.walk_handle(&mut FsOnlyPlanBuilder, path.to_owned())?;
        if hash_plan.unhashed_size() > 0 {
            stderrln!("{} to hash. Hashing...",
                      human_bytes(hash_plan.unhashed_size()));
        }
        hash_plan.walk(&mut HashAndStoreOp { fs_transfer: self })?
            .ok_or_else(|| Error::from("Nothing to hash (all ignored?)"))
    }
}

/// Methods for hashing and storing files
impl FsTransfer {
    fn hash_file(&mut self, file_path: &Path) -> Result<ObjectKey> {
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

        self.cache
            .insert(file_path.to_owned(), file_stats, last_hash.to_owned())?;

        Ok(last_hash)
    }
}

/// Methods for extracting objects back onto disk
impl FsTransfer {
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

    fn extract_object_open(&mut self,
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

struct PathWalkNode {
    path: PathBuf,
    metadata: Metadata,
    hash: Option<ObjectKey>,
    ignored: bool,
}

impl NodeLookup<PathBuf, PathWalkNode> for FsTransfer {
    fn lookup_node(&mut self, path: PathBuf) -> Result<PathWalkNode> {
        let meta = path.metadata()?;
        let hash;
        if meta.is_file() {
            hash = match self.cache
                .check_with(&path, &meta.clone().into())? {
                CacheStatus::Cached { hash } => Some(hash),
                _ => None,
            };
        } else {
            hash = None;
        }
        Ok(PathWalkNode {
            hash: hash,
            ignored: self.ignored.ignores(path.as_path()),
            path: path,
            metadata: meta,
        })
    }
}

impl NodeReader<PathWalkNode> for FsTransfer {
    fn read_children(&mut self,
                     node: &PathWalkNode)
                     -> Result<ChildMap<PathWalkNode>> {
        let mut children = BTreeMap::new();
        for entry in read_dir(&node.path)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name_or_err()?
                .to_os_string()
                .into_string()
                .map_err(|e| format!("Bad UTF-8 in name: {:?}", e))?;
            let node = self.lookup_node(path.clone())?;
            children.insert(name, node);
        }
        Ok(children)
    }
}


pub struct FileLookup {
    cache: AllCaches,
    ignored: IgnoreList,
}

impl FileLookup {
    pub fn new() -> Self {
        FileLookup {
            cache: AllCaches::new(),
            ignored: IgnoreList::default(),
        }
    }
}

impl NodeLookup<PathBuf, PathWalkNode> for FileLookup {
    fn lookup_node(&mut self, path: PathBuf) -> Result<PathWalkNode> {
        let meta = path.metadata()?;
        let hash;
        if meta.is_file() {
            hash = match self.cache
                .check_with(&path, &meta.clone().into())? {
                CacheStatus::Cached { hash } => Some(hash),
                _ => None,
            };
        } else {
            hash = None;
        }
        Ok(PathWalkNode {
            hash: hash,
            ignored: self.ignored.ignores(path.as_path()),
            path: path,
            metadata: meta,
        })
    }
}

impl NodeReader<PathWalkNode> for FileLookup {
    fn read_children(&mut self,
                     node: &PathWalkNode)
                     -> Result<ChildMap<PathWalkNode>> {
        let mut children = BTreeMap::new();
        for entry in read_dir(&node.path)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name_or_err()?
                .to_os_string()
                .into_string()
                .map_err(|e| format!("Bad UTF-8 in name: {:?}", e))?;
            let node = self.lookup_node(path.clone())?;
            children.insert(name, node);
        }
        Ok(children)
    }
}

#[derive(Clone,Copy,Eq,PartialEq,Debug)]
pub enum Status {
    Untracked,
    Ignored,
    Add,
    Offline,
    Delete,
    Unchanged,
    Modified,
    MaybeModified,
}

impl Status {
    fn code(&self) -> &'static str {
        match self {
            &Status::Untracked => "?",
            &Status::Ignored => "i",
            &Status::Add => "a",
            &Status::Offline => "o",
            &Status::Delete => "d",
            &Status::Unchanged => " ",
            &Status::Modified => "M",
            &Status::MaybeModified => "m",
        }
    }

    fn is_included_in_commit(&self) -> bool {
        match self {
            &Status::Add |
            &Status::Offline |
            &Status::Unchanged |
            &Status::Modified |
            &Status::MaybeModified => true,
            &Status::Untracked |
            &Status::Ignored |
            &Status::Delete => false,
        }
    }
}

pub struct HashPlan {
    path: PathBuf,
    is_dir: bool,
    status: Status,
    hash: Option<ObjectKey>,
    size: ObjectSize,
    children: ChildMap<HashPlan>,
}

impl HashPlan {
    fn unhashed_size(&self) -> ObjectSize {
        match self {
            &HashPlan { status, .. } if !status.is_included_in_commit() => 0,
            &HashPlan { is_dir: false, hash: None, size, .. } => size,
            _ => {
                self.children
                    .iter()
                    .map(|(_, plan)| plan.unhashed_size())
                    .sum()
            }
        }
    }
}

impl NodeWithChildren for HashPlan {
    fn children(&self) -> Option<&ChildMap<Self>> { Some(&self.children) }
}

use std::fmt;

impl<'a> fmt::Display for HashPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_dir && self.status.is_included_in_commit() {
            for (_, child) in &self.children {
                child.fmt(f)?;
            }
        } else {
            if self.status != Status::Unchanged {
                writeln!(f, "{} {}", self.status.code(), self.path.display())?
            }
        }
        Ok(())
    }
}


struct FsOnlyPlanBuilder;

impl FsOnlyPlanBuilder {
    fn status(&self, node: &PathWalkNode) -> Status {
        match node {
            &PathWalkNode { ignored: true, .. } => Status::Ignored,
            _ => Status::Add,
        }
    }
}

impl WalkOp<PathWalkNode> for FsOnlyPlanBuilder {
    type VisitResult = HashPlan;

    fn should_descend(&mut self, _ps: &PathStack, node: &PathWalkNode) -> bool {
        node.metadata.is_dir() && self.status(node).is_included_in_commit()
    }
    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: PathWalkNode)
                  -> Result<Option<Self::VisitResult>> {
        Ok(Some(HashPlan {
            status: self.status(&node),
            path: node.path,
            is_dir: node.metadata.is_dir(),
            hash: node.hash,
            size: node.metadata.len(),
            children: BTreeMap::new(),
        }))
    }
    fn post_descend(&mut self,
                    ps: &PathStack,
                    node: PathWalkNode,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        self.no_descend(ps, node).map(|result| {
            result.map(|mut plan| {
                plan.children = children;
                plan
            })
        })
    }
}

pub struct FsObjComparePlanBuilder;

type CompareNode = (Option<PathWalkNode>, Option<ObjectWalkNode>);

impl FsObjComparePlanBuilder {
    fn status(node: &CompareNode) -> Status {
        let (path_exists, path_hash, path_is_ignored) = match node.0 {
            Some(ref p) => (true, p.hash, p.ignored),
            None => (false, None, true),
        };
        let (obj_exists, obj_hash) = match node.1 {
            Some(ref o) => (true, Some(o.0)),
            None => (false, None),
        };
        match (path_exists, obj_exists, path_hash, obj_hash) {
            (true, true, Some(a), Some(b)) if a == b => Status::Unchanged,
            (true, true, Some(_), Some(_)) => Status::Modified,
            (true, true, _, _) => Status::MaybeModified,

            (true, false, _, _) if path_is_ignored => Status::Ignored,
            (true, false, _, _) => Status::Untracked,

            (false, true, _, _) => Status::Offline,

            (false, false, _, _) => unreachable!(),
        }
    }
}

impl WalkOp<(CompareNode)> for FsObjComparePlanBuilder {
    type VisitResult = HashPlan;

    fn should_descend(&mut self, _ps: &PathStack, node: &CompareNode) -> bool {
        let path_is_dir = match node.0 {
            Some(ref pwn) => pwn.metadata.is_dir(),
            None => false,
        };
        path_is_dir && Self::status(&node).is_included_in_commit()
    }
    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let status = Self::status(&node);
        match node {
            (Some(path), _) => {
                Ok(Some(HashPlan {
                    status: status,
                    path: path.path,
                    is_dir: path.metadata.is_dir(),
                    hash: path.hash,
                    size: path.metadata.len(),
                    children: BTreeMap::new(),
                }))
            }
            (None, Some(obj)) => {
                Ok(Some(HashPlan {
                    status: status,
                    hash: Some(obj.0),
                    path: "".into(),
                    is_dir: false,
                    size: 0,
                    children: BTreeMap::new(),
                }))
            }
            (None, None) => unreachable!(),
        }
    }
    fn post_descend(&mut self,
                    ps: &PathStack,
                    node: CompareNode,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        self.no_descend(ps, node).map(|result| {
            result.map(|mut plan| {
                plan.children = children;
                plan
            })
        })
    }
}

pub struct HashAndStoreOp<'a> {
    fs_transfer: &'a mut FsTransfer,
}

impl<'a> WalkOp<&'a HashPlan> for HashAndStoreOp<'a> {
    type VisitResult = ObjectKey;

    fn should_descend(&mut self, _ps: &PathStack, node: &&HashPlan) -> bool {
        node.is_dir && node.status.is_included_in_commit()
    }

    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: &HashPlan)
                  -> Result<Option<Self::VisitResult>> {
        match (node.status.is_included_in_commit(), node.hash) {
            (false, _) => Ok(None),
            (true, Some(hash)) => Ok(Some(hash)),
            (true, None) => {
                let hash = self.fs_transfer.hash_file(node.path.as_path())?;
                Ok(Some(hash))
            }
        }
    }

    fn post_descend(&mut self,
                    _ps: &PathStack,
                    _node: &HashPlan,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        if children.is_empty() {
            return Ok(None);
        }
        let mut tree = Tree::new();
        for (name, hash) in children {
            tree.insert(name, hash);
        }
        let hash = self.fs_transfer.store_object(&tree)?;
        Ok(Some(hash))
    }
}


#[cfg(test)]
mod test {
    use cache::CacheStatus;
    use dag::Blob;
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

    #[test]
    fn test_default_overwrite_policy() {
        let (temp, mut fs_transfer) = create_temp_repo("object_store");
        let wd_path = temp.path().join("work_dir");

        let source = wd_path.join("in_file");
        testutil::write_file(&source, "in_file content").unwrap();
        let hash = fs_transfer.hash_file(source.as_path()).unwrap();


        // File vs cached file
        let target = wd_path.join("cached_file");
        testutil::write_file(&target, "cached_file content").unwrap();
        fs_transfer.hash_file(target.as_path()).unwrap();

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
        fs_transfer.hash_file(target.as_path()).unwrap();

        fs_transfer.extract_object(&hash, &target).unwrap();
        assert_that!(&target, existing_dir());


        // Dir vs uncached file
        let target = wd_path.join("uncached_file");
        testutil::write_file(&target, "uncached_file content").unwrap();
        fs_transfer.hash_file(target.as_path()).unwrap();

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
