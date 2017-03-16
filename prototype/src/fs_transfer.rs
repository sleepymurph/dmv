//! Functionality for transfering files between filesystem and object store

use dag::ObjectKey;
use dag::Tree;
use error::*;
use file_store::FileStore;
use file_store::FileWalkNode;
use ignore::IgnoreList;
use object_store::ObjectStore;
use object_store::ObjectWalkNode;
use progress::ProgressCounter;
use progress::std_err_watch;
use status::HashPlan;
use status::Status;
use std::collections::BTreeMap;
use std::fs::create_dir;
use std::fs::remove_file;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use walker::*;


/// Combine a FileStore and an ObjectStore to provide transfers between them
pub struct FsTransfer {
    pub object_store: ObjectStore,
    pub file_store: FileStore,
}
impl_deref_mut!(FsTransfer => ObjectStore, object_store);

impl FsTransfer {
    pub fn with_object_store(object_store: ObjectStore) -> Self {
        let mut ignored = IgnoreList::default();
        ignored.insert(object_store.path());

        FsTransfer {
            object_store: object_store,
            file_store: FileStore::new(),
        }
    }

    pub fn with_repo_path(repo_path: PathBuf) -> Result<Self> {
        Ok(FsTransfer::with_object_store(ObjectStore::open(repo_path)?))
    }

    /// Check, hash, and store a file or directory
    pub fn hash_path(&mut self, path: &Path) -> Result<ObjectKey> {
        debug!("Hashing object, with framework");
        let hash_plan = self.file_store
            .walk_handle(&mut FsOnlyPlanBuilder, path.to_owned())?
            .ok_or_else(&Self::no_answer_err)?;

        self.hash_plan(&hash_plan)
    }

    pub fn hash_plan(&mut self, hash_plan: &HashPlan) -> Result<ObjectKey> {
        let progress =
            Arc::new(ProgressCounter::new("Hashing",
                                          hash_plan.unhashed_size()));

        let mut op = HashAndStoreOp {
            fs_transfer: self,
            progress: &*progress.clone(),
        };

        let prog_thread = thread::spawn(move || std_err_watch(progress));
        let hash = hash_plan.walk(&mut op)?.ok_or_else(&Self::no_answer_err)?;

        prog_thread.join().unwrap();
        Ok(hash)
    }

    fn no_answer_err() -> Error {
        Error::from("Nothing to hash (all ignored?)")
    }

    /// Extract a file or directory from the object store to the filesystem
    pub fn extract_object(&mut self,
                          hash: &ObjectKey,
                          path: &Path)
                          -> Result<()> {

        let mut op = ExtractObjectOp {
            file_store: &mut self.file_store,
            object_store: &self.object_store,
            extract_root: path,
        };

        self.object_store
            .walk_handle(&mut op, *hash)
            .chain_err(|| {
                format!("Could not extract {} to {}", hash, path.display())
            })?;
        Ok(())
    }

    fn hash_file(&mut self,
                 file_path: &Path,
                 progress: &ProgressCounter)
                 -> Result<ObjectKey> {
        self.file_store.hash_file(file_path, &mut self.object_store, progress)
    }
}



/// An operation that walks files to build a HashPlan
///
/// Only considers ignore and cache status. See FsObjComparePlanBuilder for an
/// operation that compares to a previous commit/tree.
pub struct FsOnlyPlanBuilder;

impl FsOnlyPlanBuilder {
    fn status(&self, node: &FileWalkNode) -> Status {
        match node {
            &FileWalkNode { ignored: true, .. } => Status::Ignored,
            _ => Status::Add,
        }
    }
}

impl WalkOp<FileWalkNode> for FsOnlyPlanBuilder {
    type VisitResult = HashPlan;

    fn should_descend(&mut self, _ps: &PathStack, node: &FileWalkNode) -> bool {
        node.metadata.is_dir() && self.status(node).is_included()
    }
    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: FileWalkNode)
                  -> Result<Option<Self::VisitResult>> {
        Ok(Some(HashPlan {
            status: self.status(&node),
            fs_path: Some(node.path),
            is_dir: node.metadata.is_dir(),
            hash: node.hash,
            size: node.metadata.len(),
            children: BTreeMap::new(),
        }))
    }
    fn post_descend(&mut self,
                    ps: &PathStack,
                    node: FileWalkNode,
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



/// An operation that walks a HashPlan to hash and store the files as a Tree
pub struct HashAndStoreOp<'a, 'b> {
    fs_transfer: &'a mut FsTransfer,
    progress: &'b ProgressCounter,
}

impl<'a, 'b> WalkOp<&'a HashPlan> for HashAndStoreOp<'a, 'b> {
    type VisitResult = ObjectKey;

    fn should_descend(&mut self, _ps: &PathStack, node: &&HashPlan) -> bool {
        node.is_dir && node.status.is_included()
    }

    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: &HashPlan)
                  -> Result<Option<Self::VisitResult>> {
        match (node.status.is_included(), node.hash, &node.fs_path) {
            (false, _, _) => {
                debug!("{} {} - skipping", node.status.code(), ps);
                Ok(None)
            }
            (true, Some(hash), _) => {
                debug!("{} {} - including with known hash {}",
                       node.status.code(),
                       ps,
                       hash);
                Ok(Some(hash))
            }
            (true, None, &Some(ref fs_path)) => {
                debug!("{} {} - hashing", node.status.code(), ps);
                let hash = self.fs_transfer
                    .hash_file(fs_path.as_path(), self.progress)?;
                Ok(Some(hash))
            }
            (true, None, &None) => {
                bail!("{} {} - Node has neither known hash nor fs_path",
                      node.status.code(),
                      ps);
            }
        }
    }

    fn post_descend(&mut self,
                    ps: &PathStack,
                    _node: &HashPlan,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        if children.is_empty() {
            debug!("  {} - dropping empty dir", ps);
            return Ok(None);
        }
        let mut tree = Tree::new();
        for (name, hash) in children {
            tree.insert(name, hash);
        }
        let hash = self.fs_transfer.store_object(&tree)?;
        debug!("  {} - storing tree {}", ps, hash);
        Ok(Some(hash))
    }
}



/// An operation that walks a Tree (or Commit) object to extract it to disk
pub struct ExtractObjectOp<'a> {
    file_store: &'a mut FileStore,
    object_store: &'a ObjectStore,
    extract_root: &'a Path,
}

impl<'a> ExtractObjectOp<'a> {
    fn abs_path(&self, ps: &PathStack) -> PathBuf {
        let mut abs_path = self.extract_root.to_path_buf();
        for path in ps {
            abs_path.push(path);
        }
        abs_path
    }
}

impl<'a> WalkOp<ObjectWalkNode> for ExtractObjectOp<'a> {
    type VisitResult = ();

    fn should_descend(&mut self,
                      _ps: &PathStack,
                      node: &ObjectWalkNode)
                      -> bool {
        node.1.is_treeish()
    }

    fn pre_descend(&mut self,
                   ps: &PathStack,
                   _node: &ObjectWalkNode)
                   -> Result<()> {
        let dir_path = self.abs_path(ps);
        if !dir_path.is_dir() {
            if dir_path.exists() {
                remove_file(&dir_path)?;
            }
            create_dir(&dir_path)?;
        }
        Ok(())
    }

    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: ObjectWalkNode)
                  -> Result<Option<Self::VisitResult>> {
        let abs_path = self.abs_path(ps);
        self.file_store
            .extract_file(self.object_store, &node.0, abs_path.as_path())?;
        Ok(None)
    }
}



#[cfg(test)]
mod test {
    use cache::CacheStatus;
    use dag::Blob;
    use dag::ObjectCommon;
    use dag::ObjectType;
    use rolling_hash::CHUNK_TARGET_SIZE;
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
        assert_eq!(fs_transfer.file_store
                       .cache
                       .status(&out_file, &out_file.metadata().unwrap())
                       .unwrap(),
                   CacheStatus::Cached(hash),
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
}
