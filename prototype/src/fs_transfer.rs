//! Functionality for transfering files between filesystem and object store

use dag::ObjectKey;
use dag::ObjectSize;
use dag::Tree;
use error::*;
use file_store::FileStore;
use ignore::IgnoreList;
use object_store::ObjectStore;
use object_store::ObjectWalkNode;
use progress::ProgressCounter;
use progress::std_err_watch;
use status::*;
use std::fs::create_dir;
use std::fs::remove_file;
use std::path::Path;
use std::path::PathBuf;
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
        let est = self.transfer_estimate(None, path)?;
        self.hash_obj_file_est(None, path, est)
    }

    pub fn transfer_estimate(&self,
                             src: Option<ObjectKey>,
                             targ: &Path)
                             -> Result<ObjectSize> {
        debug!("Checking transfer size");

        let src: Option<ComparableNode> =
            src.and_then_try(|hash| self.object_store.lookup_node(hash))?;

        let targ: Option<ComparableNode> = Some(self.file_store
            .lookup_node(targ.to_path_buf())?);

        let combo = (&self.object_store, &self.file_store);

        let mut op = TransferEstimateOp::new();
        combo.walk_node(&mut op, (src, targ))?;
        Ok(op.estimate())
    }

    /// Hash by parent object key, path to hash, and estimated hash bytes
    pub fn hash_obj_file_est(&mut self,
                             src: Option<ObjectKey>,
                             targ: &Path,
                             est: ObjectSize)
                             -> Result<ObjectKey> {

        let prog = ProgressCounter::arc("Hashing", est);
        let prog_clone = prog.clone();

        let src: Option<ComparableNode> =
            src.and_then_try(|hash| self.object_store.lookup_node(hash))?;

        let targ: Option<ComparableNode> = Some(self.file_store
            .lookup_node(targ.to_owned())?);

        let combo = (&self.object_store, &self.file_store);

        let mut op = HashAndStoreOp {
            fs_transfer: self,
            progress: &*prog.clone(),
        };

        let prog_thread = thread::spawn(move || std_err_watch(prog_clone));

        let hash = combo.walk_node(&mut op, (src, targ))?
            .ok_or_else(|| Error::from("Nothing to hash (all ignored?)"))?;

        prog.finish();
        prog_thread.join().unwrap();
        Ok(hash)
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

    fn hash_file(&self,
                 file_path: &Path,
                 progress: &ProgressCounter)
                 -> Result<ObjectKey> {
        self.file_store.hash_file(file_path, &self.object_store, progress)
    }
}



type CompareNode = (Option<ComparableNode>, Option<ComparableNode>);

/// An operation that compares files to a previous commit to build a StatusTree
///
/// Walks a filesystem tree and a Tree object in parallel, comparing them and
/// building a StatusTree. This is the basis of the status command and the first
/// step of a commit.
pub struct CompareWalkOp;
impl CompareWalkOp {
    fn status(&self, node: &CompareNode, _ps: &PathStack) -> Status {
        ComparableNode::compare(&node.0, &node.1)
    }
}
impl WalkOp<CompareNode> for CompareWalkOp {
    type VisitResult = StatusTree;

    fn should_descend(&mut self, ps: &PathStack, node: &CompareNode) -> bool {
        let targ = node.1.as_ref();
        let is_treeish = targ.map(|n| n.is_treeish).unwrap_or(false);
        let included = self.status(&node, ps).is_included();
        is_treeish && included
    }
    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        Ok(Some(StatusTree::compare(&node.0, &node.1)))
    }
    fn post_descend(&mut self,
                    ps: &PathStack,
                    node: CompareNode,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        // Convert dir node to StatusTree according to normal rules,
        // then add children
        Ok(self.no_descend(ps, node)?.map(|mut plan| {
            plan.children = children;
            plan
        }))
    }
}



/// An operation that compares files to a previous commit to build a StatusTree
///
/// Walks a filesystem tree and a Tree object in parallel, comparing them and
/// building a StatusTree. This is the basis of the status command and the first
/// step of a commit.
pub struct TransferEstimateOp {
    acc: ObjectSize,
}
impl TransferEstimateOp {
    pub fn new() -> Self { TransferEstimateOp { acc: 0 } }
    pub fn estimate(&self) -> ObjectSize { self.acc }
    fn status(&self, node: &CompareNode, _ps: &PathStack) -> Status {
        ComparableNode::compare(&node.0, &node.1)
    }
}
impl WalkOp<CompareNode> for TransferEstimateOp {
    type VisitResult = ();

    fn should_descend(&mut self, ps: &PathStack, node: &CompareNode) -> bool {
        let targ = node.1.as_ref();
        let is_treeish = targ.map(|n| n.is_treeish).unwrap_or(false);
        let included = self.status(&node, ps).is_included();
        is_treeish && included
    }
    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        self.acc += StatusTree::compare(&node.0, &node.1).transfer_size();
        Ok(None)
    }
}




/// An operation that walks a StatusTree to hash and store the files as a Tree
pub struct HashAndStoreOp<'a, 'b> {
    fs_transfer: &'a FsTransfer,
    progress: &'b ProgressCounter,
}
impl<'a, 'b> WalkOp<CompareNode> for HashAndStoreOp<'a, 'b> {
    type VisitResult = ObjectKey;

    fn should_descend(&mut self, _ps: &PathStack, node: &CompareNode) -> bool {
        let status = ComparableNode::compare(&node.0, &node.1);
        node.1.as_ref().map(|ref n| n.is_treeish).unwrap_or(false) &&
        status.is_included()
    }

    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let node = StatusTree::compare(&node.0, &node.1);
        match (node.status.is_included(), node.targ_hash, &node.fs_path) {
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
                    _node: CompareNode,
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
        node.object_type.is_treeish()
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
            .extract_file(self.object_store, &node.hash, abs_path.as_path())?;
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
