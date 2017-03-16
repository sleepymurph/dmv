//! Functionality for transfering files between filesystem and object store

use dag::ObjectKey;
use dag::Tree;
use error::*;
use file_store::FileStore;
use ignore::IgnoreList;
use object_store::ObjectStore;
use object_store::ObjectWalkNode;
use progress::ProgressCounter;
use progress::std_err_watch;
use status::*;
use std::collections::BTreeMap;
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
        debug!("Hashing object, with framework");
        let hash_plan;
        {
            let mut op = CompareWalkOp { marks: &FileMarkMap::add_root() };
            let combo = (&self.object_store, &self.file_store);
            let file_node = self.file_store.lookup_node(path.to_owned())?;
            let node = (None, Some(file_node));
            hash_plan = combo.walk_node(&mut op, node)?
                .ok_or_else(&Self::no_answer_err)?;
        }

        self.hash_plan(&hash_plan)
    }

    pub fn hash_plan(&mut self, hash_plan: &StatusTree) -> Result<ObjectKey> {
        let prog = ProgressCounter::arc("Hashing", hash_plan.transfer_size());

        let mut op = HashAndStoreOp {
            fs_transfer: self,
            progress: &*prog.clone(),
        };

        let prog_thread = thread::spawn(move || std_err_watch(prog));
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



type CompareNode = (Option<ComparableNode>, Option<ComparableNode>);

/// An operation that compares files to a previous commit to build a StatusTree
///
/// Walks a filesystem tree and a Tree object in parallel, comparing them and
/// building a StatusTree. This is the basis of the status command and the first
/// step of a commit.
pub struct CompareWalkOp<'a> {
    pub marks: &'a FileMarkMap,
}
impl<'a> CompareWalkOp<'a> {
    fn status(&self, node: &CompareNode, ps: &PathStack) -> Status {
        let exact_mark = self.marks.get(ps).map(|m| *m);
        let ancestor_mark = self.marks.get_ancestor(ps);
        ComparableNode::compare(&node.0, &node.1, exact_mark, ancestor_mark)
    }
}
impl<'a> WalkOp<CompareNode> for CompareWalkOp<'a> {
    type VisitResult = StatusTree;

    fn should_descend(&mut self, ps: &PathStack, node: &CompareNode) -> bool {
        let path = node.1.as_ref();
        let is_dir = path.map(|p| p.is_treeish).unwrap_or(false);
        let included = self.status(&node, ps).is_included();
        is_dir && included
    }
    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let obj = node.0.as_ref();
        let path = node.1.as_ref();
        Ok(Some(StatusTree {
            status: self.status(&node, ps),
            fs_path: path.and_then(|p| p.fs_path.to_owned()),
            targ_is_dir: path.map(|p| p.is_treeish).unwrap_or(false),
            targ_size: path.map(|p| p.file_size).unwrap_or(0),
            targ_hash: path.and_then(|p| p.hash).or(obj.and_then(|o| o.hash)),
            children: BTreeMap::new(),
        }))
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



/// An operation that walks a StatusTree to hash and store the files as a Tree
pub struct HashAndStoreOp<'a, 'b> {
    fs_transfer: &'a mut FsTransfer,
    progress: &'b ProgressCounter,
}
impl<'a, 'b> WalkOp<&'a StatusTree> for HashAndStoreOp<'a, 'b> {
    type VisitResult = ObjectKey;

    fn should_descend(&mut self, _ps: &PathStack, node: &&StatusTree) -> bool {
        node.targ_is_dir && node.status.is_included()
    }

    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: &StatusTree)
                  -> Result<Option<Self::VisitResult>> {
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
                    _node: &StatusTree,
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
