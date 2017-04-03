//! Functionality for transfering files between filesystem and object store

use dag::ObjectKey;
use dag::ObjectSize;
use dag::Tree;
use error::*;
use file_store::FileStore;
use file_store::FileWalkNode;
use ignore::IgnoreList;
use object_store::ObjectStore;
use object_store::ObjectWalkNode;
use progress::ProgressCounter;
use progress::std_err_watch;
use status::*;
use std::fs::create_dir;
use std::fs::remove_dir_all;
use std::fs::remove_file;
use std::io::Write;
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

    /// Hash by parent object key, path to hash, and estimated hash bytes
    pub fn hash_obj_file(&mut self,
                         parent: Option<ObjectKey>,
                         path: &Path)
                         -> Result<ObjectKey> {

        let parent =
            parent.and_then_try(|hash| self.object_store.lookup_node(hash))?;
        let path = Some(self.file_store.lookup_node(path.to_owned())?);
        let node = (parent, path);

        let combo = (&self.object_store, &self.file_store);

        // Estimate
        let mut op = TransferEstimateOp::new();
        combo.walk_node(&mut op, node.clone())?;

        // Store
        let prog = ProgressCounter::arc("Storing", op.estimate());
        let prog_clone = prog.clone();

        let mut op = HashAndStoreOp {
            fs_transfer: self,
            progress: &*prog.clone(),
        };

        let prog_thread = thread::spawn(move || std_err_watch(prog_clone));

        let hash = combo.walk_node(&mut op, node)?
            .ok_or_else(|| Error::from("Nothing to hash (all ignored?)"))?;

        prog.finish();
        prog_thread.join().unwrap();
        Ok(hash)
    }

    /// Extract a file or directory from the object store to the filesystem
    pub fn extract_object(&self, hash: &ObjectKey, path: &Path) -> Result<()> {

        let obj: ObjectWalkNode = self.object_store
            .lookup_node(hash.to_owned())?;
        let file: Option<FileWalkNode> =
            self.file_store.lookup_node(path.to_owned()).ok();

        let combo = (&self.file_store, &self.object_store);

        // Estimate
        let mut op = TransferEstimateOp::new();
        let node = (file.as_ref().map(|n| n.clone().into()),
                    Some(obj.clone().into()));
        combo.walk_node(&mut op, node)?;

        // Checkout
        let prog = ProgressCounter::arc("Extracting", op.estimate());
        let prog_clone = prog.clone();
        let prog_thread = thread::spawn(move || std_err_watch(prog_clone));

        let mut op = CheckoutOp {
            file_store: &self.file_store,
            object_store: &self.object_store,
            extract_root: path,
            progress: &prog,
        };
        let node = (file, Some(obj));
        combo.walk_node(&mut op, node)
            .chain_err(|| {
                format!("Could not extract {} to {}", hash, path.display())
            })?;

        prog.finish();
        prog_thread.join().unwrap();
        Ok(())
    }

    fn hash_file(&self,
                 file_path: &Path,
                 progress: &ProgressCounter)
                 -> Result<ObjectKey> {
        self.file_store.hash_file(file_path, &self.object_store, progress)
    }

    pub fn extract_file(&self,
                        hash: &ObjectKey,
                        path: &Path,
                        progress: &ProgressCounter)
                        -> Result<()> {
        self.file_store.extract_file(&self.object_store, hash, path, progress)
    }
}


type CompareNode = (Option<ComparableNode>, Option<ComparableNode>);



pub struct ComparePrintWalkOp<'s> {
    writer: &'s mut Write,
    show_ignored: bool,
}
impl<'s> ComparePrintWalkOp<'s> {
    pub fn new(writer: &'s mut Write, show_ignored: bool) -> Self {
        ComparePrintWalkOp {
            writer: writer,
            show_ignored: show_ignored,
        }
    }
    fn status(&self, node: &CompareNode, _ps: &PathStack) -> Status {
        ComparableNode::compare_pair(node)
    }
}
impl<'s> WalkOp<CompareNode> for ComparePrintWalkOp<'s> {
    type VisitResult = ();

    fn should_descend(&mut self, ps: &PathStack, node: &CompareNode) -> bool {
        let targ = node.1.as_ref();
        let is_treeish = targ.map(|n| n.is_treeish).unwrap_or(false);
        let included = self.status(&node, ps).is_included();
        is_treeish && included
    }
    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let status = ComparableNode::compare_pair(&node);
        let show = status != Status::Unchanged &&
                   (status != Status::Ignored || self.show_ignored);
        let mut ps = ps.to_string_lossy();
        if node.1.map(|n| n.is_treeish).unwrap_or(false) {
            ps += "/";
        }
        if show {
            writeln!(self.writer, "{} {}", status.code(), ps)?;
        }
        Ok(None)
    }
}



type MultiCompareNode = (Vec<Option<ComparableNode>>, Option<ComparableNode>);

pub struct MultiComparePrintWalkOp<'s> {
    writer: &'s mut Write,
    show_ignored: bool,
}
impl<'s> MultiComparePrintWalkOp<'s> {
    pub fn new(writer: &'s mut Write, show_ignored: bool) -> Self {
        MultiComparePrintWalkOp {
            writer: writer,
            show_ignored: show_ignored,
        }
    }
    fn status(&self, node: &MultiCompareNode, _ps: &PathStack) -> Vec<Status> {
        node.0
            .iter()
            .map(|src| ComparableNode::compare(src.as_ref(), node.1.as_ref()))
            .collect()
    }
}
impl<'s> WalkOp<MultiCompareNode> for MultiComparePrintWalkOp<'s> {
    type VisitResult = ();

    fn should_descend(&mut self,
                      ps: &PathStack,
                      node: &MultiCompareNode)
                      -> bool {
        let targ = node.1.as_ref();
        let is_treeish = targ.map(|n| n.is_treeish).unwrap_or(false);
        let included = self.status(&node, ps).iter().any(|s| s.is_included());
        is_treeish && included
    }
    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: MultiCompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let status = self.status(&node, &ps);
        let show = status.iter().any(|status| {
            *status != Status::Unchanged &&
            (*status != Status::Ignored || self.show_ignored)
        });
        let mut ps = ps.to_string_lossy();
        if node.1.map(|n| n.is_treeish).unwrap_or(false) {
            ps += "/";
        }
        if show {
            for status in status {
                write!(self.writer, "{}", status.code())?;
            }
            writeln!(self.writer, " {}", ps)?;
        }
        Ok(None)
    }
}


pub struct TransferEstimateOp {
    acc: ObjectSize,
}
impl TransferEstimateOp {
    pub fn new() -> Self { TransferEstimateOp { acc: 0 } }
    pub fn estimate(&self) -> ObjectSize { self.acc }
    fn status(&self, node: &CompareNode, _ps: &PathStack) -> Status {
        ComparableNode::compare_pair(node)
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
                  ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let status = ComparableNode::compare_pair(&node);
        let size = node.1.as_ref().map(|n| n.file_size).unwrap_or(0);
        if status.needs_transfer() {
            self.acc += size;
        }
        trace!("{} {} -- {} to transfer, {} total",
               status.code(),
               ps.display(),
               size,
               self.acc);
        Ok(None)
    }
}




pub struct HashAndStoreOp<'a, 'b> {
    fs_transfer: &'a FsTransfer,
    progress: &'b ProgressCounter,
}
impl<'a, 'b> WalkOp<CompareNode> for HashAndStoreOp<'a, 'b> {
    type VisitResult = ObjectKey;

    fn should_descend(&mut self, _ps: &PathStack, node: &CompareNode) -> bool {
        let status = ComparableNode::compare_pair(node);
        node.1.as_ref().map(|ref n| n.is_treeish).unwrap_or(false) &&
        status.is_included()
    }

    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let status = ComparableNode::compare_pair(&node);
        let file_hash = node.1.as_ref().and_then(|n| n.hash);
        let file_path = node.1.as_ref().and_then(|n| n.fs_path.as_ref());
        match (status.is_included(), file_hash, &file_path) {
            (false, _, _) => {
                debug!("{} {} - skipping", status.code(), ps.display());
                Ok(None)
            }
            (true, Some(hash), _) => {
                debug!("{} {} - including with known hash {}",
                       status.code(),
                       ps.display(),
                       hash);
                Ok(Some(hash))
            }
            (true, None, &Some(ref fs_path)) => {
                debug!("{} {} - hashing", status.code(), ps.display());
                let hash = self.fs_transfer
                    .hash_file(fs_path.as_path(), self.progress)?;
                Ok(Some(hash))
            }
            (true, None, &None) => {
                bail!("{} {} - Node has neither known hash nor fs_path",
                      status.code(),
                      ps.display());
            }
        }
    }

    fn post_descend(&mut self,
                    ps: &PathStack,
                    _node: CompareNode,
                    children: ChildMap<Self::VisitResult>)
                    -> Result<Option<Self::VisitResult>> {
        if children.is_empty() {
            debug!("  {} - dropping empty dir", ps.display());
            return Ok(None);
        }
        let mut tree = Tree::new();
        for (name, hash) in children {
            tree.insert(name, hash);
        }
        let hash = self.fs_transfer.store_object(&tree)?;
        debug!("  {} - storing tree {}", ps.display(), hash);
        Ok(Some(hash))
    }
}



type CheckoutNode = (Option<FileWalkNode>, Option<ObjectWalkNode>);

/// An operation that walks a Tree (or Commit) object to extract it to disk
pub struct CheckoutOp<'a> {
    file_store: &'a FileStore,
    object_store: &'a ObjectStore,
    extract_root: &'a Path,
    progress: &'a ProgressCounter,
}
impl<'a> WalkOp<CheckoutNode> for CheckoutOp<'a> {
    type VisitResult = ();

    fn should_descend(&mut self, _ps: &PathStack, node: &CheckoutNode) -> bool {
        node.1.map(|n| n.object_type.is_treeish()).unwrap_or(false)
    }

    fn pre_descend(&mut self,
                   ps: &PathStack,
                   _node: &CheckoutNode)
                   -> Result<()> {
        let path = ps.join_to(self.extract_root);
        create_dir_clobber(&path)
    }

    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: CheckoutNode)
                  -> Result<Option<Self::VisitResult>> {
        let file = node.0;
        let obj = node.1;
        let status = ComparableNode::compare_into(file.clone(), obj.clone());

        let path = if &**ps == Path::new("") {
            self.extract_root.to_owned()
        } else {
            ps.join_to(self.extract_root)
        };

        if status == Status::Delete {
            let file = file.unwrap(); // safe to unwrap
            if file.metadata.is_dir() {
                debug!("Checkout: Removing dir  {}", path.display());
                remove_dir_all(&file.path)?;
            } else {
                debug!("Checkout: Removing file {}", path.display());
                remove_file(&file.path)?;
            }
        }

        if status.needs_transfer() {
            let hash = obj.unwrap().hash; // safe to unwrap
            self.file_store
                .extract_file(self.object_store, &hash, &path, self.progress)
                .chain_err(|| {
                    format!("Checkout: Could not extract object {} to {}",
                            hash,
                            path.display())
                })?;
        }

        Ok(None)
    }
}



type ThreeWayMergeNode = (Vec<Option<ObjectWalkNode>>, Option<FileWalkNode>);
enum MergeSlot {
    Common = 0,
    Theirs = 1,
}
pub struct ThreeWayMergeWalkOp<'a> {
    fs_transfer: &'a FsTransfer,
    base_path: &'a Path,
    progress: &'a ProgressCounter,
}
impl<'a> ThreeWayMergeWalkOp<'a> {
    pub fn new(fs_transfer: &'a FsTransfer,
               base_path: &'a Path,
               progress: &'a ProgressCounter)
               -> Self {
        ThreeWayMergeWalkOp {
            base_path: base_path,
            fs_transfer: fs_transfer,
            progress: progress,
        }
    }
}
impl<'a> WalkOp<ThreeWayMergeNode> for ThreeWayMergeWalkOp<'a> {
    type VisitResult = ();

    fn should_descend(&mut self,
                      _ps: &PathStack,
                      node: &ThreeWayMergeNode)
                      -> bool {
        let is_dir = |n: &Option<FileWalkNode>| {
            n.as_ref().map(|n| n.metadata.is_dir()).unwrap_or(false)
        };
        let is_tree = |n: &Option<ObjectWalkNode>| {
            n.as_ref().map(|n| n.object_type.is_treeish()).unwrap_or(false)
        };

        let wd_is_dir = is_dir(&node.1);
        let wd_is_ignore = &node.1.as_ref().map(|n| n.ignored).unwrap_or(false);
        let theirs_is_tree = is_tree(&node.0[MergeSlot::Theirs as usize]);

        wd_is_dir && !wd_is_ignore || theirs_is_tree
    }
    fn pre_descend(&mut self,
                   ps: &PathStack,
                   _node: &ThreeWayMergeNode)
                   -> Result<()> {
        let path = ps.join_to(self.base_path);
        create_dir_clobber(&path)
    }
    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: ThreeWayMergeNode)
                  -> Result<Option<Self::VisitResult>> {
        let path = ps.join_to(self.base_path);

        let wd = match node.1 {
            None => None,
            Some(FileWalkNode { hash: Some(h), .. }) => Some(h),
            Some(FileWalkNode { ignored: true, .. }) => None,
            Some(FileWalkNode { hash: None, .. }) => {
                Some(self.fs_transfer.hash_file(&path, &self.progress)?)
            }
        };
        let common = node.0[MergeSlot::Common as usize].map(|n| n.hash);
        let theirs = node.0[MergeSlot::Theirs as usize].map(|n| n.hash);

        #[derive(Debug,Clone,Copy)]
        enum Action {
            KeepWd,
            KeepTheirs,
            Conflict,
        }

        let action = match (common, wd, theirs) {
            (_, w, t) if w == t => Action::KeepWd,
            (c, _, t) if c == t => Action::KeepWd,
            (c, w, _) if c == w => Action::KeepTheirs,
            (_, _, _) => Action::Conflict,
        };

        trace!("{}: common: {:?}, wd: {:?}, theirs: {:?} => {:?}",
               ps.display(),
               common,
               wd,
               theirs,
               action);

        match action {
            Action::KeepWd => (),
            Action::KeepTheirs => {
                match theirs {
                    Some(t) => {
                        self.fs_transfer
                            .extract_file(&t, &path, &self.progress)?;
                    }
                    None => {
                        if path.exists() {
                            remove_file(path)?;
                        }
                    }
                }
            }
            Action::Conflict => {
                bail!("Conflict not implemented. common: {:?}, wd: {:?}, \
                       theirs: {:?}",
                      common,
                      wd,
                      theirs);
            }
        }
        Ok(None)
    }
}



fn create_dir_clobber(path: &Path) -> Result<()> {
    if !path.is_dir() {
        if path.exists() {
            debug!("Removing file {}", path.display());
            remove_file(&path)?;
        }
        debug!("Creating dir  {}", path.display());
        create_dir(&path)?;
    }
    Ok(())
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
        let hash = fs_transfer.hash_obj_file(None, &filepath).unwrap();

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
        let (temp, fs_transfer) = create_temp_repo("object_store");

        let out_file = temp.path().join("foo");
        let hash = Blob::from("12345").calculate_hash();

        let result = fs_transfer.extract_object(&hash, &out_file);
        assert!(result.is_err());
    }
}
