//! Working Directory: Files checked out from an ObjectStore

use constants::DEFAULT_BRANCH_NAME;
use constants::HIDDEN_DIR_NAME;
use dag::Commit;
use dag::ObjectKey;
use disk_backed::DiskBacked;
use error::*;
use file_store::FileWalkNode;
use find_repo::RepoLayout;
use fs_transfer::FsTransfer;
use object_store::ObjectStore;
use object_store::ObjectWalkNode;
use status::Status;
use status::StatusTree;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use walker::*;


/// Certain operations that a file can be marked for, such as add or delete
#[derive(Debug,Clone,Copy,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
pub enum FileMark {
    /// Mark this file for addition
    Add,
    /// Mark this file for deletion
    Delete,
}

wrapper_struct!(
#[derive(Debug,Clone,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
struct FileMarkMap(BTreeMap<PathStack, FileMark>);
);
impl FileMarkMap {
    fn new() -> Self { FileMarkMap(BTreeMap::new()) }

    fn get_ancestor(&self, ps: &PathStack) -> Option<FileMark> {
        let mut ps = ps.clone();
        loop {
            if let Some(mark) = self.get(&ps) {
                return Some(*mark);
            }
            if ps.len() == 0 {
                return None;
            } else {
                ps.pop();
            }
        }
    }
}



/// State stored in a file in the WorkDir, including current branch
#[derive(Debug,Clone,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
pub struct WorkDirState {
    parents: Vec<ObjectKey>,
    branch: Option<String>,
    marks: FileMarkMap,
}

impl Default for WorkDirState {
    fn default() -> Self {
        WorkDirState {
            parents: Vec::new(),
            branch: Some(DEFAULT_BRANCH_NAME.to_owned()),
            marks: FileMarkMap::new(),
        }
    }
}



/// The working directory, an ObjectStore plus FileStore plus state of branches
pub struct WorkDir {
    fs_transfer: FsTransfer,
    path: PathBuf,
    state: DiskBacked<WorkDirState>,
}
impl_deref_mut!(WorkDir => FsTransfer, fs_transfer);

impl WorkDir {
    fn state_path(wd_path: &Path) -> PathBuf {
        wd_path.join(HIDDEN_DIR_NAME).join("work_dir_state")
    }

    pub fn init(layout: RepoLayout) -> Result<Self> {
        let os = ObjectStore::init(layout.osd)?;
        let state = DiskBacked::new("work dir state",
                                    Self::state_path(&layout.wd));
        Ok(WorkDir {
            fs_transfer: FsTransfer::with_object_store(os),
            path: layout.wd,
            state: state,
        })
    }
    pub fn open(layout: RepoLayout) -> Result<Self> {
        Ok(WorkDir {
            fs_transfer: FsTransfer::with_repo_path(layout.osd)?,
            state: DiskBacked::read_or_default("work dir state",
                                               Self::state_path(&layout.wd))?,
            path: layout.wd,
        })
    }

    pub fn path(&self) -> &Path { &self.path }

    pub fn branch(&self) -> Option<&str> {
        self.state.branch.as_ref().map(|s| s.as_str())
    }

    pub fn head(&self) -> Option<ObjectKey> {
        match self.parents().len() {
            0 => None,
            _ => Some(self.parents()[0].to_owned()),
        }
    }

    pub fn parents(&self) -> &Vec<ObjectKey> { &self.state.parents }

    fn parents_short_hashes(&self) -> Vec<String> {
        self.state
            .parents
            .iter()
            .map(|h| h.to_short())
            .collect::<Vec<String>>()
    }

    pub fn status(&mut self) -> Result<StatusTree> {
        debug!("Current branch: {}. Parents: {}",
               self.branch().unwrap_or("<detached head>"),
               self.parents_short_hashes().join(","));

        let abs_path = self.path().to_owned();
        let rel_path = PathBuf::from("");
        let parent = match self.parents().to_owned() {
            ref v if v.len() == 1 => {
                self.try_find_tree_path(&v[0], &rel_path)?
                    .and_then_try(|hash| {
                        self.fs_transfer
                            .object_store
                            .lookup_node(hash)
                    })?
            }
            ref v if v.len() == 0 => None,
            _ => unimplemented!(),
        };
        let path = Some(self.fs_transfer.file_store.lookup_node(abs_path)?);
        let combo = (&self.fs_transfer.file_store,
                     &self.fs_transfer.object_store);
        let mut op = FsObjComparePlanBuilder { marks: &self.state.marks };
        combo.walk_node(&mut op, (path, parent))?
            .ok_or_else(|| Error::from("Nothing to hash (all ignored?)"))
    }

    pub fn mark(&mut self, path: &Path, mark: FileMark) -> Result<()> {
        let path = PathStack::from_path(path)?;
        self.state.marks.insert(path, mark);
        self.state.flush()?;
        Ok(())
    }

    pub fn commit(&mut self,
                  message: String)
                  -> Result<(Option<&str>, ObjectKey)> {

        let hash_plan = self.status()?;
        let tree_hash = self.hash_plan(&hash_plan)?;

        let commit = Commit {
            tree: tree_hash,
            parents: self.parents().to_owned(),
            message: message,
        };
        let hash = self.store_object(&commit)?;
        self.state.parents = vec![hash];
        if let Some(branch) = self.state.branch.clone() {
            self.update_ref(branch, hash)?;
        }
        self.state.marks.clear();
        self.state.flush()?;
        Ok((self.branch(), hash))
    }

    pub fn update_ref_to_head(&mut self, ref_name: &str) -> Result<ObjectKey> {
        match self.head() {
            Some(head) => {
                self.update_ref(ref_name, head)?;
                Ok(head)
            }
            None => {
                bail!("Asked to set ref '{}' to head, but no \
                                     current head (no initial commit)",
                      ref_name)
            }
        }
    }
}



/// An operation that compares files to a previous commit to build a StatusTree
///
/// Walks a filesystem tree and a Tree object in parallel, comparing them and
/// building a StatusTree. This is the basis of the status command and the first
/// step of a commit.
pub struct FsObjComparePlanBuilder<'a> {
    marks: &'a FileMarkMap,
}

type CompareNode = (Option<FileWalkNode>, Option<ObjectWalkNode>);

impl<'a> FsObjComparePlanBuilder<'a> {
    fn status(&self, node: &CompareNode, ps: &PathStack) -> Status {
        let ex_mk = self.marks.get(ps).map(|m| *m);
        let an_mk = self.marks.get_ancestor(ps);

        let (path_exists, path_hash, path_is_ignored) = match node.0 {
            Some(ref p) => (true, p.hash, p.ignored),
            None => (false, None, true),
        };
        let (obj_exists, obj_hash) = match node.1 {
            Some(ref o) => (true, Some(o.hash)),
            None => (false, None),
        };
        match (path_exists, obj_exists, path_hash, obj_hash) {
            (_, _, _, _) if an_mk == Some(FileMark::Delete) => Status::Delete,

            (true, true, Some(a), Some(b)) if a == b => Status::Unchanged,
            (true, true, Some(_), Some(_)) => Status::Modified,
            (true, true, _, _) => Status::MaybeModified,

            (true, false, _, _) if ex_mk == Some(FileMark::Add) => Status::Add,
            (true, false, _, _) if path_is_ignored => Status::Ignored,
            (true, false, _, _) if an_mk == Some(FileMark::Add) => Status::Add,
            (true, false, _, _) => Status::Untracked,

            (false, true, _, _) => Status::Offline,

            (false, false, _, _) => unreachable!(),
        }
    }
}

impl<'a> WalkOp<CompareNode> for FsObjComparePlanBuilder<'a> {
    type VisitResult = StatusTree;

    fn should_descend(&mut self, ps: &PathStack, node: &CompareNode) -> bool {
        let path_is_dir = match node.0 {
            Some(ref pwn) => pwn.metadata.is_dir(),
            None => false,
        };
        path_is_dir && self.status(&node, ps).is_included()
    }
    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: CompareNode)
                  -> Result<Option<Self::VisitResult>> {
        let status = self.status(&node, ps);
        match node {
            (Some(path), _) => {
                Ok(Some(StatusTree {
                    status: status,
                    fs_path: Some(path.path),
                    is_dir: path.metadata.is_dir(),
                    hash: path.hash,
                    targ_size: path.metadata.len(),
                    children: BTreeMap::new(),
                }))
            }
            (None, Some(obj)) => {
                Ok(Some(StatusTree {
                    status: status,
                    hash: Some(obj.hash),
                    fs_path: None,
                    is_dir: false,
                    targ_size: 0,
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

        // Convert dir node to StatusTree according to normal rules
        match self.no_descend(ps, node)? {
            Some(mut plan) => {
                // Then add children
                plan.children = children;
                Ok(Some(plan))
            }
            None => Ok(None),
        }
    }
}



#[cfg(test)]
mod test {
    use rustc_serialize::json;
    use super::*;

    #[test]
    fn test_serialize_work_dir_state() {
        let obj = WorkDirState::default();

        let encoded = json::encode(&obj).unwrap();
        // assert_eq!(encoded, "see encoded");
        let decoded: WorkDirState = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
