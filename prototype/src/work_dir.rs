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
use status::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use walker::*;


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
        let mut op = FileObjectCompareWalkOp { marks: &self.state.marks };
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
pub struct FileObjectCompareWalkOp<'a> {
    marks: &'a FileMarkMap,
}

type FileObjectNode = (Option<FileWalkNode>, Option<ObjectWalkNode>);

impl<'a> FileObjectCompareWalkOp<'a> {
    fn status(&self, node: &FileObjectNode, ps: &PathStack) -> Status {
        let path = node.0.as_ref();
        let obj = node.1.as_ref();
        StatusCompare {
                src_exists: path.is_some(),
                src_hash: path.and_then(|p| p.hash),
                src_is_ignored: path.map(|p| p.ignored).unwrap_or(false),

                targ_exists: obj.is_some(),
                targ_hash: obj.map(|n| n.hash),

                exact_mark: self.marks.get(ps).map(|m| *m),
                ancestor_mark: self.marks.get_ancestor(ps),
            }
            .compare()
    }
}

impl<'a> WalkOp<FileObjectNode> for FileObjectCompareWalkOp<'a> {
    type VisitResult = StatusTree;

    fn should_descend(&mut self,
                      ps: &PathStack,
                      node: &FileObjectNode)
                      -> bool {
        let path = node.0.as_ref();
        let is_dir = path.map(|p| p.metadata.is_dir()).unwrap_or(false);
        let included = self.status(&node, ps).is_included();
        is_dir && included
    }
    fn no_descend(&mut self,
                  ps: &PathStack,
                  node: FileObjectNode)
                  -> Result<Option<Self::VisitResult>> {
        let path = node.0.as_ref();
        let obj = node.1.as_ref();
        Ok(Some(StatusTree {
            status: self.status(&node, ps),
            fs_path: path.map(|p| p.path.to_owned()),
            targ_is_dir: path.map(|p| p.metadata.is_dir()).unwrap_or(false),
            targ_size: path.map(|p| p.metadata.len()).unwrap_or(0),
            targ_hash: path.and_then(|p| p.hash).or(obj.map(|o| o.hash)),
            children: BTreeMap::new(),
        }))
    }
    fn post_descend(&mut self,
                    ps: &PathStack,
                    node: FileObjectNode,
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
