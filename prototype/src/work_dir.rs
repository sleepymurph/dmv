//! Working Directory: Files checked out from an ObjectStore

use constants::DEFAULT_BRANCH_NAME;
use constants::HIDDEN_DIR_NAME;
use dag::Commit;
use dag::ObjectKey;
use dag::ObjectType;
use dag::Tree;
use disk_backed::DiskBacked;
use encodable;
use error::*;
use find_repo::RepoLayout;
use fs_transfer::FsTransfer;
use item::PartialItem;
use item::PartialTree;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug,Clone,Hash,PartialEq,Eq)]
pub enum LeafStatus {
    /// Path is new and untracked
    Untracked,

    /// Path is untracked and ignored
    Ignored,

    /// Path is new and marked for addition
    Add,

    /// Path is missing because it is not checked out
    Offline,

    /// Path is missing because it is marked for deletion
    Delete,

    /// Path exists and matches cache
    Unchanged,

    /// Path exists and is newer than cache
    Modified,

    /// Path exists but is not cached, so modified status is unknown
    MaybeModified,
}

impl LeafStatus {
    fn code(&self) -> &'static str {
        match self {
            &LeafStatus::Untracked => "?",
            &LeafStatus::Ignored => "i",
            &LeafStatus::Add => "a",
            &LeafStatus::Offline => "o",
            &LeafStatus::Delete => "d",
            &LeafStatus::Unchanged => " ",
            &LeafStatus::Modified => "M",
            &LeafStatus::MaybeModified => "m",
        }
    }
}

#[derive(Debug,Clone,Hash,PartialEq,Eq)]
pub enum Status {
    Leaf(LeafStatus),
    Tree(StatusTree),
}

type StatusTree = BTreeMap<OsString, Status>;

impl Status {
    fn write(&self, f: &mut fmt::Formatter, prefix: &PathBuf) -> fmt::Result {
        match self {
            &Status::Leaf(LeafStatus::Ignored) => Ok(()),
            &Status::Leaf(LeafStatus::Unchanged) => Ok(()),
            &Status::Leaf(ref leaf) => {
                write!(f, "{} {}\n", leaf.code(), prefix.display())
            }
            &Status::Tree(ref tree) => {
                for (path, status) in tree {
                    status.write(f, &prefix.join(path))?;
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.write(f, &PathBuf::from(""))
    }
}

#[derive(Debug,Clone,Copy,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
pub enum FileMark {
    /// Mark this file for addition
    Add,
    /// Mark this file for deletion
    Delete,
}

type FileMarkMap = BTreeMap<encodable::PathBuf, FileMark>;

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

pub struct WorkDir {
    fs_transfer: FsTransfer,
    path: PathBuf,
    state: DiskBacked<WorkDirState>,
}

fn work_dir_state_path(wd_path: &Path) -> PathBuf {
    wd_path.join(HIDDEN_DIR_NAME).join("work_dir_state")
}


impl WorkDir {
    pub fn init(layout: RepoLayout) -> Result<Self> {
        let os = ObjectStore::init(layout.osd)?;
        let state = DiskBacked::new("work dir state",
                                    work_dir_state_path(&layout.wd));
        Ok(WorkDir {
            fs_transfer: FsTransfer::with_object_store(os),
            path: layout.wd,
            state: state,
        })
    }
    pub fn open(layout: RepoLayout) -> Result<Self> {
        Ok(WorkDir {
            fs_transfer: FsTransfer::with_repo_path(layout.osd)?,
            state:
                DiskBacked::read_or_default("work dir state",
                                            work_dir_state_path(&layout.wd))?,
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

    pub fn check_status(&mut self) -> Result<Status> {

        let abs_path = self.path().to_owned();
        let rel_path = PathBuf::from("");
        let key = match self.parents().to_owned() {
            ref v if v.len() == 1 => self.try_find_tree_path(&v[0], &rel_path)?,
            ref v if v.len() == 0 => None,
            _ => unimplemented!(),
        };
        let partial = self.fs_transfer.check_status(&abs_path)?;
        self.check_status_inner(&rel_path, key, partial)
    }

    fn check_status_inner(&mut self,
                          rel_path: &Path,
                          key: Option<ObjectKey>,
                          partial: PartialItem)
                          -> Result<Status> {
        trace!("comparing {} to {:?}", rel_path.display(), key);
        use self::Status::*;
        use self::LeafStatus::*;

        let mark = self.state.marks.get(rel_path.into()).map(ToOwned::to_owned);

        debug!("check_status_inner: {}, ignore: {}",
               rel_path.display(),
               partial.mark_ignore);
        match (key, mark, partial.is_vacant()) {
            (None, Some(FileMark::Add), _) => Ok(Leaf(Add)),
            (None, _, true) => Ok(Leaf(Ignored)),
            (None, _, _) => Ok(Leaf(Untracked)),
            (Some(key), _, _) => self.compare_path(rel_path, &key, partial),
        }
    }

    fn compare_path(&mut self,
                    rel_path: &Path,
                    key: &ObjectKey,
                    partial: PartialItem)
                    -> Result<Status> {
        use self::Status::*;
        use self::LeafStatus::*;
        use item::ItemClass::*;
        use item::LoadItems::*;

        debug!("compare_path: {}, ignore: {}",
               rel_path.display(),
               partial.mark_ignore);
        match partial {
            PartialItem { hash: Some(ref cached), .. } if cached == key => {
                Ok(Leaf(Unchanged))
            }
            PartialItem { hash: Some(_), .. } => Ok(Leaf(Modified)),
            PartialItem { mark_ignore: true, .. } => Ok(Leaf(Ignored)),
            PartialItem { class: BlobLike(_), .. } => Ok(Leaf(MaybeModified)),
            PartialItem { class: TreeLike(Loaded(ref partial)), .. } => {
                match self.open_object(&key)?.header().object_type {
                    ObjectType::Blob | ObjectType::ChunkedBlob => {
                        Ok(Leaf(Modified))
                    }
                    ObjectType::Tree | ObjectType::Commit => {
                        let tree = self.open_tree(&key)?;
                        self.compare_dir(rel_path, tree, partial)
                            .map(|st| Tree(st))
                    }
                }
            }
            _ => unimplemented!(),
        }
    }

    fn compare_dir(&mut self,
                   rel_path: &Path,
                   tree: Tree,
                   partial: &PartialTree)
                   -> Result<StatusTree> {
        use self::Status::*;
        use self::LeafStatus::*;

        let mut status = StatusTree::new();
        // Check all child paths in directory
        for (ch_name, ch_partial) in partial.iter() {
            let ch_rel_path = rel_path.join(&ch_name);
            let ch_key = tree.get(ch_name).map(|k| k.to_owned());
            let ch_status = self.check_status_inner(&ch_rel_path,
                                    ch_key,
                                    ch_partial.to_owned())?;
            status.insert(ch_name.to_owned(), ch_status);
        }
        // Check missing files
        for ch_name in tree.keys() {
            status.entry(ch_name.to_owned())
                .or_insert_with(|| Leaf(Offline));
            // TODO: Check ignores
        }
        Ok(status)
    }

    pub fn mark_for_add(&mut self, path: PathBuf) -> Result<()> {
        if !path.exists() {
            bail!("Path does not exist: {}", path.display());
        }
        self.state.marks.insert(path.into(), FileMark::Add);
        self.state.flush()?;
        Ok(())
    }

    pub fn commit(&mut self,
                  message: String)
                  -> Result<(Option<&str>, ObjectKey)> {
        debug!("Current branch: {}. Parents: {}",
               self.branch().unwrap_or("<detached head>"),
               self.parents_short_hashes().join(","));

        let path = self.path().to_owned();
        let tree_hash = self.hash_path(&path)?;
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

impl_deref_mut!(WorkDir => FsTransfer, fs_transfer);

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
