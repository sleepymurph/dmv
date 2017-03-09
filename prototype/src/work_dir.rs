//! Working Directory: Files checked out from an ObjectStore

use constants::DEFAULT_BRANCH_NAME;
use constants::HIDDEN_DIR_NAME;
use dag::Commit;
use dag::ObjectKey;
use disk_backed::DiskBacked;
use encodable;
use error::*;
use find_repo::RepoLayout;
use fs_transfer::FsTransfer;
use item::LoadItems;
use item::PartialItem;
use object_store::ObjectStore;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
        let a = match self.parents().to_owned() {
            ref v if v.len() == 1 => self.try_find_tree_path(&v[0], &rel_path)?,
            ref v if v.len() == 0 => None,
            _ => unimplemented!(),
        };
        let mut a = a.and_then_try(|a| self.load_item(a))?;
        let mut b = Some(self.load_item(abs_path)?);
        self.compare_option_items(rel_path.as_ref(), a.as_mut(), b.as_mut())
    }


    fn compare_option_items(&mut self,
                            rel_path: &Path,
                            a: Option<&mut PartialItem>,
                            b: Option<&mut PartialItem>)
                            -> Result<Status> {
        use self::Status::*;
        use self::LeafStatus::*;
        let mark = self.state.marks.get(rel_path).map(ToOwned::to_owned);
        match (a, b, mark) {
            (Some(a), Some(b), _) => self.compare_items(rel_path, a, b),
            (None, Some(&mut PartialItem { mark_ignore: true, .. }), _) => {
                Ok(Leaf(Ignored))
            }
            (None, Some(_), Some(FileMark::Add)) => Ok(Leaf(Add)),
            (None, Some(_), _) => Ok(Leaf(Untracked)),
            (Some(_), None, Some(FileMark::Delete)) => Ok(Leaf(Delete)),
            (Some(_), None, _) => Ok(Leaf(Offline)),
            (None, None, _) => bail!("Nothing to compare"),
        }
    }

    fn compare_items(&mut self,
                     rel_path: &Path,
                     a: &mut PartialItem,
                     b: &mut PartialItem)
                     -> Result<Status> {
        use self::Status::*;
        use self::LeafStatus::*;
        use item::ItemClass::*;
        match (a, b) {
            (&mut PartialItem { hash: Some(a), .. },
             &mut PartialItem { hash: Some(b), .. }) if a == b => {
                Ok(Leaf(Unchanged))
            }

            (&mut PartialItem { class: BlobLike(_), .. },
             &mut PartialItem { class: BlobLike(_), .. }) => {
                Ok(Leaf(MaybeModified))
            }

            (&mut PartialItem { class: TreeLike(ref mut a), .. },
             &mut PartialItem { class: TreeLike(ref mut b), .. }) => {
                Ok(Status::Tree(self.compare_children(rel_path, a, b)?))
            }

            (&mut PartialItem { hash: Some(_), .. },
             &mut PartialItem { hash: Some(_), .. }) => Ok(Leaf(Modified)),

            _ => Ok(Leaf(MaybeModified)),
        }
    }

    fn compare_children(&mut self,
                        rel_path: &Path,
                        a: &mut LoadItems,
                        b: &mut LoadItems)
                        -> Result<StatusTree> {
        let a = self.load_in_place(a)?;
        let b = self.load_in_place(b)?;

        let mut statuses = StatusTree::new();
        let all_names = a.keys()
            .chain(b.keys())
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        for name in all_names {
            let a = a.get_mut(&name);
            let b = b.get_mut(&name);
            let rel_path = rel_path.join(&name);
            let status = self.compare_option_items(&rel_path, a, b)?;
            statuses.insert(name.to_owned(), status);
        }
        Ok(statuses)
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
