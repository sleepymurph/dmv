//! Working Directory: Files checked out from an ObjectStore

use cache::CacheStatus;
use constants::DEFAULT_BRANCH_NAME;
use constants::HIDDEN_DIR_NAME;
use dag::Commit;
use dag::ObjectHandle;
use dag::ObjectKey;
use dag::Tree;
use disk_backed::DiskBacked;
use encodable;
use error::*;
use find_repo::RepoLayout;
use fs_transfer::FsTransfer;
use fsutil::is_empty_dir;
use object_store::ObjectStore;
use std::collections::BTreeMap;
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

type StatusTree = BTreeMap<PathBuf, Status>;

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

#[derive(Debug,Clone,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
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
        match self.parents().to_owned() {
            ref v if v.len() == 1 => {
                let key = self.try_find_tree_path(&v[0], &rel_path)?;
                self.check_status_inner(&abs_path, &rel_path, key)
            }
            ref v if v.len() == 0 => {
                self.check_status_inner(&abs_path, &rel_path, None)
            }
            _ => unimplemented!(),
        }
    }

    fn check_status_inner(&mut self,
                          abs_path: &Path,
                          rel_path: &Path,
                          key: Option<ObjectKey>)
                          -> Result<Status> {
        trace!("comparing {} to {:?}", rel_path.display(), key);
        use self::Status::*;
        use self::LeafStatus::*;
        match (key, abs_path.exists()) {
            (None, false) => {
                bail!("Path does not exist: {}", rel_path.display())
            }
            (None, true) => {
                if self.ignored.ignores(&rel_path) || is_empty_dir(&abs_path)? {
                    return Ok(Leaf(Ignored));
                }
                Ok(Leaf(Untracked))
            }
            (Some(_), false) => Ok(Leaf(Offline)),
            (Some(key), true) => self.compare_path(abs_path, rel_path, &key),
        }
    }

    fn compare_path(&mut self,
                    abs_path: &Path,
                    rel_path: &Path,
                    key: &ObjectKey)
                    -> Result<Status> {
        use self::Status::*;
        use self::LeafStatus::*;
        let path_meta =
            abs_path.metadata()
                .chain_err(|| {
                    format!("getting metadata for {}", rel_path.display())
                })?;

        if path_meta.is_file() {
            match self.cache.check_with(&abs_path, &path_meta.into())? {
                CacheStatus::Cached { hash: cached } if &cached == key => {
                    Ok(Leaf(Unchanged))
                }
                CacheStatus::Cached { .. } => Ok(Leaf(Modified)),
                CacheStatus::Modified { .. } => Ok(Leaf(Modified)),
                CacheStatus::NotCached { .. } => Ok(Leaf(MaybeModified)),
            }

        } else if path_meta.is_dir() {
            match self.open_object(&key)? {
                // Was a file, now a dir. Definitely modified.
                ObjectHandle::Blob(_) |
                ObjectHandle::ChunkedBlob(_) => Ok(Leaf(Modified)),

                // Both dirs, need to compare recursively.
                ObjectHandle::Tree(raw) => {
                    let tree = raw.read_content()?;
                    self.compare_dir(abs_path, rel_path, tree)
                        .map(|status_tree| Tree(status_tree))
                }
                ObjectHandle::Commit(raw) => {
                    let tree = raw.read_content()
                        .and_then(|commit| self.open_tree(&commit.tree))?;
                    self.compare_dir(abs_path, rel_path, tree)
                        .map(|status_tree| Tree(status_tree))
                }
            }


        } else {
            unimplemented!()
        }
    }

    fn compare_dir(&mut self,
                   abs_path: &Path,
                   rel_path: &Path,
                   tree: Tree)
                   -> Result<StatusTree> {
        use self::Status::*;
        use self::LeafStatus::*;

        let mut status = StatusTree::new();
        // Check all child paths in directory
        for entry in abs_path.read_dir()? {
            let entry = entry?;
            let ch_abs_path = entry.path();
            let ch_name = PathBuf::from(ch_abs_path.file_name_or_err()?);
            let ch_rel_path = rel_path.join(&ch_name);
            let ch_key = tree.get(&ch_name).map(|k| k.to_owned());
            let ch_status =
                self.check_status_inner(&ch_abs_path, &ch_rel_path, ch_key)?;
            status.insert(ch_name, ch_status);
        }
        // Check missing files
        for ch_name in tree.keys() {
            status.entry(ch_name.to_owned())
                .or_insert_with(|| Leaf(Offline));
        }
        Ok(status)
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
