//! Working Directory: Files checked out from an ObjectStore

use constants::DEFAULT_BRANCH_NAME;
use constants::HIDDEN_DIR_NAME;
use dag::Commit;
use dag::ObjectKey;
use disk_backed::DiskBacked;
use error::*;
use find_repo::RepoLayout;
use fs_transfer::ComparePrintWalkDisplay;
use fs_transfer::FsTransfer;
use object_store::Commits;
use object_store::ObjectStore;
use object_store::RevSpec;
use status::*;
use std::path::Path;
use std::path::PathBuf;
use walker::*;


/// State stored in a file in the WorkDir, including current branch
#[derive(Debug,Clone,Hash,PartialEq,Eq,RustcEncodable,RustcDecodable)]
pub struct WorkDirState {
    parents: Vec<ObjectKey>,
    branch: Option<String>,
}

impl Default for WorkDirState {
    fn default() -> Self {
        WorkDirState {
            parents: Vec::new(),
            branch: Some(DEFAULT_BRANCH_NAME.to_owned()),
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

    /// Assume a single parent and return that (for now)
    fn parent(&self) -> Option<ObjectKey> {
        match self.parents() {
            ref v if v.len() == 1 => Some(v[0]),
            ref v if v.len() == 0 => None,
            _ => unimplemented!(),
        }
    }

    fn parents_short_hashes(&self) -> Vec<String> {
        self.state
            .parents
            .iter()
            .map(|h| h.to_short())
            .collect::<Vec<String>>()
    }

    pub fn status(&mut self,
                  show_ignored: bool,
                  rev1: Option<&str>,
                  rev2: Option<&str>)
                  -> Result<()> {
        debug!("Current branch: {}. Parents: {}",
               self.branch().unwrap_or("<detached head>"),
               self.parents_short_hashes().join(","));

        let abs_path = self.path().to_owned();

        let rev1 =
            rev1.and_then_try(|r| self.object_store.expect_ref_or_hash(&r))?
                .map(|r| r.into_hash());
        let rev2 =
            rev2.and_then_try(|r| self.object_store.expect_ref_or_hash(&r))?
                .map(|r| r.into_hash());

        match (rev1, rev2) {
            (None, None) => {
                let parent = self.parent();
                self.status_obj_file(show_ignored, parent, abs_path)
            }
            (Some(hash1), None) => {
                self.status_obj_file(show_ignored, Some(hash1), abs_path)
            }
            (Some(hash1), Some(hash2)) => {
                self.status_obj_obj(show_ignored, hash1, hash2)
            }
            (None, Some(_)) => unreachable!(),
        }
    }

    fn status_obj_file(&mut self,
                       show_ignored: bool,
                       src: Option<ObjectKey>,
                       targ: PathBuf)
                       -> Result<()> {

        let src: Option<ComparableNode> =
            src.and_then_try(|hash| self.object_store.lookup_node(hash))?;

        let targ: Option<ComparableNode> = Some(self.file_store
            .lookup_node(targ)?);

        let node = (src, targ);

        let combo = (&self.object_store, &self.file_store);
        let display = ComparePrintWalkDisplay::new(show_ignored, &combo, node);

        print!("{}", display);
        Ok(())
    }

    fn status_obj_obj(&mut self,
                      show_ignored: bool,
                      src: ObjectKey,
                      targ: ObjectKey)
                      -> Result<()> {

        let src: ComparableNode = self.object_store.lookup_node(src)?;
        let targ: ComparableNode = self.object_store.lookup_node(targ)?;
        let node = (Some(src), Some(targ));
        let combo = (&self.object_store, &self.object_store);

        let display = ComparePrintWalkDisplay::new(show_ignored, &combo, node);

        print!("{}", display);
        Ok(())
    }

    pub fn commit(&mut self,
                  message: String)
                  -> Result<(Option<&str>, ObjectKey)> {

        let abs_path = self.path().to_owned();
        let parent = self.parent();
        let est = self.transfer_est_hash(parent, &abs_path)?;
        let tree_hash = self.hash_obj_file_est(parent, &abs_path, est)?;

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

    pub fn checkout(&mut self, rev: &str) -> Result<()> {
        let abs_path = self.path().to_owned();
        let rev = self.object_store.expect_ref_or_hash(rev)?;
        self.fs_transfer.extract_object(rev.hash(), &abs_path)?;
        self.state.parents = vec![*rev.hash()];
        self.state.branch = rev.ref_name().map(|s| s.to_owned());
        self.state.flush()?;
        Ok(())
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

    pub fn log(&self, start: &RevSpec) -> Result<Commits> {
        Ok(Commits {
            object_store: &self,
            next: self.try_find_object(start)?,
            head: self.parent(),
        })
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
