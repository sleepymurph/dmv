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
use object_store::ObjectStore;
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
        let tree_hash = self.hash_obj_file(parent, &abs_path)?;

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

    pub fn log(&self) -> Result<()> {
        use object_store::DepthFirstCommitSort;
        use std::cmp;

        debug!("First pass: sort commits");
        let mut start_refs: Vec<ObjectKey> = self.parents().clone();
        start_refs.extend(self.object_store.refs().iter().map(|(_, v)| v));
        start_refs.dedup();

        let mut sorted = DepthFirstCommitSort::new(&self.object_store,
                                                   start_refs).run()?;

        debug!("Second pass: print");
        let mut slots = Vec::new();
        while let Some((hash, commit)) = sorted.pop() {
            if !slots.contains(&hash) {
                slots.push(hash);
            }
            let mut slot = 0;
            while slots[slot] != hash {
                slot += 1;
                assert!(slot < slots.len(), "current hash not found in slots");
            }

            LogGlyph::print_ascii(&LogGlyph::commit_pat(slots.len(), slot));

            let mut refs = self.object_store.refs_for(&hash);
            let parent_ref_name = self.parents()
                .iter()
                .enumerate()
                .filter(|&(_, head_hash)| head_hash == &hash)
                .map(|(i, _)| i)
                .take(1)
                .next()
                .map(|p| match self.parents().len() {
                    1 => "HEAD".to_owned(),
                    _ => format!("PARENT{}", p),
                });
            if let Some(s) = parent_ref_name {
                refs.insert(0, s);
            }
            match refs.len() {
                0 => println!("{} {}", hash, commit.message),
                _ => {
                    println!("{} ({}) {}",
                             hash,
                             refs.join(", "),
                             commit.message)
                }
            }


            let mut transition_glyphs = None;
            match commit.parents.len() {
                0 => {
                    // Dead end
                    transition_glyphs =
                        Some(LogGlyph::contract_pat(slots.len(), slot, None));
                    slots.remove(slot);
                }
                1 => {
                    slots[slot] = commit.parents[0];
                    // Potential join
                    let mut dup = 0;
                    while dup < slots.len() {
                        if slots[dup] == slots[slot] && dup != slot {
                            break;
                        }
                        dup += 1;
                    }
                    if dup < slots.len() {
                        let (slot, dup) = (cmp::min(slot, dup),
                                           cmp::max(slot, dup));
                        transition_glyphs =
                            Some(LogGlyph::contract_pat(slots.len(),
                                                        slot,
                                                        Some(dup)));
                        slots.remove(dup);
                    }
                }
                _ => unimplemented!(),
            }
            if let Some(glyphs) = transition_glyphs {
                if glyphs.len() > 0 {
                    LogGlyph::print_ascii(&glyphs);
                    println!();
                }
            }
        }

        Ok(())
    }
}

#[cfg_attr(rustfmt, rustfmt_skip)]
const LOG_GLYPHS:&'static [(LogGlyph,&'static str)] = &[
    (LogGlyph::Commit,      "* "),
    (LogGlyph::Straight,    "| "),
    (LogGlyph::Join,        "|/"),
    (LogGlyph::ShiftLeft,   " /"),
    (LogGlyph::StartSpan,   "+-"),
    (LogGlyph::Span,        "--"),
    (LogGlyph::EndSpanUp,   "-´"),
    (LogGlyph::EndSpanDown, "-,"),
];

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
enum LogGlyph {
    Commit,
    Straight,
    Join,
    ShiftLeft,
    StartSpan,
    Span,
    EndSpanUp,
    EndSpanDown,
}
impl LogGlyph {
    fn glyph(&self) -> &'static (LogGlyph, &'static str) {
        let glyph = &LOG_GLYPHS[*self as usize];
        assert_eq!(glyph.0, *self, "Mismatch in glyph constants");
        glyph
    }
    pub fn ascii(&self) -> &'static str { self.glyph().1 }
    pub fn print_ascii(glyphs: &[LogGlyph]) {
        for glyph in glyphs {
            print!("{}", glyph.ascii());
        }
    }
    pub fn commit_pat(slots: usize, commit_slot: usize) -> Vec<LogGlyph> {
        let mut glyphs = Vec::with_capacity(slots);
        for i in 0..slots {
            match i {
                _ if i == commit_slot => glyphs.push(LogGlyph::Commit),
                _ => glyphs.push(LogGlyph::Straight),
            }
        }
        glyphs
    }
    pub fn contract_pat(slots: usize,
                        commit_slot: usize,
                        dup_slot: Option<usize>)
                        -> Vec<LogGlyph> {
        let mut glyphs = Vec::with_capacity(slots);
        let c = commit_slot;
        for i in 0..slots - 1 {
            glyphs.push(match dup_slot {
                _ if i < c => LogGlyph::Straight,
                None if i == c => LogGlyph::Join,
                None if i > c => LogGlyph::ShiftLeft,
                Some(d) if i == c && d == c + 1 => LogGlyph::Join,
                Some(d) if i == c && d > c + 1 => LogGlyph::StartSpan,
                Some(d) if c + 1 < i && i < d - 1 => LogGlyph::Span,
                Some(d) if i == d - 1 && d != c + 1 => LogGlyph::EndSpanUp,
                Some(d) if i > d => LogGlyph::ShiftLeft,
                _ => unreachable!(),
            });
        }
        glyphs
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
