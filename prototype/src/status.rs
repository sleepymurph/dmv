use std::collections;
use std::path;

use dag;

type ModifiedMap = collections::BTreeMap<path::PathBuf, PathStatus>;
type PathSet = collections::BTreeSet<path::PathBuf>;

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct DirStatus {
    known: dag::Tree,
    newmodified: ModifiedMap,
    to_hash_total_size: dag::ObjectSize,
    missing: PathSet,
}

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum PathStatus {
    /// Path (file or directory) matches commit
    Known { hash: dag::ObjectKey },
    /// Path (file or directory) exists in commit but is missing from file system
    Deleted,

    /// File does not exist in the previous commit
    NewFile { size: dag::ObjectSize },
    /// File exists in commit and is modified on disk
    ModifiedFile { size: dag::ObjectSize },
    /// File exists in commit and may be modified on disk, but test is expensive
    UncachedFile { size: dag::ObjectSize },

    /// Path is a directory that is modified
    ModifiedDir { status: DirStatus },
}

impl DirStatus {
    pub fn new() -> Self {
        DirStatus {
            known: dag::Tree::new(),
            newmodified: ModifiedMap::new(),
            to_hash_total_size: 0,
            missing: PathSet::new(),
        }
    }

    pub fn insert(&mut self, name: path::PathBuf, status: PathStatus) {
        match status {
            PathStatus::Known { hash } => {
                self.known.insert(name, hash);
            }
            PathStatus::Deleted => {
                self.missing.insert(name);
            }
            PathStatus::NewFile { size } |
            PathStatus::ModifiedFile { size } |
            PathStatus::UncachedFile { size } => {
                self.to_hash_total_size += size;
                self.newmodified.insert(name, status);
            }
            PathStatus::ModifiedDir { status } => {
                self.to_hash_total_size += status.to_hash_total_size();
                self.newmodified
                    .insert(name, PathStatus::ModifiedDir { status: status });
            }
        };
    }

    pub fn to_hash_total_size(&self) -> dag::ObjectSize {
        self.to_hash_total_size
    }

    pub fn is_modified(&self) -> bool {
        self.newmodified.len() != 0 || self.missing.len() != 0
    }
}
