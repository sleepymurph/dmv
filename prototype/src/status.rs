use std::collections;
use std::path;

use dag;

type ModifiedMap = collections::BTreeMap<path::PathBuf, ModifiedChild>;
type PathSet = collections::BTreeSet<path::PathBuf>;

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct DirStatus {
    known: dag::Tree,
    newmodified: ModifiedMap,
    to_hash_total_size: dag::ObjectSize,
    missing: PathSet,
}

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum ModifiedChild {
    File {
        newmodified: NewModified,
        size: dag::ObjectSize,
    },
    Dir {
        newmodified: NewModified,
        status: DirStatus,
    },
}

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum NewModified {
    /// File or directory does not exist in the previous commit
    New,
    /// File or directory exists in the commit and is modified
    Modified,
    /// File exists and may be modified, we must hash it to tell (files only)
    NoCache,
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

    pub fn insert_known(&mut self, name: path::PathBuf, hash: dag::ObjectKey) {
        self.known.insert(name, hash);
    }

    pub fn insert_modified(&mut self,
                           name: path::PathBuf,
                           modified: ModifiedChild) {
        self.to_hash_total_size += match modified {
            ModifiedChild::File { size, .. } => size,
            ModifiedChild::Dir { ref status, .. } => {
                status.to_hash_total_size()
            }
        };
        self.newmodified.insert(name, modified);
    }

    pub fn to_hash_total_size(&self) -> dag::ObjectSize {
        self.to_hash_total_size
    }
}
