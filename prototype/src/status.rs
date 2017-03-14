//! File status objects

use dag::ObjectKey;
use dag::ObjectSize;
use std::fmt;
use std::path::PathBuf;
use walker::*;

/// Status of an individual file or dir, as compared to a commit
#[derive(Clone,Copy,Eq,PartialEq,Debug)]
pub enum Status {
    Untracked,
    Ignored,
    Add,
    Offline,
    Delete,
    Unchanged,
    Modified,
    MaybeModified,
}

impl Status {
    /// A short status code for display
    pub fn code(&self) -> &'static str {
        match self {
            &Status::Untracked => "?",
            &Status::Ignored => "i",
            &Status::Add => "a",
            &Status::Offline => "o",
            &Status::Delete => "d",
            &Status::Unchanged => " ",
            &Status::Modified => "M",
            &Status::MaybeModified => "m",
        }
    }

    /// Would a file with this status be included in a commit?
    pub fn is_included(&self) -> bool {
        match self {
            &Status::Add |
            &Status::Offline |
            &Status::Unchanged |
            &Status::Modified |
            &Status::MaybeModified => true,

            &Status::Untracked |
            &Status::Ignored |
            &Status::Delete => false,
        }
    }
}


/// A hierarchy of paths and their statuses, describing a potential commit
pub struct HashPlan {
    pub path: PathBuf,
    pub is_dir: bool,
    pub status: Status,
    pub hash: Option<ObjectKey>,
    pub size: ObjectSize,
    pub children: ChildMap<HashPlan>,
}

impl HashPlan {
    /// Total size of all unhashed files in this hierarchy
    pub fn unhashed_size(&self) -> ObjectSize {
        match self {
            &HashPlan { status, .. } if !status.is_included() => 0,
            &HashPlan { is_dir: false, hash: None, size, .. } => size,
            _ => {
                self.children
                    .iter()
                    .map(|(_, plan)| plan.unhashed_size())
                    .sum()
            }
        }
    }
}

impl NodeWithChildren for HashPlan {
    fn children(&self) -> Option<&ChildMap<Self>> { Some(&self.children) }
}

impl<'a> fmt::Display for HashPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.is_dir && self.status.is_included() {
            for (_, child) in &self.children {
                child.fmt(f)?;
            }
        } else {
            if self.status != Status::Unchanged {
                writeln!(f, "{} {}", self.status.code(), self.path.display())?
            }
        }
        Ok(())
    }
}
