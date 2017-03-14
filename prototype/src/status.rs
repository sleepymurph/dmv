//! File status objects

use dag::ObjectKey;
use dag::ObjectSize;
use error::*;
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

    pub fn display(&self) -> HashPlanDisplay { HashPlanDisplay::new(self) }
}

impl NodeWithChildren for HashPlan {
    fn children(&self) -> Option<&ChildMap<Self>> { Some(&self.children) }
}


/// A wrapper to Display a HashPlan, with options
pub struct HashPlanDisplay<'a> {
    hash_plan: &'a HashPlan,
    show_ignored: bool,
}
impl<'a> HashPlanDisplay<'a> {
    fn new(hp: &'a HashPlan) -> Self {
        HashPlanDisplay {
            hash_plan: hp,
            show_ignored: false,
        }
    }
    pub fn show_ignored(mut self, si: bool) -> Self {
        self.show_ignored = si;
        self
    }
}
impl<'a> fmt::Display for HashPlanDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut op = HashPlanDisplayOp {
            show_ignored: self.show_ignored,
            formatter: f,
        };
        match self.hash_plan.walk(&mut op) {
            Ok(_) => Ok(()),
            Err(_) => Err(fmt::Error),
        }
    }
}


/// An operation that walks a HashPlan to Display it
struct HashPlanDisplayOp<'s, 'f: 's> {
    show_ignored: bool,
    formatter: &'s mut fmt::Formatter<'f>,
}
impl<'a, 'b> WalkOp<&'a HashPlan> for HashPlanDisplayOp<'a, 'b> {
    type VisitResult = ();

    fn should_descend(&mut self, _ps: &PathStack, node: &&HashPlan) -> bool {
        node.is_dir && node.status.is_included()
    }

    fn no_descend(&mut self,
                  _ps: &PathStack,
                  node: &HashPlan)
                  -> Result<Option<Self::VisitResult>> {
        let show = node.status != Status::Unchanged &&
                   (node.status != Status::Ignored || self.show_ignored);
        if show {
            writeln!(self.formatter,
                     "{} {}",
                     node.status.code(),
                     node.path.display())?;
        }
        Ok(None)
    }
}
