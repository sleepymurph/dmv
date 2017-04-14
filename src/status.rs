//! File status objects

use dag::ObjectKey;
use dag::ObjectSize;
use std::path::PathBuf;

/// Status of an individual file or dir, as compared to a commit
#[derive(Clone,Copy,Eq,PartialEq,Debug)]
pub enum Status {
    Ignored,
    Add,
    // Offline,
    Delete,
    Unchanged,
    Modified,
    MaybeModified,
}

impl Status {
    /// A short status code for display
    pub fn code(&self) -> &'static str {
        match self {
            &Status::Ignored => "i",
            &Status::Add => "a",
            // &Status::Offline => "o",
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
            // &Status::Offline |
            &Status::Unchanged |
            &Status::Modified |
            &Status::MaybeModified => true,

            &Status::Ignored |
            &Status::Delete => false,
        }
    }

    pub fn needs_transfer(&self) -> bool {
        match self {
            &Status::Add |
            // &Status::Offline |
            &Status::Modified |
            &Status::MaybeModified => true,

            &Status::Unchanged |
            &Status::Ignored |
            &Status::Delete => false,
        }
    }
}


#[derive(Debug,Clone)]
pub struct ComparableNode {
    pub is_treeish: bool,
    pub file_size: ObjectSize,
    pub hash: Option<ObjectKey>,
    pub fs_path: Option<PathBuf>,
    pub is_ignored: bool,
}

impl ComparableNode {
    pub fn compare_pair(pair: &(Option<ComparableNode>,
                                Option<ComparableNode>))
                        -> Status {
        Self::compare(pair.0.as_ref(), pair.1.as_ref())
    }
    pub fn compare_into<A, B>(src: Option<A>, targ: Option<B>) -> Status
        where A: Into<ComparableNode>,
              B: Into<ComparableNode>
    {
        Self::compare(src.map(|n| n.into()).as_ref(),
                      targ.map(|n| n.into()).as_ref())
    }
    pub fn compare(src: Option<&ComparableNode>,
                   targ: Option<&ComparableNode>)
                   -> Status {

        let src = src.as_ref();
        let targ = targ.as_ref();

        let src_exists = src.is_some();
        let targ_exists = targ.is_some();
        let src_hash = src.and_then(|n| n.hash);
        let targ_hash = targ.and_then(|n| n.hash);
        let src_is_ignored = src.map(|n| n.is_ignored).unwrap_or(false);
        let targ_is_ignored = targ.map(|n| n.is_ignored).unwrap_or(false);

        match (src_exists, targ_exists, src_hash, targ_hash) {

            (true, true, Some(a), Some(b)) if a == b => Status::Unchanged,
            (true, true, _, _) if src_is_ignored && targ_is_ignored => {
                Status::Ignored
            }
            (true, true, Some(_), Some(_)) => Status::Modified,
            (true, true, _, _) => Status::MaybeModified,

            (false, true, _, _) if targ_is_ignored => Status::Ignored,
            (false, true, _, _) => Status::Add,

            (true, false, _, _) if src_is_ignored => Status::Ignored,
            (true, false, _, _) => Status::Delete,

            (false, false, _, _) => Status::Unchanged,
        }
    }
}
