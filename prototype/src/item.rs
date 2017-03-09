//! Items that represent either objects in the store or files on disk

use dag::*;
use error::*;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::fs::Metadata;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone,PartialEq,Hash,Debug)]
pub enum ItemHandle {
    Path(PathBuf),
    Object(ObjectKey),
}

impl From<PathBuf> for ItemHandle {
    fn from(o: PathBuf) -> Self { ItemHandle::Path(o) }
}
impl From<ObjectKey> for ItemHandle {
    fn from(o: ObjectKey) -> Self { ItemHandle::Object(o) }
}
impl<'a, T: ?Sized, O> From<&'a T> for ItemHandle
    where T: ToOwned<Owned = O> + 'a,
          O: Borrow<T>,
          O: Into<ItemHandle>
{
    fn from(o: &'a T) -> Self { o.to_owned().into() }
}

impl fmt::Display for ItemHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &ItemHandle::Path(ref path) => write!(f, "{}", path.display()),
            &ItemHandle::Object(ref hash) => write!(f, "{}", hash),
        }
    }
}


#[derive(Clone,PartialEq,Hash,Debug)]
pub enum LoadItems {
    NotLoaded(ItemHandle),
    Loaded(PartialTree),
}
use self::LoadItems::*;

#[derive(Clone,PartialEq,Hash,Debug)]
pub enum ItemClass {
    BlobLike(ObjectSize),
    TreeLike(LoadItems),
}
use self::ItemClass::*;

impl ItemClass {
    pub fn for_path(path: &Path, meta: &Metadata) -> Result<Self> {
        if meta.is_file() {
            Ok(ItemClass::BlobLike(meta.len()))
        } else if meta.is_dir() {
            Ok(ItemClass::TreeLike(
                    NotLoaded(ItemHandle::Path(path.to_owned()))))
        } else {
            bail!("Unknown file type for {}:", path.display())
        }
    }
}

#[derive(Clone,PartialEq,Hash,Debug)]
pub struct PartialItem {
    pub class: ItemClass,
    pub hash: Option<ObjectKey>,
    pub mark_ignore: bool,
}

impl PartialItem {
    pub fn unhashed_file(size: ObjectSize) -> Self {
        PartialItem {
            class: BlobLike(size),
            hash: None,
            mark_ignore: false,
        }
    }
    pub fn hashed_file(size: ObjectSize, hash: ObjectKey) -> Self {
        PartialItem {
            class: BlobLike(size),
            hash: Some(hash),
            mark_ignore: false,
        }
    }
    pub fn ignored_file(size: ObjectSize) -> Self {
        let mut partial = PartialItem::unhashed_file(size);
        partial.mark_ignore = true;
        partial
    }
    pub fn hon(&self) -> HashedOrNot {
        match self {
            &PartialItem { hash: Some(ref hash), .. } => {
                HashedOrNot::Hashed(hash)
            }
            &PartialItem { hash: None, class: BlobLike(size), .. } => {
                HashedOrNot::UnhashedFile(size)
            }
            &PartialItem { hash: None,
                           class: TreeLike(Loaded(ref partial)),
                           .. } => HashedOrNot::Dir(partial),
            _ => panic!("Cannot convert to HashedOrNot: {:?}", self),
        }
    }
    pub fn unhashed_size(&self) -> ObjectSize {
        match self {
            &PartialItem { hash: Some(_), .. } => 0,
            &PartialItem { class: BlobLike(size), .. } => size,
            &PartialItem { class: TreeLike(Loaded(ref children)), .. } => {
                children.unhashed_size()
            }
            &PartialItem { class: TreeLike(NotLoaded(_)),
                           mark_ignore: true,
                           .. } => 0,
            _ => panic!("Cannot calculate unhashed_size: {:?}", self),
        }
    }
    pub fn is_vacant(&self) -> bool {
        match self {
            &PartialItem { hash: Some(_), .. } => false,
            &PartialItem { mark_ignore: true, .. } => true,
            &PartialItem { class: TreeLike(Loaded(ref children)), .. } => {
                children.is_vacant()
            }
            _ => false,
        }
    }
    pub fn prune_vacant(&self) -> PartialItem {
        PartialItem {
            class: match &self.class {
                &BlobLike(size) => BlobLike(size),
                &TreeLike(Loaded(ref children)) => {
                    TreeLike(Loaded(children.prune_vacant()))
                }
                other => other.to_owned(),
            },
            hash: self.hash.to_owned(),
            mark_ignore: self.mark_ignore,
        }
    }
}

impl From<PartialTree> for PartialItem {
    fn from(pt: PartialTree) -> Self {
        PartialItem {
            class: TreeLike(Loaded(pt)),
            hash: None,
            mark_ignore: false,
        }
    }
}

impl From<Blob> for PartialItem {
    fn from(b: Blob) -> Self {
        PartialItem {
            class: BlobLike(b.content_size()),
            hash: Some(b.calculate_hash()),
            mark_ignore: false,
        }
    }
}

#[derive(Clone,PartialEq,Hash,Debug)]
pub enum HashedOrNot<'a> {
    Hashed(&'a ObjectKey),
    UnhashedFile(ObjectSize),
    Dir(&'a PartialTree),
}

type PartialMap = BTreeMap<OsString, PartialItem>;

/// An incomplete Tree object that requires some files to be hashed
#[derive(Clone,PartialEq,Hash,Debug)]
pub struct PartialTree(PartialMap);

impl_deref!(PartialTree => PartialMap);

impl PartialTree {
    pub fn new() -> Self { PartialTree(PartialMap::new()) }

    /// Calculate the total size of all unhashed children
    ///
    /// How many bytes must be hashed to complete this Tree?
    pub fn unhashed_size(&self) -> ObjectSize {
        self.unhashed().map(|(_, unhashed)| unhashed.unhashed_size()).sum()
    }

    /// Insert a new child path
    ///
    /// Accepts any type that can be converted into a PartialItem.
    pub fn insert<P, T>(&mut self, path: P, st: T)
        where P: Into<OsString>,
              T: Into<PartialItem>
    {
        self.0.insert(path.into(), st.into());
    }

    /// Get an iterator of unhashed children
    pub fn unhashed<'a>
        (&'a self)
         -> Box<Iterator<Item = (&'a OsString, &'a PartialItem)> + 'a> {
        Box::new(self.0.iter().filter(|&(_, entry)| entry.hash.is_none()))
    }

    /// Returns true if there are no files worth storing in the PartialTree
    pub fn is_vacant(&self) -> bool {
        for entry in self.0.values() {
            if !entry.is_vacant() {
                return false;
            }
        }
        true
    }

    pub fn prune_vacant(&self) -> PartialTree {
        PartialTree(self.0
            .iter()
            .filter(|&(_, ref entry)| !entry.is_vacant())
            .map(|(name, entry)| (name.to_owned(), entry.prune_vacant()))
            .collect())
    }
}

/// Create and populate a PartialTree object
#[macro_export]
macro_rules! partial_tree {
    ( $( $k:expr => $v:expr , )*) => {
        map!{ $crate::item::PartialTree::new(), $( $k => $v, )* };
    }
}

impl IntoIterator for PartialTree {
    type Item = (OsString, PartialItem);
    type IntoIter = <BTreeMap<OsString, PartialItem> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}


#[cfg(test)]
mod test {

    use dag::*;
    use std::ffi::OsString;
    use super::*;

    #[test]
    fn test_partial_tree() {

        // Create partial tree

        let mut partial = partial_tree!{
                "foo" => PartialItem::hashed_file(256, object_key(0)),
                "bar" => PartialItem::hashed_file(512, object_key(2)),
                "baz" => PartialItem::hashed_file(1024,object_key(1)),
                "fizz" => PartialItem::unhashed_file(1024),
                "buzz" => partial_tree!{
                    "strange" => PartialItem::unhashed_file(2048),
                },
        };

        assert_eq!(partial.get(&OsString::from("fizz")),
                   Some(&PartialItem::unhashed_file(1024)));

        assert_eq!(partial.unhashed_size(), 3072);

        // Begin adding hashes for incomplete objects

        partial.insert("buzz", PartialItem::hashed_file(2048, object_key(3)));
        assert_eq!(partial.unhashed_size(), 1024);

        partial.insert("fizz", PartialItem::hashed_file(1024, object_key(4)));

        // Should be complete now

        assert_eq!(partial.unhashed_size(), 0);
    }

    #[test]
    fn test_partial_tree_prune() {
        let partial = partial_tree!{
                "foo" => PartialItem::hashed_file(256,object_key(0)),
                "fizz" => PartialItem::unhashed_file(1024),
                ".prototype_cache" => PartialItem::ignored_file(123),
                "buzz" => partial_tree!{
                    "strange" => PartialItem::unhashed_file(2048),
                    ".prototype_cache" => PartialItem::ignored_file(123),
                },
                "empty" => PartialTree::new(),
        };

        let expected = partial_tree!{
                "foo" => PartialItem::hashed_file(256,object_key(0)),
                "fizz" => PartialItem::unhashed_file(1024),
                "buzz" => partial_tree!{
                    "strange" => PartialItem::unhashed_file(2048),
                },
        };

        assert_eq!(partial.prune_vacant(), expected);
    }

    #[test]
    fn test_partial_tree_with_zero_unhashed() {
        let partial = partial_tree!{
                "foo" => PartialItem::hashed_file(256,object_key(0)),
                "bar" => partial_tree!{
                    "baz" => PartialItem::hashed_file(512,object_key(1)),
                },
        };

        assert_eq!(partial.unhashed_size(), 0, "no files need to be hashed");

        assert_eq!(partial.get(&OsString::from("bar")),
                   Some(&PartialItem::from(partial_tree!{
                    "baz" => PartialItem::hashed_file(512,object_key(1)),
                       })),
                   "the nested PartialTree still holds information that \
                    would be lost if we replaced it with just a hash");
    }
}
