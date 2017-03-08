use cache::CacheStatus;
use human_readable;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io;
use super::*;

type PathKeyMap = BTreeMap<OsString, ObjectKey>;

wrapper_struct!{
    /// DAG Object representing a directory
    #[derive(Clone,Eq,PartialEq,Hash,Debug)]
    pub struct Tree(PathKeyMap);
}

impl Tree {
    pub fn new() -> Self { Tree(PathKeyMap::new()) }

    pub fn insert<P>(&mut self, name: P, hash: ObjectKey)
        where P: Into<OsString>
    {
        self.0.insert(name.into(), hash);
    }
}

/// Create and populate a Tree object
#[macro_export]
macro_rules! tree_object {
    ( $( $k:expr => $v:expr , )* ) => {
        map!{ $crate::dag::Tree::new(), $( $k=>$v, )* };
    }
}

const TREE_ENTRY_SEPARATOR: u8 = b'\n';

impl ObjectCommon for Tree {
    fn object_type(&self) -> ObjectType { ObjectType::Tree }
    fn content_size(&self) -> ObjectSize {
        self.0.iter().fold(0, |acc, x| {
            acc + KEY_SIZE_BYTES + x.0.as_os_str().len() + 1
        }) as ObjectSize
    }

    fn write_content(&self, writer: &mut io::Write) -> io::Result<()> {
        for entry in &self.0 {
            try!(writer.write(entry.1.as_ref()));
            try!(writer.write(entry.0.to_str().unwrap().as_bytes()));
            try!(writer.write(&[TREE_ENTRY_SEPARATOR]));
        }
        Ok(())
    }

    fn pretty_print(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();
        write!(&mut output,
               "Tree Index

Object content size:    {:>10}

",
               human_readable::human_bytes(self.content_size()))
            .unwrap();

        for entry in &self.0 {
            write!(&mut output,
                   "{:x} {}\n",
                   entry.1,
                   entry.0.to_str().unwrap())
                .unwrap();
        }
        output
    }
}

impl ReadObjectContent for Tree {
    fn read_content<R: io::BufRead>(reader: &mut R) -> Result<Self> {

        let mut tree = Tree::new();

        loop {
            // Read hash
            let mut hash_buf = [0u8; KEY_SIZE_BYTES];
            let bytes_read = try!(reader.read(&mut hash_buf));
            if bytes_read == 0 {
                break;
            }
            let hash = ObjectKey::from(hash_buf);

            // Read name
            let mut name_buf: Vec<u8> = Vec::new();
            try!(reader.read_until(TREE_ENTRY_SEPARATOR, &mut name_buf));
            name_buf.pop(); // Drop the string-ending separator
            let name = try!(String::from_utf8(name_buf));
            tree.insert(name, hash);
        }
        Ok(tree)
    }
}


#[derive(Clone,Copy,Eq,PartialEq,Hash,Debug)]
pub enum ItemClass {
    BlobLike,
    TreeLike,
    Unknown,
}

use self::ItemClass::*;

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct PartialItem {
    pub class: ItemClass,
    pub size: ObjectSize,
    pub hash: Option<ObjectKey>,
    pub children: Option<PartialTree>,
    pub mark_ignore: bool,
}

impl PartialItem {
    pub fn unhashed_file(size: ObjectSize) -> Self {
        PartialItem {
            class: BlobLike,
            size: size,
            hash: None,
            children: None,
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
            &PartialItem { hash: None,
                           class: TreeLike,
                           children: Some(ref partial),
                           .. } => HashedOrNot::Dir(partial),
            &PartialItem { hash: None,
                           class: TreeLike,
                           children: None,
                           .. } => unimplemented!(),
            &PartialItem { hash: None, size, .. } => {
                HashedOrNot::UnhashedFile(size)
            }
        }
    }
    pub fn unhashed_size(&self) -> ObjectSize {
        match self.hon() {
            HashedOrNot::Hashed(_) => 0,
            HashedOrNot::UnhashedFile(size) => size,
            HashedOrNot::Dir(ref partial) => partial.unhashed_size(),
        }
    }
    pub fn is_vacant(&self) -> bool {
        match self {
            &PartialItem { hash: Some(_), .. } => false,
            &PartialItem { mark_ignore: true, .. } => true,
            &PartialItem { children: Some(ref children), .. } => {
                children.is_vacant()
            }
            _ => false,
        }
    }
    pub fn prune_vacant(&self) -> PartialItem {
        PartialItem {
            class: self.class,
            size: self.size,
            hash: self.hash.to_owned(),
            children: match self.children {
                Some(ref children) => Some(children.prune_vacant()),
                None => None,
            },
            mark_ignore: self.mark_ignore,
        }
    }
}

impl From<CacheStatus> for PartialItem {
    fn from(s: CacheStatus) -> Self {
        match s {
            CacheStatus::Cached { hash } => PartialItem::from(hash),
            CacheStatus::Modified { size } |
            CacheStatus::NotCached { size } => PartialItem::unhashed_file(size),
        }
    }
}

impl From<PartialTree> for PartialItem {
    fn from(pt: PartialTree) -> Self {
        PartialItem {
            class: TreeLike,
            size: 0,
            hash: None,
            children: Some(pt),
            mark_ignore: false,
        }
    }
}

impl From<ObjectKey> for PartialItem {
    fn from(hash: ObjectKey) -> Self {
        PartialItem {
            class: Unknown,
            size: 0,
            hash: Some(hash),
            children: None,
            mark_ignore: false,
        }
    }
}

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum HashedOrNot<'a> {
    Hashed(&'a ObjectKey),
    UnhashedFile(ObjectSize),
    Dir(&'a PartialTree),
}

type PartialMap = BTreeMap<OsString, PartialItem>;

/// An incomplete Tree object that requires some files to be hashed
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
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

impl From<Tree> for PartialTree {
    fn from(t: Tree) -> Self {
        let mut partial = PartialTree::new();
        for (name, hash) in t.0 {
            partial.insert(name, hash);
        }
        partial
    }
}

/// Create and populate a PartialTree object
#[macro_export]
macro_rules! partial_tree {
    ( $( $k:expr => $v:expr , )*) => {
        map!{ $crate::dag::PartialTree::new(), $( $k => $v, )* };
    }
}

impl IntoIterator for PartialTree {
    type Item = (OsString, PartialItem);
    type IntoIter = <BTreeMap<OsString, PartialItem> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}


#[cfg(test)]
mod test {

    use std::ffi::OsString;
    use std::io;
    use super::super::*;
    use testutil;
    use testutil::rand::Rng;

    #[test]
    fn test_write_tree() {
        // Construct object
        let mut rng = testutil::TestRand::default();

        let object = tree_object!{
            "foo" => rng.gen::<ObjectKey>(),
            "bar" => rng.gen::<ObjectKey>(),
            "baz" => rng.gen::<ObjectKey>(),
        };

        // Write out
        let mut output: Vec<u8> = Vec::new();
        object.write_to(&mut output).expect("write out object");

        // Read in header
        let mut reader = io::BufReader::new(output.as_slice());
        let header = ObjectHeader::read_from(&mut reader).expect("read header");

        assert_eq!(header.object_type, ObjectType::Tree);
        assert_ne!(header.content_size, 0);

        // Read in object content
        let readobject = Tree::read_content(&mut reader)
            .expect("read object content");

        assert_eq!(readobject, object);
    }

    #[test]
    fn test_tree_sort_by_name() {
        let tree = tree_object!{
            "foo" => object_key(0),
            "bar" => object_key(2),
            "baz" => object_key(1),
        };

        let names: Vec<String> = tree.iter()
            .map(|ent| ent.0.to_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["bar", "baz", "foo"]);
    }

    #[test]
    fn test_partial_tree() {

        // Create partial tree

        let mut partial = partial_tree!{
                "foo" => object_key(0),
                "bar" => object_key(2),
                "baz" => object_key(1),
                "fizz" => PartialItem::unhashed_file(1024),
                "buzz" => partial_tree!{
                    "strange" => PartialItem::unhashed_file(2048),
                },
        };

        assert_eq!(partial.get(&OsString::from("fizz")),
                   Some(&PartialItem::unhashed_file(1024)));

        assert_eq!(partial.unhashed_size(), 3072);

        // Begin adding hashes for incomplete objects

        partial.insert("buzz", object_key(3));
        assert_eq!(partial.get(&OsString::from("buzz")),
                   Some(&PartialItem::from(object_key(3))));
        assert_eq!(partial.unhashed_size(), 1024);

        partial.insert("fizz", object_key(4));

        // Should be complete now

        assert_eq!(partial.unhashed_size(), 0);
    }

    #[test]
    fn test_partial_tree_prune() {
        let partial = partial_tree!{
                "foo" => object_key(0),
                "fizz" => PartialItem::unhashed_file(1024),
                ".prototype_cache" => PartialItem::ignored_file(123),
                "buzz" => partial_tree!{
                    "strange" => PartialItem::unhashed_file(2048),
                    ".prototype_cache" => PartialItem::ignored_file(123),
                },
                "empty" => PartialTree::new(),
        };

        let expected = partial_tree!{
                "foo" => object_key(0),
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
                "foo" => object_key(0),
                "bar" => partial_tree!{
                    "baz" => object_key(1),
                },
        };

        assert_eq!(partial.unhashed_size(), 0, "no files need to be hashed");

        assert_eq!(partial.get(&OsString::from("bar")),
                   Some(&PartialItem::from(partial_tree!{
                        "baz" => object_key(1),
                       })),
                   "the nested PartialTree still holds information that \
                    would be lost if we replaced it with just a hash");
    }
}
