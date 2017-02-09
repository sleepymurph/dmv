use cache::CacheStatus;
use humanreadable;
use std::collections;
use std::io;
use std::path::PathBuf;
use super::*;

type PathKeyMap = collections::BTreeMap<PathBuf, ObjectKey>;
type PathKeyMapIter<'a> = collections::btree_map::Iter<'a, PathBuf, ObjectKey>;

/// DAG Object representing a directory
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct Tree {
    entries: PathKeyMap,
}

impl Tree {
    pub fn new() -> Self { Tree { entries: PathKeyMap::new() } }

    pub fn insert<P>(&mut self, name: P, hash: ObjectKey)
        where P: Into<PathBuf>
    {
        self.entries.insert(name.into(), hash);
    }

    pub fn iter(&self) -> PathKeyMapIter { self.entries.iter() }

    pub fn len(&self) -> usize { self.entries.len() }
}

#[macro_export]
macro_rules! tree_object {
    ( $( $path:expr => $hash:expr , )* ) => {
        {
            let mut tree = $crate::dag::Tree::new();
            $( tree.insert($path, $hash); )*
            tree
        }
    }
}

const TREE_ENTRY_SEPARATOR: u8 = b'\n';

impl ObjectCommon for Tree {
    fn object_type(&self) -> ObjectType { ObjectType::Tree }
    fn content_size(&self) -> ObjectSize {
        self.entries.iter().fold(0, |acc, x| {
            acc + KEY_SIZE_BYTES + x.0.as_os_str().len() + 1
        }) as ObjectSize
    }

    fn write_content(&self, writer: &mut io::Write) -> io::Result<()> {
        for entry in &self.entries {
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
               humanreadable::human_bytes(self.content_size()))
            .unwrap();

        for entry in &self.entries {
            write!(&mut output, "{} {}\n", entry.1, entry.0.to_str().unwrap())
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


type UnhashedMap = collections::BTreeMap<PathBuf, UnhashedPath>;

/// An incomplete Tree object that requires some files to be hashed
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct PartialTree {
    tree: Tree,
    unhashed: UnhashedMap,
}

/// For PartialTree: A child path that needs hashing
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum UnhashedPath {
    /// The child path is a file, carry its size
    File(ObjectSize),
    /// The child path is a directory, carry its PartialTree
    Dir(PartialTree),
}

/// For PartialTree: A child path that may or may not need hashing
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum HashedOrNot {
    /// The child path is a file with a known hash, carry the hash
    Hashed(ObjectKey),
    /// The child path is a file with unknown hash, carry the size
    UnhashedFile(ObjectSize),
    /// The child path is a directory
    Dir(PartialTree),
}

impl PartialTree {
    pub fn new() -> Self { PartialTree::from(Tree::new()) }

    /// Calculate the total size of all unhashed children
    ///
    /// How many bytes must be hashed to complete this Tree?
    pub fn unhashed_size(&self) -> ObjectSize {
        self.unhashed.values().map(|unhashed| unhashed.unhashed_size()).sum()
    }

    /// Insert a new child path
    ///
    /// Accepts any type that can be converted into a HashedOrNot.
    pub fn insert<P, T>(&mut self, path: P, st: T)
        where P: Into<PathBuf>,
              T: Into<HashedOrNot>
    {

        let path = path.into();
        let st = st.into();
        match st {
            HashedOrNot::Hashed(hash) => self.insert_hash(path, hash),
            HashedOrNot::UnhashedFile(size) => {
                self.insert_unhashed(path, UnhashedPath::File(size))
            }
            HashedOrNot::Dir(partial) => {
                self.insert_unhashed(path, UnhashedPath::Dir(partial))
            }
        }
    }

    fn insert_hash(&mut self, path: PathBuf, hash: ObjectKey) {
        self.unhashed.remove(&path);
        self.tree.insert(path, hash);
    }

    fn insert_unhashed<T>(&mut self, path: PathBuf, unknown: T)
        where UnhashedPath: From<T>
    {
        let unknown = UnhashedPath::from(unknown);
        self.unhashed.insert(path, unknown);
    }

    /// Get a map of unhashed children: path => UnhashedPath
    pub fn unhashed(&self) -> &UnhashedMap { &self.unhashed }

    /// Get a Tree from the known hashed children
    pub fn tree(&self) -> &Tree { &self.tree }

    /// Do all children have known hashes?
    ///
    /// Note that a PartialTree can be "incomplete," even if it has no files
    /// that need to be hashed. This can happen if one of the children is a
    /// PartialTree that is "complete." We may be able to calculate the hash of
    /// that subtree, but storing it as just a hash would loose the information
    /// we have about its children. So we should not do that until we can be
    /// sure that the tree has been stored in an object store.
    pub fn is_complete(&self) -> bool { self.unhashed.len() == 0 }
}

impl From<Tree> for PartialTree {
    fn from(t: Tree) -> Self {
        PartialTree {
            tree: t,
            unhashed: UnhashedMap::new(),
        }
    }
}

#[macro_export]
macro_rules! partial_tree {
    (
        $( $name:expr => $hashed_or_not:expr , )*
    ) => {
        {
            let mut partial = $crate::dag::PartialTree::new();
            $( partial.insert($name, $hashed_or_not); )*
            partial
        }
    }
}

// Conversions for HashedOrNot

impl From<CacheStatus> for HashedOrNot {
    fn from(s: CacheStatus) -> Self {
        use cache::CacheStatus::*;
        match s {
            Cached { hash } => HashedOrNot::Hashed(hash),
            Modified { size } |
            NotCached { size } => HashedOrNot::UnhashedFile(size),
        }
    }
}

impl From<PartialTree> for HashedOrNot {
    fn from(pt: PartialTree) -> Self { HashedOrNot::Dir(pt) }
}

impl From<UnhashedPath> for HashedOrNot {
    fn from(unhashed: UnhashedPath) -> Self {
        match unhashed {
            UnhashedPath::File(size) => HashedOrNot::UnhashedFile(size),
            UnhashedPath::Dir(partial) => HashedOrNot::Dir(partial),
        }
    }
}

impl From<ObjectKey> for HashedOrNot {
    fn from(hash: ObjectKey) -> Self { HashedOrNot::Hashed(hash) }
}

// Conversions for UnhashedPath

impl From<ObjectSize> for UnhashedPath {
    fn from(s: ObjectSize) -> Self { UnhashedPath::File(s) }
}

impl From<PartialTree> for UnhashedPath {
    fn from(pt: PartialTree) -> Self { UnhashedPath::Dir(pt) }
}

impl UnhashedPath {
    pub fn unhashed_size(&self) -> ObjectSize {
        match *self {
            UnhashedPath::File(size) => size,
            UnhashedPath::Dir(ref partial_tree) => partial_tree.unhashed_size(),
        }
    }
}

#[cfg(test)]
mod test {

    use std::io;
    use std::path::PathBuf;
    use super::super::*;
    use testutil;

    fn random_hash(rng: &mut testutil::TestRand) -> ObjectKey {
        let rand_bytes = rng.gen_byte_vec(KEY_SIZE_BYTES);
        ObjectKey::from_bytes(rand_bytes.as_slice()).unwrap()
    }

    #[test]
    fn test_write_tree() {
        // Construct object
        let mut rng = testutil::TestRand::default();

        let object = tree_object!{
            "foo" => random_hash(&mut rng),
            "bar" => random_hash(&mut rng),
            "baz" => random_hash(&mut rng),
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

    fn shortkey(num: u8) -> ObjectKey {
        let mut vec = [0u8; KEY_SIZE_BYTES];
        vec[KEY_SIZE_BYTES - 1] = num;
        ObjectKey::from_bytes(&vec).unwrap()
    }

    #[test]
    fn test_tree_sort_by_name() {
        let tree = tree_object!{
            "foo" => shortkey(0),
            "bar" => shortkey(2),
            "baz" => shortkey(1),
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
                "foo" => shortkey(0),
                "bar" => shortkey(2),
                "baz" => shortkey(1),
                "fizz" => UnhashedPath::File(1024),
                "buzz" => partial_tree!{
                    "strange" => UnhashedPath::File(2048),
                },
        };

        assert_eq!(partial.unhashed().get(&PathBuf::from("fizz")),
                   Some(&UnhashedPath::File(1024)));

        assert_eq!(partial.unhashed_size(), 3072);

        assert_eq!(partial.tree(),
                   &tree_object!{
                        "foo" => shortkey(0),
                        "bar" => shortkey(2),
                        "baz" => shortkey(1),
        });

        assert!(!partial.is_complete());

        // Begin adding hashes for incomplete objects

        partial.insert("buzz", shortkey(3));
        assert_eq!(partial.unhashed().get(&PathBuf::from("buzz")),
                   None,
                   "After setting hash, path should be removed from unhashed");
        assert_eq!(partial.unhashed_size(), 1024);

        partial.insert("fizz", shortkey(4));

        // Should be complete now

        assert!(partial.unhashed().is_empty());
        assert!(partial.is_complete());
        assert_eq!(partial.unhashed_size(), 0);

        assert_eq!(partial.tree(),
                   &tree_object!{
                        "foo" => shortkey(0),
                        "bar" => shortkey(2),
                        "baz" => shortkey(1),
                        "fizz" => shortkey(4),
                        "buzz" => shortkey(3),
        });
    }

    #[test]
    fn test_partial_tree_with_zero_unhashed() {
        let partial = partial_tree!{
                "foo" => shortkey(0),
                "bar" => partial_tree!{
                    "baz" => shortkey(1),
                },
        };

        assert_eq!(partial.unhashed_size(), 0, "no files need to be hashed");
        assert_eq!(partial.is_complete(), false, "still incomplete");

        assert_eq!(partial.tree(),
                   &tree_object!{
                        "foo" => shortkey(0),
                   },
                   "not safe to take the tree value: it is missing the \
                    subtree");

        assert_eq!(partial.unhashed().get(&PathBuf::from("bar")),
                   Some(&UnhashedPath::Dir(partial_tree!{
                        "baz" => shortkey(1),
                   })),
                   "the nested PartialTree still holds information that \
                    would be lost if we replaced it with just a hash");
    }
}
