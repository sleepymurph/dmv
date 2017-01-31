use error::*;
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

    pub fn insert(&mut self, name: PathBuf, hash: ObjectKey) {
        self.entries.insert(name, hash);
    }

    pub fn iter(&self) -> PathKeyMapIter { self.entries.iter() }

    pub fn len(&self) -> usize { self.entries.len() }
}

#[macro_export]
macro_rules! tree_object {
    ( $( $path:expr => $hash:expr, )* ) => {
        {
            let mut tree = $crate::dag::Tree::new();
            $(
                tree.insert(::std::path::PathBuf::from($path),
                            $crate::dag::ObjectKey::from($hash));
            )*
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
        let mut name_buf: Vec<u8> = Vec::new();
        let mut hash_buf = [0u8; KEY_SIZE_BYTES];

        let mut tree = Tree::new();

        loop {
            let bytes_read = try!(reader.read(&mut hash_buf));
            if bytes_read == 0 {
                break;
            }

            let hash = try!(ObjectKey::from_bytes(&hash_buf));

            try!(reader.read_until(TREE_ENTRY_SEPARATOR, &mut name_buf));
            name_buf.pop(); // Drop the string-ending separator
            let name = String::from_utf8(name_buf.clone()).unwrap();
            let name = PathBuf::from(&name);
            tree.insert(name, hash);
        }
        Ok(tree)
    }
}


type UnhashedMap = collections::BTreeMap<PathBuf, UnhashedPath>;

/// An incomplete Tree object that requires some files to be hashed
pub struct PartialTree {
    tree: Tree,
    unhashed: UnhashedMap,
    unhashed_size: ObjectSize,
}

pub enum UnhashedPath {
    File(ObjectSize),
    Dir(PartialTree),
}

impl PartialTree {
    pub fn new() -> Self { PartialTree::from(Tree::new()) }
    pub fn unhashed_size(&self) -> ObjectSize { self.unhashed_size }

    pub fn insert_hash(&mut self, path: PathBuf, hash: ObjectKey) {
        if let Some(u) = self.unhashed.remove(&path) {
            self.unhashed_size -= u.unhashed_size();
        }
        self.tree.insert(path, hash);
    }

    pub fn insert_unhashed(&mut self, path: PathBuf, unknown: UnhashedPath) {
        self.unhashed_size += unknown.unhashed_size();
        self.unhashed.insert(path, unknown);
    }

    pub fn insert_unhashed_file(&mut self, path: PathBuf, s: ObjectSize) {
        self.insert_unhashed(path, UnhashedPath::File(s))
    }

    pub fn insert_unhashed_dir(&mut self, path: PathBuf, p: PartialTree) {
        self.insert_unhashed(path, UnhashedPath::Dir(p))
    }

    pub fn unhashed(&self) -> &UnhashedMap { &self.unhashed }
}

impl From<Tree> for PartialTree {
    fn from(t: Tree) -> Self {
        PartialTree {
            tree: t,
            unhashed: UnhashedMap::new(),
            unhashed_size: 0,
        }
    }
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
    use super::super::*;
    use testutil;

    fn random_hash(rng: &mut testutil::RandBytes) -> ObjectKey {
        let rand_bytes = rng.next_many(KEY_SIZE_BYTES);
        ObjectKey::from_bytes(rand_bytes.as_slice()).unwrap()
    }

    #[test]
    fn test_write_tree() {
        // Construct object
        let mut rng = testutil::RandBytes::new();

        let object = tree_object!{
            "foo" => random_hash(&mut rng),
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
}
