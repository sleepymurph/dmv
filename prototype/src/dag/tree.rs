use std::collections;
use std::io;
use std::io::Write;
use std::path;

use super::*;

type PathMap = collections::BTreeMap<path::PathBuf, ObjectKey>;
type PathMapIter<'a> = collections::btree_map::Iter<'a,
                                                    path::PathBuf,
                                                    ObjectKey>;

/// A large blob made of many smaller chunks
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct Tree {
    entries: PathMap,
}

impl Tree {
    pub fn new() -> Self {
        Tree { entries: PathMap::new() }
    }

    pub fn insert(&mut self, name: path::PathBuf, hash: ObjectKey) {
        self.entries.insert(name, hash);
    }

    pub fn iter(&self) -> PathMapIter {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

const TREE_ENTRY_SEPARATOR: u8 = b'\n';

impl Object for Tree {
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);

        let tree_size = self.entries.iter().fold(0, |acc, x| {
            acc + KEY_SIZE_BYTES + x.0.as_os_str().len() + 1
        }) as ObjectSize;

        let header = ObjectHeader {
            object_type: ObjectType::Tree,
            content_size: tree_size,
        };

        try!(header.write_to(&mut writer));

        for entry in &self.entries {
            try!(writer.write(entry.1.as_ref()));
            try!(writer.write(entry.0.to_str().unwrap().as_bytes()));
            try!(writer.write(&[TREE_ENTRY_SEPARATOR]));
        }

        Ok(writer.hash())
    }

    fn read_from<R: io::BufRead>(mut reader: &mut R) -> Result<Self, DagError> {
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
            let name = path::Path::new(&name).to_owned();
            tree.insert(name, hash);
        }
        Ok(tree)
    }
}

#[cfg(test)]
mod test {
    use super::super::*;
    use testutil;

    use std::io;
    use std::path;

    fn random_hash(rng: &mut testutil::RandBytes) -> ObjectKey {
        let rand_bytes = rng.next_many(KEY_SIZE_BYTES);
        ObjectKey::from_bytes(rand_bytes.as_slice()).unwrap()
    }

    #[test]
    fn test_write_tree() {
        // Construct object
        let mut rng = testutil::RandBytes::new();

        let mut object = Tree::new();
        object.insert(path::Path::new("foo").to_owned(), random_hash(&mut rng));

        // Write out
        let mut output: Vec<u8> = Vec::new();
        object.write_to(&mut output).expect("write out object");

        // Read in header
        let mut reader = io::BufReader::new(output.as_slice());
        let header = ObjectHeader::read_from(&mut reader).expect("read header");

        assert_eq!(header.object_type, ObjectType::Tree);
        assert_ne!(header.content_size, 0);

        // Read in object content
        let readobject = Tree::read_from(&mut reader)
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
        let mut tree = Tree::new();
        tree.insert(path::Path::new("foo").to_owned(), shortkey(0));
        tree.insert(path::Path::new("bar").to_owned(), shortkey(2));
        tree.insert(path::Path::new("baz").to_owned(), shortkey(1));

        let names: Vec<String> = tree.iter()
            .map(|ent| ent.0.to_str().unwrap().to_string())
            .collect();
        assert_eq!(names, vec!["bar", "baz", "foo"]);
    }
}
