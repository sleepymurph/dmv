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

impl IntoIterator for Tree {
    type Item = (OsString, ObjectKey);
    type IntoIter = <BTreeMap<OsString, ObjectKey> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
}

#[cfg(test)]
mod test {

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
}
