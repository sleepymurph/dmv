use std::io;
use std::io::Write;
use std::path;

use super::*;

/// A large blob made of many smaller chunks
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct Tree {
    pub entries: Vec<TreeEntry>,
}

#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct TreeEntry {
    pub hash: ObjectKey,
    pub name: path::PathBuf,
}

impl Tree {
    pub fn new() -> Self {
        Tree { entries: Vec::new() }
    }

    pub fn add_entry(&mut self, hash: ObjectKey, name: &path::Path) {
        let new_entry = TreeEntry {
            hash: hash,
            name: name.to_owned(),
        };
        self.entries.push(new_entry);
    }
}

const TREE_ENTRY_SEPARATOR: u8 = b'\n';

impl Object for Tree {
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);

        let tree_size = self.entries.iter().fold(0, |acc, x| {
            acc + KEY_SIZE_BYTES + x.name.as_os_str().len() + 1
        }) as ObjectSize;

        let header = ObjectHeader {
            object_type: ObjectType::Tree,
            content_size: tree_size,
        };

        try!(header.write_to(&mut writer));

        for entry in &self.entries {
            try!(writer.write(entry.hash.as_ref()));
            try!(writer.write(entry.name.to_str().unwrap().as_bytes()));
            try!(writer.write(&[TREE_ENTRY_SEPARATOR]));
        }

        Ok(writer.hash())
    }

    fn read_from<R: io::BufRead>(mut reader: &mut R) -> Result<Self, DagError> {
        let mut entry_buf: Vec<u8> = Vec::new();

        let mut entries: Vec<TreeEntry> = Vec::new();
        loop {
            let bytes_read = try!(reader.read_until(TREE_ENTRY_SEPARATOR, &mut entry_buf));
            if bytes_read == 0 {
                break;
            }
            let hash = ObjectKey::from_bytes(&entry_buf[0..KEY_SIZE_BYTES])
                .unwrap();
            let mut name = entry_buf[KEY_SIZE_BYTES..].to_owned();
            name.pop(); // Drop the string-ending separator
            let name = String::from_utf8(name).unwrap();
            let name = path::Path::new(&name).to_owned();
            entries.push(TreeEntry {
                hash: hash,
                name: name,
            });
        }
        Ok(Tree { entries: entries })
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
        object.entries.push(TreeEntry{
            hash: random_hash(&mut rng),
            name: path::Path::new("foo").to_owned(),
        });

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
}
