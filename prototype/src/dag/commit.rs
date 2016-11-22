use std::io;
use std::io::Write;

use super::*;

/// A large blob made of many smaller chunks
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct Commit {
    pub tree: ObjectKey,
    pub parents: Vec<ObjectKey>,
    pub message: String,
}

impl Commit {
    pub fn new() -> Self {
        Commit {
            tree: ObjectKey::zero(),
            parents: Vec::new(),
            message: String::new(),
        }
    }
}

impl Object for Commit {
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);

        let content_size = OBJECT_SIZE_BYTES + 1 +
                           OBJECT_SIZE_BYTES * self.parents.len() +
                           self.message.as_bytes().len();
        let content_size = content_size as ObjectSize;

        let header = ObjectHeader {
            object_type: ObjectType::Commit,
            content_size: content_size,
        };

        try!(header.write_to(&mut writer));

        try!(writer.write(self.tree.as_ref()));
        try!(writer.write(&[self.parents.len() as u8]));
        for parent in self.parents.iter() {
            try!(writer.write(parent.as_ref()));
        }
        try!(writer.write(self.message.as_bytes()));

        Ok(writer.hash())
    }

    fn read_from<R: io::BufRead>(mut reader: &mut R) -> Result<Self, DagError> {
        let mut hash_buf = [0u8; KEY_SIZE_BYTES];
        try!(reader.read_exact(&mut hash_buf));
        let tree = ObjectKey::from_bytes(&hash_buf).unwrap();

        let mut num_parents_buf = [0u8; 1];
        try!(reader.read_exact(&mut num_parents_buf));
        let num_parents = num_parents_buf[0];
        let mut parents: Vec<ObjectKey> =
            Vec::with_capacity(num_parents as usize);

        for _ in 0..num_parents {
            try!(reader.read_exact(&mut hash_buf));
            let parent = ObjectKey::from_bytes(&hash_buf).unwrap();
            parents.push(parent);
        }

        let mut message = String::new();
        try!(reader.read_to_string(&mut message));

        Ok(Commit {
            tree: tree,
            parents: parents,
            message: message,
        })
    }
}


#[cfg(test)]
mod test {
    use super::super::*;
    use testutil;

    use std::io;

    fn random_hash(rng: &mut testutil::RandBytes) -> ObjectKey {
        let rand_bytes = rng.next_many(KEY_SIZE_BYTES);
        ObjectKey::from_bytes(rand_bytes.as_slice()).unwrap()
    }

    #[test]
    fn test_write_tree() {
        // Construct object
        let mut rng = testutil::RandBytes::new();

        let mut object = Commit::new();
        object.tree = random_hash(&mut rng);
        object.parents.push(random_hash(&mut rng));
        object.parents.push(random_hash(&mut rng));
        object.parents.push(random_hash(&mut rng));
        object.message = String::from("Test Commit");

        // Write out
        let mut output: Vec<u8> = Vec::new();
        object.write_to(&mut output).expect("write out object");

        // Read in header
        let mut reader = io::BufReader::new(output.as_slice());
        let header = ObjectHeader::read_from(&mut reader).expect("read header");

        assert_eq!(header.object_type, ObjectType::Commit);
        assert_ne!(header.content_size, 0);

        // Read in object content
        let readobject = Commit::read_from(&mut reader)
            .expect("read object content");

        assert_eq!(readobject, object);
    }
}