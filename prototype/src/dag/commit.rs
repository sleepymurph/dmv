use std::io;
use super::*;

/// A commit object: a Tree with parents and other metadata
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

impl ObjectCommon for Commit {
    fn object_type(&self) -> ObjectType { ObjectType::Commit }
    fn content_size(&self) -> ObjectSize {
        let content_size = OBJECT_SIZE_BYTES + 1 +
                           OBJECT_SIZE_BYTES * self.parents.len() +
                           self.message.as_bytes().len();
        content_size as ObjectSize
    }

    fn write_content(&self, writer: &mut io::Write) -> io::Result<()> {
        try!(writer.write(self.tree.as_ref()));
        try!(writer.write(&[self.parents.len() as u8]));
        for parent in self.parents.iter() {
            try!(writer.write(parent.as_ref()));
        }
        try!(writer.write(self.message.as_bytes()));

        Ok(())
    }

    fn pretty_print(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();
        let parents_join = self.parents
            .iter()
            .map(|h| h.to_hex())
            .collect::<Vec<String>>()
            .join(",");
        write!(&mut output,
               "Commit

Object content size:    {:>10}
Tree:       {:x}
Parents:    {}

{}
",
               human_readable::human_bytes(self.content_size()),
               self.tree,
               parents_join,
               self.message)
            .unwrap();

        output
    }
}

impl ReadObjectContent for Commit {
    fn read_content<R: io::BufRead>(reader: &mut R) -> Result<Self> {
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

    use std::io;
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
        let readobject = Commit::read_content(&mut reader)
            .expect("read object content");

        assert_eq!(readobject, object);
    }
}
