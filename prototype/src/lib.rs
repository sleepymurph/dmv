#![allow(dead_code)]

mod objectstore;
mod rollinghash;
mod testutil;
mod dag2;

mod dag {
    pub type ObjectKey = String;
    pub type ObjectSize = u64;

    pub enum ObjectType {
        Blob,
        Tree,
        Commit,
    }

    pub struct ObjectStat {
        pub hash: ObjectKey,
        pub objecttype: ObjectType,
        pub size: ObjectSize,
    }
}

mod repo {

    extern crate crypto;

    use std::io::{Read, BufRead, Write, Result, sink};

    use self::crypto::digest::Digest;
    use self::crypto::sha1::Sha1;

    use dag::ObjectKey;
    use objectstore::ObjectStore;
    use rollinghash::ChunkReader;

    pub struct Repository<OS>
        where OS: ObjectStore
    {
        objectstore: OS,
    }

    impl<OS> Repository<OS>
        where OS: ObjectStore
    {
        pub fn new(objectstore: OS) -> Self {
            Repository { objectstore: objectstore }
        }

        pub fn store<R: BufRead>(&mut self,
                                 input: &mut R)
                                 -> Result<ObjectKey> {
            let mut digest = Sha1::new();
            for chunk in ChunkReader::wrap(input) {
                let chunk = try!(chunk);
                let mut incoming = try!(self.objectstore.new_object());
                digest.input(&chunk);
                try!(incoming.write(&chunk));
                try!(self.objectstore
                    .save_object(digest.result_str(), incoming));
            }
            Ok(digest.result_str())
        }

        pub fn has_object(&mut self, key: &ObjectKey) -> bool {
            self.objectstore.has_object(key)
        }
    }

    pub fn hash_object<R: Read>(input: R) -> Result<ObjectKey> {
        hash_and_copy_object(input, sink())
    }

    pub fn hash_and_copy_object<R: Read, W: Write>(mut input: R,
                                                   mut output: W)
                                                   -> Result<ObjectKey> {
        let buf_size = 4096;
        let mut buf: Vec<u8> = Vec::with_capacity(buf_size);
        buf.resize(buf_size, 0);

        let mut digest = Sha1::new();

        loop {
            match input.read(&mut buf) {
                Ok(0) => break,
                Ok(size) => {
                    digest.input(&buf[0..size]);
                    try!(output.write(&buf[0..size]));
                }
                Err(err) => return Err(err),
            }
        }
        Ok(digest.result_str())
    }


    #[cfg(test)]
    mod test {

        use std::io::BufReader;

        use super::*;
        use objectstore::test::InMemoryObjectStore;
        use testutil::RandBytes;

        fn do_hash_and_copy_test(input: &[u8], expected_key: &str) {
            let mut output: Vec<u8> = Vec::new();
            let hash = hash_and_copy_object(input, &mut output)
                .expect("hash input");
            assert_eq!(hash, expected_key);
            assert_eq!(output, input);
        }

        #[test]
        fn test_hash_and_copy_object_simple() {
            do_hash_and_copy_test("Hello!".as_bytes(),
                                  "69342c5c39e5ae5f0077aecc32c0f81811fb8193");
        }

        #[test]
        fn test_hash_and_copy_object_large() {
            let input = [0u8; 1024 * 1024];
            do_hash_and_copy_test(&input,
                                  "3b71f43ff30f4b15b5cd85dd9e95ebc7e84eb5a3");
        }

        #[test]
        fn test_hash_object() {
            let input = "Hello!".as_bytes();
            let expected_key = "69342c5c39e5ae5f0077aecc32c0f81811fb8193";
            let hash = hash_object(input).expect("hash input");
            assert_eq!(hash, expected_key);
        }

        #[test]
        fn test_repo_store() {
            let mut repo = Repository::new(InMemoryObjectStore::new());
            let mut input = "Hello!".as_bytes();
            let expected_key = "69342c5c39e5ae5f0077aecc32c0f81811fb8193";
            let hash = repo.store(&mut input).expect("hash input");
            assert_eq!(hash, expected_key);

            assert!(repo.has_object(&hash));
        }

        #[test]
        #[ignore]
        fn test_repo_store_chunks() {
            let mut rng = RandBytes::new();
            let mut repo = Repository::new(InMemoryObjectStore::new());
            let mut input = BufReader::new(rng.as_read(100 * 1024));
            let expected_key = "69342c5c39e5ae5f0077aecc32c0f81811fb8193";
            let hash = repo.store(&mut input).expect("hash input");
            assert_eq!(hash, expected_key);

            assert!(repo.has_object(&hash));
        }

    }
}
