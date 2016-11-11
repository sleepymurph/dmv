#![allow(dead_code)]
mod objectstore;

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

    use std::io::{Read, Write, Result, sink};

    use self::crypto::digest::Digest;
    use self::crypto::sha1::Sha1;

    use dag::ObjectKey;

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

        use super::*;

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
    }
}
