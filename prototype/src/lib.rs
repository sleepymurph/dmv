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

mod rollinghash {

    pub type RollingHashValue = u32;

    pub struct RollingHash {
        value: RollingHashValue,
        window: Vec<u8>,
        window_size: usize,
        pos: usize,
        full: bool,
    }

    impl RollingHash {
        pub fn new(window_size: usize) -> Self {
            let mut window = Vec::with_capacity(window_size);
            window.resize(window_size, 0);
            RollingHash {
                value: 0,
                window: vec![0; window_size],
                window_size: window_size,
                pos: 0,
                full: false,
            }
        }

        pub fn reset(&mut self) {
            self.window.clear();
            self.window.resize(self.window_size, 0);
            self.pos = 0;
            self.full = false;
            self.value = 0;
        }

        pub fn slide(&mut self, byte: u8) {
            let outgoing = self.window[self.pos] as RollingHashValue;
            let incoming = byte as RollingHashValue;
            self.value = self.value - outgoing + incoming;
            self.window[self.pos] = byte;
            self.pos = (self.pos + 1) % self.window_size;
            if self.pos == 0 {
                self.full = true;
            }
        }

        pub fn full(&self) -> bool {
            self.full
        }

        pub fn value(&self) -> RollingHashValue {
            self.value
        }
    }

    const WINDOW_SIZE:usize = 4096;
    const MATCH_BITS: RollingHashValue = 13;

    pub struct ChunkFlagger {
        hasher: RollingHash,
        mask: RollingHashValue,
    }

    impl ChunkFlagger {
        pub fn new() -> Self {
            let mut mask: RollingHashValue = 1;
            for _ in 0..MATCH_BITS {
                mask = (mask << 1) + 1;
            }
            ChunkFlagger {
                hasher: RollingHash::new(WINDOW_SIZE),
                mask: mask,
            }
        }

        pub fn slide(&mut self, byte: u8) -> bool {
            self.hasher.slide(byte);

            if self.hasher.full() && (self.hasher.value() & self.mask) == 0 {
                self.hasher.reset();
                true
            } else {
                false
            }
        }
    }

    #[cfg(test)]
    mod test {

        use super::*;
        use std::io::{Read, BufReader, Bytes};
        use std::fs::File;

        fn rand_bytes() -> Bytes<BufReader<File>> {
            let urandom = File::open("/dev/urandom").expect("open urandom");
            let bytereader = BufReader::new(urandom);
            bytereader.bytes()
        }

        #[test]
        /// This test shows that the Rabin value increases slowly after a reset
        ///
        /// It demonstrates why you need to fill the window before checking the
        /// hash.
        fn test_rolling_hash_values() {
            let mut hasher = RollingHash::new(256);
            let mut hashvals: Vec<RollingHashValue> = Vec::new();
            for byte in rand_bytes().take(10) {
                hasher.slide(byte.unwrap());
                hashvals.push(hasher.value());
            }

            // Uncomment to see all hash values
            // assert_eq!(hashvals, []);

            assert!(hashvals[0] < hashvals[1]);
            assert!(hashvals[1] < hashvals[2]);
        }


        const CHUNK_TARGET_SIZE: usize = 8 * 1024;
        const CHUNK_REPEAT: usize = 50;

        fn mean_std<I>(input: I) -> (usize, usize)
            where I: Iterator<Item = usize>
        {
            let (mut n, mut sum, mut sumsq) = (0, 0, 0);
            for x in input {
                n += 1;
                sum += x;
                sumsq += x * x;
            }
            let mean = sum / n;
            let var = (sumsq - sum * sum / n) / (n - 1);
            let std = (var as f64).sqrt() as usize;
            (mean, std)
        }

        #[test]
        fn test_chunk_flag_happens() {
            let mut flagger = ChunkFlagger::new();
            let mut chunk_offsets: Vec<usize> = Vec::new();
            for (count, byte) in rand_bytes()
                .take(CHUNK_TARGET_SIZE * CHUNK_REPEAT)
                .enumerate() {
                if flagger.slide(byte.unwrap()) {
                    chunk_offsets.push(count);
                }
            }
            assert!(chunk_offsets.len() > 0,
                    "Expected input to be broken in to chunks, but no chunks \
                     were found.");

            let mut chunk_sizes: Vec<usize> = Vec::new();
            chunk_sizes.push(chunk_offsets[0]);
            for i in 1..chunk_offsets.len() {
                chunk_sizes.push(chunk_offsets[i] - chunk_offsets[i - 1]);
            }

            // Uncomment to get all chunk sizes
            assert_eq!(chunk_sizes, []);

            let (mean, std) = mean_std(chunk_sizes.into_iter());
            assert_eq!((mean, std), (0, 0));
        }
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
