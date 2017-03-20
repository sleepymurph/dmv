//! Rolling hash implementations, used to break files into chunks

use dag;
use dag::ToHashed;
use std::io::{BufRead, Result};

/// The integer/byte type used to store a rolling hash's value
pub type RollingHashValue = u32;

/// A rolling hash calculator
pub struct RollingHasher {
    value: RollingHashValue,
    window: Vec<u8>,
    window_size: usize,
    pos: usize,
    full: bool,
}

impl RollingHasher {
    pub fn new(window_size: usize) -> Self {
        let mut window = Vec::with_capacity(window_size);
        window.resize(window_size, 0);
        RollingHasher {
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

    pub fn full(&self) -> bool { self.full }

    pub fn value(&self) -> RollingHashValue { self.value }
}

/// Target size for each chunk
///
/// The rolling hash window and the number of bits that have to match will be
/// adjusted so that the probability of hitting a chunk boundary is one out of
/// this number of bytes.
pub const CHUNK_TARGET_SIZE: usize = 15 * 1024;
const WINDOW_SIZE: usize = 4096;
const MATCH_BITS: RollingHashValue = 14;

/// Flags chunk boundaries where the rolling hash has enough zero bits
pub struct ChunkFlagger {
    hasher: RollingHasher,
    mask: RollingHashValue,
}

impl ChunkFlagger {
    pub fn new() -> Self {
        ChunkFlagger {
            hasher: RollingHasher::new(WINDOW_SIZE),
            mask: 1 << MATCH_BITS,
        }
    }

    /// Adds a byte to the hash, returns true if this byte triggers a flag
    pub fn slide(&mut self, byte: u8) {
        if self.flag() {
            self.hasher.reset();
        }
        self.hasher.slide(byte);
    }

    pub fn flag(&self) -> bool {
        self.hasher.full() && (self.hasher.value() % self.mask) == 0
    }

    /// Slides across the buffer, returns position of first flag
    ///
    /// Note that the position points to the byte that triggers the flag. So
    /// this position marks the **end** of the chunk.
    pub fn slide_until(&mut self, buf: &[u8]) -> Option<usize> {
        for bufpos in 0..buf.len() {
            self.slide(buf[bufpos]);
            if self.flag() {
                return Some(bufpos);
            }
        }
        None
    }
}

/// Breaks a file into chunks and emits them as byte vectors
pub struct ChunkReader<R: BufRead> {
    reader: R,
    flagger: ChunkFlagger,
}

impl<R: BufRead> ChunkReader<R> {
    pub fn wrap(reader: R) -> Self {
        ChunkReader {
            reader: reader,
            flagger: ChunkFlagger::new(),
        }
    }

    pub fn read_chunk(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let mut read = 0;
        loop {
            let (done, used);
            {
                let available = try!(self.reader.fill_buf());

                match self.flagger.slide_until(available) {
                    Some(pos) => {
                        buf.extend_from_slice(&available[..pos + 1]);
                        done = true;
                        used = pos + 1;
                    }
                    None => {
                        buf.extend_from_slice(available);
                        done = false;
                        used = available.len();
                    }
                }
            }

            self.reader.consume(used);
            read += used;
            if done || used == 0 {
                return Ok(read);
            }
        }
    }
}

impl<R: BufRead> Iterator for ChunkReader<R> {
    type Item = Result<Vec<u8>>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut buf: Vec<u8> = Vec::new();
        match self.read_chunk(&mut buf) {
            Ok(0) => None,
            Ok(_n) => Some(Ok(buf)),
            Err(e) => Some(Err(e)),
        }
    }
}

/// Breaks a file into chunks and emits them as Objects
///
/// - If the stream is empty, emits one empty Blob.
/// - If the stream contains only one chunk, emits it as a Blob and then stops.
/// - If the stream contains multiple chunks, emits them as Blobs, followed by a
/// final ChunkedBlob.
///
/// ```
/// use prototype::rolling_hash::read_file_objects;
/// use prototype::dag::HashedObject;
/// use std::io::BufReader;
///
/// let file = b"Hello world!".as_ref();
/// let mut objects = Vec::<HashedObject>::new();
/// for object in read_file_objects(BufReader::new(file)) {
///     objects.push(object.unwrap());
/// }
/// ```
pub fn read_file_objects<R: BufRead>(reader: R) -> ObjectReader<R> {
    ObjectReader::wrap(reader)
}

/// Breaks a file into chunks and emits them as Objects
///
/// Usually created by the `read_file_objects` function.
pub struct ObjectReader<R: BufRead> {
    chunker: ChunkReader<R>,
    chunk_index: Option<dag::ChunkedBlob>,
}

impl<R: BufRead> ObjectReader<R> {
    pub fn wrap(reader: R) -> Self {
        ObjectReader {
            chunker: ChunkReader::wrap(reader),
            chunk_index: Some(dag::ChunkedBlob::new()),
        }
    }
}

impl<R: BufRead> Iterator for ObjectReader<R> {
    type Item = Result<dag::HashedObject>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_chunk = self.chunker.next();
        match next_chunk {
            Some(Err(e)) => Some(Err(e)), // Error: Just pass it on
            Some(Ok(chunk)) => {
                // Valid chunk: Wrap as a blob, add to index, and pass on
                match self.chunk_index {
                    Some(ref mut index) => {
                        let blob = dag::Blob::from(chunk);
                        let hashed = index.add_blob(blob);
                        Some(Ok(hashed))
                    }
                    None => unreachable!(), // Index is not consumed until end
                }
            }
            None => {
                // End of chunks
                match self.chunk_index.take() {
                    None => None,   // Index already consumed: End of stream
                    Some(index) => {
                        // Chunks finished, but index pending
                        if index.chunks.len() == 0 {
                            // Zero chunks, file was empty: Emit one empty blob
                            Some(Ok(dag::Blob::empty().to_hashed()))
                        } else if index.chunks.len() == 1 {
                            // Just one chunk: End without index
                            None
                        } else {
                            // Multiple chunks: Emit index object
                            Some(Ok(index.to_hashed()))
                        }
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod test {

    use dag;
    use dag::ToHashed;
    use human_readable::human_bytes;
    use std::collections;
    use std::io;
    use std::io::Write;
    use super::*;
    use testutil::TestRand;
    use testutil::rand::Rng;
    use variance::VarianceCalc;

    #[test]
    /// This test shows that the Rabin value increases slowly after a reset
    ///
    /// It demonstrates why you need to fill the window before checking the
    /// hash.
    fn test_rolling_hash_values() {
        let mut hasher = RollingHasher::new(256);
        let mut hashvals: Vec<RollingHashValue> = Vec::new();
        for byte in TestRand::default().gen_iter::<u8>().take(10) {
            hasher.slide(byte);
            hashvals.push(hasher.value());
        }

        // Uncomment to see all hash values
        // assert_eq!(hashvals, []);

        assert!(hashvals[0] < hashvals[1]);
        assert!(hashvals[1] < hashvals[2]);
    }


    fn mean_std<'a, I>(input: I) -> (usize, usize)
        where I: Iterator<Item = &'a usize>
    {
        let (mut n, mut sum, mut sumsq) = (0, 0, 0);
        for &x in input {
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
    fn test_mean_std() {
        let input: &[usize] = &[2, 4, 4, 4, 5, 5, 7, 9];
        let (expected_mean, expected_std) = (5, 2);
        let (mean, std) = mean_std(input.iter());
        assert_eq!((mean, std), (expected_mean, expected_std));
    }

    #[test]
    fn test_chunk_target_size() {
        const CHUNK_TARGET_MIN: usize = 10 * 1024;
        const CHUNK_TARGET_MAX: usize = 25 * 1024;
        const ACCEPTABLE_DEVIATION: usize = 25 * 1024;
        const CHUNK_REPEAT: usize = 100;

        let mut vcalc = VarianceCalc::new();
        let mut flagger = ChunkFlagger::new();
        let mut last_offset: usize = 0;
        for (offset, byte) in TestRand::default()
            .gen_iter::<u8>()
            .take(CHUNK_TARGET_SIZE * CHUNK_REPEAT)
            .enumerate() {

            flagger.slide(byte);
            if flagger.flag() {
                vcalc.item((offset - last_offset) as i64);
                last_offset = offset;
            }
        }

        let mean = vcalc.mean() as usize;
        let std = vcalc.std() as usize;

        assert!(vcalc.count() > 1 && CHUNK_TARGET_MIN < mean &&
                mean < CHUNK_TARGET_MAX &&
                std < ACCEPTABLE_DEVIATION,
                "count: {}, mean: {}, std: {}",
                vcalc.count(),
                human_bytes(mean as u64),
                human_bytes(std as u64));
    }


    #[test]
    fn test_chunk_reader() {
        let mut rng = TestRand::default();
        let rand_bytes = rng.gen_byte_vec(10 * CHUNK_TARGET_SIZE);
        let mut chunk_read = ChunkReader::wrap(rand_bytes.as_slice());

        let mut chunks: Vec<Vec<u8>> = Vec::new();

        for chunk in &mut chunk_read {
            chunks.push(chunk.expect("read chunk"));
        }

        assert!(chunks.len() > 1,
                format!("Expected input to be broken into chunks. Got {} \
                         chunks",
                        chunks.len()));

        let reconstructed = chunks.into_iter().fold(vec![], |mut a, v| {
            a.extend(v);
            a
        });
        assert_eq!(reconstructed, rand_bytes);
    }

    #[test]
    fn test_object_iterator_empty() {
        let input_bytes = Vec::<u8>::new();
        let mut object_read = read_file_objects(input_bytes.as_slice());

        let obj = object_read.next().expect("Some").expect("Ok");
        assert_eq!(obj,
                   dag::Blob::empty().to_hashed(),
                   "first object should be an empty Blob");

        let obj = object_read.next();
        assert!(obj.is_none(), "should not emit any more objects");
    }

    #[test]
    fn test_object_iterator_one_chunk() {
        let mut rng = TestRand::default();
        let input_bytes = rng.gen_byte_vec(10);
        let mut object_read = read_file_objects(input_bytes.as_slice());

        let obj = object_read.next().expect("Some").expect("Ok");
        assert_eq!(obj,
                   dag::Blob::from(input_bytes.clone()).to_hashed(),
                   "first object should be a blob containing the entire file");

        let obj = object_read.next();
        assert!(obj.is_none(), "should not emit any more objects");
    }


    #[test]
    fn test_object_iterator_two_chunks() {
        do_object_reconstruction_test(CHUNK_TARGET_SIZE, 2);
    }

    #[test]
    fn test_object_iterator_many_chunks() {
        do_object_reconstruction_test(CHUNK_TARGET_SIZE * 10, 9);
    }

    type ObjectStore = collections::HashMap<dag::ObjectKey, dag::Object>;

    fn do_object_reconstruction_test(input_size: usize,
                                     expected_chunks: usize) {
        let mut rng = TestRand::default();
        let input_bytes = rng.gen_byte_vec(input_size);
        let mut object_read = read_file_objects(input_bytes.as_slice());

        let mut objects = ObjectStore::new();
        let last_key = dump_into_store(&mut object_read, &mut objects);

        assert_eq!(objects.len(),
                   expected_chunks + 1,
                   "unexpected number of chunks");

        let reconstructed = reconstruct_file(&objects, &last_key);
        assert_eq!(reconstructed.len(),
                   input_bytes.len(),
                   "reconstructed file has wrong length");
        assert_eq!(reconstructed,
                   input_bytes,
                   "reconstructed file does not match input");
    }

    /// Dump all read objects into an object store, return hash of last object
    fn dump_into_store<R: io::BufRead>(object_read: &mut ObjectReader<R>,
                                       object_store: &mut ObjectStore)
                                       -> dag::ObjectKey {
        let mut last_key = None;
        for obj in object_read {
            let obj = obj.unwrap();
            let (k, v) = obj.to_kv();
            object_store.insert(k, v);
            last_key = Some(k);
        }
        last_key.expect("No objects returned from ObjectReader")
    }

    /// Reconstruct file from ChunkedBlob object key
    fn reconstruct_file(object_store: &ObjectStore,
                        index_key: &dag::ObjectKey)
                        -> Vec<u8> {
        let mut reconstructed = Vec::<u8>::new();

        let index_obj = object_store.get(index_key);
        if let Some(&dag::Object::ChunkedBlob(ref index)) = index_obj {

            for chunk_offset in &index.chunks {
                let chunk = object_store.get(&chunk_offset.hash);
                if let Some(&dag::Object::Blob(ref blob)) = chunk {
                    reconstructed.write(&blob.content).unwrap();
                } else {
                    panic!("Expected Blob, got {:?}", chunk);
                }
            }

        } else {
            panic!("Expected ChunkedBlob, got {:?}", index_obj);
        }
        reconstructed
    }

}
