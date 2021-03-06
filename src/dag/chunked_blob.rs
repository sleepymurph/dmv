use human_readable;
use std::fmt;
use std::io;
use super::*;

/// A large blob made of many smaller chunks
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct ChunkedBlob {
    pub total_size: ObjectSize,
    pub chunks: Vec<ChunkOffset>,
}

/// An individual record inside a ChunkedBlob
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct ChunkOffset {
    pub offset: ObjectSize,
    pub size: ObjectSize,
    pub hash: ObjectKey,
}

impl ChunkedBlob {
    pub fn new() -> Self {
        ChunkedBlob {
            total_size: 0,
            chunks: Vec::new(),
        }
    }

    pub fn add_chunk(&mut self, size: ObjectSize, hash: ObjectKey) {
        let new_chunk = ChunkOffset {
            offset: self.total_size,
            size: size,
            hash: hash,
        };
        self.chunks.push(new_chunk);
        self.total_size += size;
    }

    /// Add a blob to the chunk index
    ///
    /// Because adding the blob requires calculating the hash, we give the blob
    /// back along with the hash, as a HashedObject. This way the hash can be
    /// reused.
    pub fn add_blob(&mut self, blob: Blob) -> HashedObject {
        let size = blob.content_size();
        let hashed = blob.to_hashed();
        self.add_chunk(size, hashed.hash().to_owned());
        hashed
    }
}

const CHUNK_RECORD_SIZE: usize = OBJECT_SIZE_BYTES * 2 + KEY_SIZE_BYTES;

impl ObjectCommon for ChunkedBlob {
    fn object_type(&self) -> ObjectType { ObjectType::ChunkedBlob }

    fn content_size(&self) -> ObjectSize {
        (OBJECT_SIZE_BYTES * 2 +
         self.chunks.len() *
         CHUNK_RECORD_SIZE) as ObjectSize
    }

    fn write_content(&self, writer: &mut io::Write) -> io::Result<()> {
        try!(write_object_size(writer, self.total_size));
        try!(write_object_size(writer, self.chunks.len() as ObjectSize));

        for chunk in &self.chunks {
            try!(write_object_size(writer, chunk.offset));
            try!(write_object_size(writer, chunk.size));
            try!(writer.write(chunk.hash.as_ref()));
        }

        Ok(())
    }

    fn pretty_print(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();
        write!(&mut output,
               "Chunked Blob Index

Chunks:                 {:>10}
Object content size:    {:>10}
Total file size:        {:>10}

",
               self.chunks.len(),
               human_readable::human_bytes(self.content_size()),
               human_readable::human_bytes(self.total_size))
            .unwrap();

        write!(&mut output, "{:10}  {:10}  {}\n", "offset", "size", "hash")
            .unwrap();

        for chunk in &self.chunks {
            write!(&mut output,
                   "{:>010x}  {:>10}  {:x}\n",
                   chunk.offset,
                   human_readable::human_bytes(chunk.size),
                   chunk.hash)
                .unwrap();
        }
        output
    }
}


impl ReadObjectContent for ChunkedBlob {
    fn read_content<R: io::BufRead>(reader: &mut R) -> Result<Self> {
        let mut chunk_record_buf = [0u8; CHUNK_RECORD_SIZE];

        let total_size = try!(read_object_size(reader));
        let num_chunks = try!(read_object_size(reader));
        let mut chunks: Vec<ChunkOffset> = Vec::new();
        loop {
            match reader.read_exact(&mut chunk_record_buf) {
                Ok(()) => {
                    let chunk_offset =
                        object_size_from_bytes(&chunk_record_buf[0..8]);
                    let chunk_size =
                        object_size_from_bytes(&chunk_record_buf[8..16]);
                    let chunk_hash =
                        ObjectKey::from_bytes(&chunk_record_buf[16..]);

                    chunks.push(ChunkOffset {
                        offset: chunk_offset,
                        size: chunk_size,
                        hash: chunk_hash,
                    });
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => bail!(e),
            }
        }
        if chunks.len() as ObjectSize != num_chunks {
            bail!("Only read {} of {} chunks", chunks.len(), num_chunks);
        }
        Ok(ChunkedBlob {
            total_size: total_size,
            chunks: chunks,
        })
    }
}


impl fmt::Display for ChunkOffset {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "ChunkOffset( {} at {:#010x}, {} )",
               self.hash,
               self.offset,
               human_readable::human_bytes(self.size))
    }
}

#[cfg(test)]
mod test {
    use rolling_hash;
    use std::collections;

    use std::io;
    use std::io::Write;
    use super::super::*;
    use testutil;

    fn create_random_chunkedblob
        ()
        -> (Vec<u8>, collections::HashMap<ObjectKey, Blob>, ChunkedBlob)
    {
        // Set up a "file" of random bytes
        let mut rng = testutil::TestRand::default();
        let rand_bytes = rng.gen_byte_vec(10 * rolling_hash::CHUNK_TARGET_SIZE);

        // Break into chunks, indexed by ChunkedBlob
        let mut chunked_blob = ChunkedBlob::new();
        let mut chunk_store: collections::HashMap<ObjectKey, Blob> =
            collections::HashMap::new();

        {
            let mut chunk_read =
                rolling_hash::ChunkReader::wrap(rand_bytes.as_slice());
            for chunk in &mut chunk_read {
                let blob = Blob::from(chunk.expect("chunk"));
                let hash = blob.write_to(&mut io::sink()).expect("write chunk");
                chunked_blob.add_chunk(blob.content_size(), hash);
                chunk_store.insert(hash, blob);
            }
        }

        (rand_bytes, chunk_store, chunked_blob)
    }

    #[test]
    fn test_chunk_and_reconstruct() {
        let (rand_bytes, chunk_store, chunked_blob) =
            create_random_chunkedblob();

        assert_eq!(chunked_blob.total_size,
                   rand_bytes.len() as ObjectSize,
                   "Cumulative size");

        // Reconstruct original large "file"
        let mut reconstructed: Vec<u8> = Vec::new();
        for chunk_offset in &chunked_blob.chunks {
            let blob = chunk_store.get(&chunk_offset.hash).unwrap();
            reconstructed.write_all(blob.content()).expect("reconstruct chunk");
        }

        assert_eq!(reconstructed, rand_bytes, "reconstructed content");
    }

    #[test]
    fn test_write_chunkedblob() {
        // Construct object
        let (_, _, chunked_blob) = create_random_chunkedblob();

        // Write out
        let mut output: Vec<u8> = Vec::new();
        chunked_blob.write_to(&mut output).expect("write out chunked blob");

        // Read in header
        let mut reader = io::BufReader::new(output.as_slice());
        let header = ObjectHeader::read_from(&mut reader).expect("read header");

        assert_eq!(header.object_type, ObjectType::ChunkedBlob);
        assert_ne!(header.content_size, 0);

        // Read in object content
        let readobject = ChunkedBlob::read_content(&mut reader)
            .expect("read object content");

        assert_eq!(readobject, chunked_blob);
    }
}
