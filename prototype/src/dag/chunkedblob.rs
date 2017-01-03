use siprefix;
use std::io;
use std::io::Write;

use super::*;

/// A large blob made of many smaller chunks
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct ChunkedBlob {
    pub total_size: ObjectSize,
    pub chunks: Vec<ChunkOffset>,
}

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

    pub fn pretty_print(&self) -> String {
        use std::fmt::Write;
        let mut output = String::new();
        write!(&mut output,
               "Chunked Blob

Object content size:    {:>10}
Total file size:        {:>10}

",
               siprefix::human_bytes(self.content_size()),
               siprefix::human_bytes(self.total_size))
            .unwrap();

        write!(&mut output, "{:10}  {:10}  {}\n", "offset", "size", "hash")
            .unwrap();

        for chunk in &self.chunks {
            write!(&mut output,
                   "{:>010x}  {:>10}  {}\n",
                   chunk.offset,
                   siprefix::human_bytes(chunk.size),
                   chunk.hash)
                .unwrap();
        }
        output
    }

    fn content_size(&self) -> ObjectSize {
        (OBJECT_SIZE_BYTES +
         self.chunks.len() *
         CHUNK_RECORD_SIZE) as ObjectSize
    }
}

const CHUNK_RECORD_SIZE: usize = OBJECT_SIZE_BYTES * 2 + KEY_SIZE_BYTES;

impl Object for ChunkedBlob {
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);

        let header = ObjectHeader {
            object_type: ObjectType::ChunkedBlob,
            content_size: self.content_size(),
        };

        try!(header.write_to(&mut writer));

        try!(write_object_size(&mut writer, self.total_size));

        for chunk in &self.chunks {
            try!(write_object_size(&mut writer, chunk.offset));
            try!(write_object_size(&mut writer, chunk.size));
            try!(writer.write(chunk.hash.as_ref()));
        }

        Ok(writer.hash())
    }

    fn read_from<R: io::BufRead>(mut reader: &mut R) -> Result<Self, DagError> {
        let mut chunk_record_buf = [0u8; CHUNK_RECORD_SIZE];

        let total_size = try!(read_object_size(&mut reader));
        let mut chunks: Vec<ChunkOffset> = Vec::new();
        loop {
            let bytes_read = try!(reader.read(&mut chunk_record_buf));
            match bytes_read {
                0 => break,
                _ if bytes_read == CHUNK_RECORD_SIZE => {
                    let chunk_offset =
                        object_size_from_bytes(&chunk_record_buf[0..8]);
                    let chunk_size =
                        object_size_from_bytes(&chunk_record_buf[8..16]);
                    let chunk_hash =
                        ObjectKey::from_bytes(&chunk_record_buf[16..]).unwrap();

                    chunks.push(ChunkOffset {
                        offset: chunk_offset,
                        size: chunk_size,
                        hash: chunk_hash,
                    });
                }
                _ => return Err(DagError::from(io::Error::new(
                            io::ErrorKind::UnexpectedEof, ""))),
            }
        }
        Ok(ChunkedBlob {
            total_size: total_size,
            chunks: chunks,
        })
    }
}


#[cfg(test)]
mod test {
    use rollinghash;
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
        let mut rng = testutil::RandBytes::new();
        let rand_bytes = rng.next_many(10 * rollinghash::CHUNK_TARGET_SIZE);

        // Break into chunks, indexed by ChunkedBlob
        let mut chunkedblob = ChunkedBlob::new();
        let mut chunk_store: collections::HashMap<ObjectKey, Blob> =
            collections::HashMap::new();

        {
            let mut chunk_read =
                rollinghash::ChunkReader::wrap(rand_bytes.as_slice());
            for chunk in &mut chunk_read {
                let blob = Blob::from_vec(chunk.expect("chunk"));
                let hash = blob.write_to(&mut io::sink()).expect("write chunk");
                chunkedblob.add_chunk(blob.size(), hash);
                chunk_store.insert(hash, blob);
            }
        }

        (rand_bytes, chunk_store, chunkedblob)
    }

    #[test]
    fn test_chunk_and_reconstruct() {
        let (rand_bytes, chunk_store, chunkedblob) =
            create_random_chunkedblob();

        assert_eq!(chunkedblob.total_size,
                   rand_bytes.len() as ObjectSize,
                   "Cumulative size");

        // Reconstruct original large "file"
        let mut reconstructed: Vec<u8> = Vec::new();
        for chunk_offset in &chunkedblob.chunks {
            let blob = chunk_store.get(&chunk_offset.hash).unwrap();
            reconstructed.write_all(blob.content()).expect("reconstruct chunk");
        }

        assert_eq!(reconstructed, rand_bytes, "reconstructed content");
    }

    #[test]
    fn test_write_chunkedblob() {
        // Construct object
        let (_, _, chunkedblob) = create_random_chunkedblob();

        // Write out
        let mut output: Vec<u8> = Vec::new();
        chunkedblob.write_to(&mut output).expect("write out chunked blob");

        // Read in header
        let mut reader = io::BufReader::new(output.as_slice());
        let header = ObjectHeader::read_from(&mut reader).expect("read header");

        assert_eq!(header.object_type, ObjectType::ChunkedBlob);
        assert_ne!(header.content_size, 0);

        // Read in object content
        let readobject = ChunkedBlob::read_from(&mut reader)
            .expect("read object content");

        assert_eq!(readobject, chunkedblob);
    }
}
