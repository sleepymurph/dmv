use super::*;

use std::io;
use std::io::Write;

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
}

impl Object for ChunkedBlob {
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        unimplemented!();
        // let mut writer = HashWriter::wrap(writer);
        // let header = ObjectHeader {
        // object_type: ObjectType::Blob,
        // content_size: self.content.len() as ObjectSize,
        // };
        // try!(header.write_to(&mut writer));
        // try!(writer.write(&self.content));
        // Ok(writer.hash())
        //
    }
    fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError> {
        unimplemented!();
        // let mut content: Vec<u8> = Vec::new();
        // try!(reader.read_to_end(&mut content));
        // Ok(Blob { content: content })
        //
    }
}


#[cfg(test)]
mod test {
    use super::super::*;
    use testutil;
    use rollinghash;

    use std::io;
    use std::io::Write;
    use std::collections;

    #[test]
    fn test_chunk_and_reconstruct() {
        // Set up a "file" of random bytes
        let mut rng = testutil::RandBytes::new();
        let rand_bytes = rng.next_many(10 * rollinghash::CHUNK_TARGET_SIZE);

        // Break into chunks, indexed by ChunkedBlob
        let mut chunk_read =
            rollinghash::ChunkReader::wrap(rand_bytes.as_slice());
        let mut chunkedblob = ChunkedBlob::new();
        let mut chunk_store: collections::HashMap<ObjectKey, Blob> =
            collections::HashMap::new();

        for chunk in &mut chunk_read {
            let blob = Blob::from_vec(chunk.expect("chunk"));
            let hash = blob.write_to(&mut io::sink()).expect("write chunk");
            chunkedblob.add_chunk(blob.size(), hash);
            chunk_store.insert(hash, blob);
        }

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
}
