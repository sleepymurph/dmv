use super::*;

use std::io;
use std::io::Write;

/// Blobs
///
/// Blobs are a special case because often when dealing with the DAG we
/// don't need to read in the actual data. So we have an Option for the
/// binary data, which represents whether the data has been loaded or not.
/// It will not be read in by default, but it will be necessary when writing
/// out, in order to finish the write and compute the hash.
///
/// Blobs are assumed to be able to fit in memory because of the way that
/// large files are broken into chunks when stored. So it should be safe to
/// use a `Vec<u8>` to hold the contents.
#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct Blob {
    content: Vec<u8>,
}

impl From<Vec<u8>> for Blob {
    fn from(v: Vec<u8>) -> Blob {
        Blob::from_vec(v)
    }
}

impl Blob {
    pub fn from_vec(v: Vec<u8>) -> Blob {
        Blob { content: v }
    }
}

impl Object for Blob {
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);
        let header = ObjectHeader {
            object_type: ObjectType::Blob,
            content_size: self.content.len() as ObjectSize,
        };
        try!(header.write_to(&mut writer));
        try!(writer.write(&self.content));
        Ok(writer.hash())
    }
    fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError> {
        let mut content: Vec<u8> = Vec::new();
        try!(reader.read_to_end(&mut content));
        Ok(Blob { content: content })
    }
}


#[cfg(test)]
mod test {
    use super::super::*;

    use std::io;

    #[test]
    fn test_write_blob() {
        let content = b"Hello world!";
        let content_size = content.len() as ObjectSize;
        let blob = Blob::from_vec(content.to_vec());

        let mut output: Vec<u8> = Vec::new();
        blob.write_to(&mut output).expect("write out blob");

        // panic!(format!("{:?}",output));

        let mut reader = io::BufReader::new(output.as_slice());
        let header = ObjectHeader::read_from(&mut reader).expect("read header");

        assert_eq!(header,
                   ObjectHeader {
                       object_type: ObjectType::Blob,
                       content_size: content_size,
                   });

        let readblob = Blob::read_from(&mut reader).expect("read rest of blob");

        assert_eq!(readblob,
                   blob,
                   "Should be able to get the rest of the content by \
                    continuing to read from the same reader.");
    }
}
