//! Implementation of the directed acyclic graph (DAG)

mod objectkey;
pub use self::objectkey::*;

extern crate byteorder;
use self::byteorder::WriteBytesExt;
use self::byteorder::ByteOrder;

use std::io;
use std::io::Write;

/// Type used for sizing and seeking in objects
pub type ObjectSize = u64;

#[derive(Debug)]
pub enum DagError {
    ParseKey { bad_key: String },
    BadObjectHeader { msg: String },
    IoError(io::Error),
}

impl From<io::Error> for DagError {
    fn from(err: io::Error) -> Self {
        DagError::IoError(err)
    }
}

#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub enum ObjectType {
    Blob,
    ChunkedBlob,
    Tree,
    Commit,
}

#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct ObjectHeader {
    object_type: ObjectType,
    content_size: ObjectSize,
}

impl ObjectHeader {
    pub fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self.object_type {
            ObjectType::Blob => {
                try!(writer.write(b"blob"));
                try!(writer.write_u64::<byteorder::BigEndian>(self.content_size));
            }
            _ => unimplemented!(),
        }
        Ok(())
    }

    pub fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError> {
        let mut header = [0u8; 12];
        try!(reader.read_exact(&mut header));

        let object_type_marker = &header[0..4];
        let object_type = match object_type_marker {
            b"blob" => ObjectType::Blob,
            _ => {
                return Err(DagError::BadObjectHeader {
                    msg: format!("Unrecognized object type bytes: {:?}",
                                 object_type_marker),
                })
            }
        };
        let content_size = byteorder::BigEndian::read_u64(&header[4..12]);

        Ok(ObjectHeader {
            object_type: object_type,
            content_size: content_size,
        })
    }
}

pub trait Object: Sized {
    /// Write object, header AND content, to the give writer
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey>;

    /// Read object, content only, from the given reader
    fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError>;
}

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
    fn from_vec(v: Vec<u8>) -> Blob {
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
    use super::*;

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
