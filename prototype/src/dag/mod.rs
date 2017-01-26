//! Implementation of the directed acyclic graph (DAG)

use byteorder;
use byteorder::ByteOrder;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use error::*;
use std::io;

mod objectkey;
pub use self::objectkey::*;

mod blob;
pub use self::blob::*;

mod chunkedblob;
pub use self::chunkedblob::*;

mod tree;
pub use self::tree::*;

mod commit;
pub use self::commit::*;


/// Type used for sizing and seeking in objects
pub type ObjectSize = u64;

/// Size of ObjectSize type in bytes
pub const OBJECT_SIZE_BYTES: usize = 8;

/// Write an ObjectSize value to a Write stream
pub fn write_object_size(writer: &mut io::Write,
                         objectsize: ObjectSize)
                         -> io::Result<()> {
    writer.write_u64::<byteorder::BigEndian>(objectsize)
}

/// Read an ObjectSize value from a Read stream
pub fn read_object_size<R: io::Read>(reader: &mut R) -> io::Result<ObjectSize> {
    reader.read_u64::<byteorder::BigEndian>()
}

/// Read an ObjectSize value from a byte array
pub fn object_size_from_bytes(buf: &[u8]) -> ObjectSize {
    byteorder::BigEndian::read_u64(buf)
}

/// Simple enum to represent the available object types
#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub enum ObjectType {
    Blob,
    ChunkedBlob,
    Tree,
    Commit,
}

/// Metadata common to all objects that is written to the header of object files
#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct ObjectHeader {
    pub object_type: ObjectType,
    pub content_size: ObjectSize,
}

impl ObjectHeader {
    pub fn write_to(&self, writer: &mut io::Write) -> io::Result<()> {
        let object_type_marker = match self.object_type {
            ObjectType::Blob => b"blob",
            ObjectType::ChunkedBlob => b"ckbl",
            ObjectType::Tree => b"tree",
            ObjectType::Commit => b"cmmt",
        };
        try!(writer.write(object_type_marker));
        try!(writer.write_u64::<byteorder::BigEndian>(self.content_size));
        Ok(())
    }

    pub fn read_from(reader: &mut io::Read) -> Result<Self> {
        let mut header = [0u8; 12];
        try!(reader.read_exact(&mut header));

        let object_type_marker = &header[0..4];
        let object_type = match object_type_marker {
            b"blob" => ObjectType::Blob,
            b"ckbl" => ObjectType::ChunkedBlob,
            b"tree" => ObjectType::Tree,
            b"cmmt" => ObjectType::Commit,
            _ => {
                bail!(ErrorKind::BadObjectHeader(format!("Unrecognized \
                                                          object type \
                                                          bytes: {:?}",
                                                         object_type_marker)))
            }
        };
        let content_size = byteorder::BigEndian::read_u64(&header[4..12]);

        Ok(ObjectHeader {
            object_type: object_type,
            content_size: content_size,
        })
    }
}

/// Common operations on all objects
pub trait ObjectCommon: Sized {
    fn object_type(&self) -> ObjectType;
    fn content_size(&self) -> ObjectSize;

    /// Create a header for this object
    fn header(&self) -> ObjectHeader {
        ObjectHeader {
            object_type: self.object_type(),
            content_size: self.content_size(),
        }
    }

    /// Write object, header AND content, to the given writer
    fn write_to(&self, writer: &mut io::Write) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);
        try!(self.header().write_to(&mut writer));
        try!(self.write_content(&mut writer));
        Ok(writer.hash())
    }

    /// Write content bytes to the given writer
    fn write_content(&self, writer: &mut io::Write) -> io::Result<()>;

    /// Read object, content only, from the given reader
    fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self> {
        Self::read_content(reader)
    }

    /// Read object, content only, from the given reader
    fn read_content<R: io::BufRead>(reader: &mut R) -> Result<Self>;

    /// Print a well-formatted human-readable version of the object
    fn pretty_print(&self) -> String;
}
