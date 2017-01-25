//! Implementation of the directed acyclic graph (DAG)

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

extern crate byteorder;
use self::byteorder::ByteOrder;
use self::byteorder::ReadBytesExt;
use self::byteorder::WriteBytesExt;

use std::io;

/// Type used for sizing and seeking in objects
pub type ObjectSize = u64;

/// Size of ObjectSize type in bytes
pub const OBJECT_SIZE_BYTES: usize = 8;

pub fn write_object_size(writer: &mut io::Write,
                         objectsize: ObjectSize)
                         -> io::Result<()> {
    writer.write_u64::<byteorder::BigEndian>(objectsize)
}

pub fn read_object_size<R: io::Read>(reader: &mut R) -> io::Result<ObjectSize> {
    reader.read_u64::<byteorder::BigEndian>()
}

pub fn object_size_from_bytes(buf: &[u8]) -> ObjectSize {
    byteorder::BigEndian::read_u64(buf)
}

#[derive(Debug)]
pub enum DagError {
    ParseKey { bad_key: String },
    BadObjectHeader { msg: String },
    BadKeyLength { bad_key: Vec<u8> },
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
    pub object_type: ObjectType,
    pub content_size: ObjectSize,
}

impl ObjectHeader {
    pub fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        match self.object_type {
            ObjectType::Blob => {
                try!(writer.write(b"blob"));
            }
            ObjectType::ChunkedBlob => {
                try!(writer.write(b"ckbl"));
            }
            ObjectType::Tree => {
                try!(writer.write(b"tree"));
            }
            ObjectType::Commit => {
                try!(writer.write(b"cmmt"));
            }
        }
        try!(writer.write_u64::<byteorder::BigEndian>(self.content_size));
        Ok(())
    }

    pub fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError> {
        let mut header = [0u8; 12];
        try!(reader.read_exact(&mut header));

        let object_type_marker = &header[0..4];
        let object_type = match object_type_marker {
            b"blob" => ObjectType::Blob,
            b"ckbl" => ObjectType::ChunkedBlob,
            b"tree" => ObjectType::Tree,
            b"cmmt" => ObjectType::Commit,
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
    fn write_to<W: io::Write>(&self, writer: &mut W) -> io::Result<ObjectKey> {
        let mut writer = HashWriter::wrap(writer);
        try!(self.header().write_to(&mut writer));
        try!(self.write_content(&mut writer));
        Ok(writer.hash())
    }

    /// Write content bytes to the given writer
    fn write_content<W: io::Write>(&self, writer: &mut W) -> io::Result<()>;

    /// Read object, content only, from the given reader
    fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError> {
        Self::read_content(reader)
    }

    /// Read object, content only, from the given reader
    fn read_content<R: io::BufRead>(reader: &mut R) -> Result<Self, DagError>;

    /// Print a well-formatted human-readable version of the object
    fn pretty_print(&self) -> String;
}
