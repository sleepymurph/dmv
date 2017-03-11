//! Implementation of the directed acyclic graph (DAG)

use byteorder;
use byteorder::ByteOrder;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use error::*;
use human_readable;
use std::fmt;
use std::io;

mod object_key;
pub use self::object_key::*;

mod hash;
pub use self::hash::*;

mod blob;
pub use self::blob::*;

mod chunked_blob;
pub use self::chunked_blob::*;

#[macro_use]
mod tree;
pub use self::tree::*;

mod commit;
pub use self::commit::*;

mod object_handle;
pub use self::object_handle::*;


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
pub fn read_object_size(reader: &mut io::Read) -> io::Result<ObjectSize> {
    reader.read_u64::<byteorder::BigEndian>()
}

/// Read an ObjectSize value from a byte array
pub fn object_size_from_bytes(buf: &[u8]) -> ObjectSize {
    byteorder::BigEndian::read_u64(buf)
}

/// Simple enum to represent the available object types
#[derive(Clone,Copy,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub enum ObjectType {
    Blob,
    ChunkedBlob,
    Tree,
    Commit,
}

impl ObjectType {
    pub fn is_treeish(&self) -> bool {
        self == &ObjectType::Tree || self == &ObjectType::Commit
    }
    pub fn is_blobish(&self) -> bool {
        self == &ObjectType::Blob || self == &ObjectType::ChunkedBlob
    }
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let type_str = match self {
            &ObjectType::Blob => "Blob",
            &ObjectType::ChunkedBlob => "Chunked Blob Index",
            &ObjectType::Tree => "Tree",
            &ObjectType::Commit => "Commit",
        };
        write!(f, "{}", type_str)
    }
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

    pub fn read_content<R: io::BufRead>(&self,
                                        reader: &mut R)
                                        -> Result<Object> {
        let object = match self.object_type {
            ObjectType::Blob => Object::Blob(Blob::read_content(reader)?),
            ObjectType::ChunkedBlob => {
                Object::ChunkedBlob(ChunkedBlob::read_content(reader)?)
            }
            ObjectType::Tree => Object::Tree(Tree::read_content(reader)?),
            ObjectType::Commit => Object::Commit(Commit::read_content(reader)?),
        };
        Ok(object)
    }
}

impl fmt::Display for ObjectHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "{}, size: {}",
               self.object_type,
               human_readable::human_bytes(self.content_size))
    }
}

/// Common operations on all dag object types
pub trait ObjectCommon {
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
        use std::io::Write;
        let mut writer = HashWriter::wrap(writer);
        try!(self.header().write_to(&mut writer));
        try!(self.write_content(&mut writer));
        try!(writer.flush());
        Ok(writer.hash())
    }

    /// Write content bytes to the given writer
    fn write_content(&self, writer: &mut io::Write) -> io::Result<()>;

    /// Calculate the hash key for this object
    fn calculate_hash(&self) -> ObjectKey {
        self.write_to(&mut io::sink())
            .expect("IO error should be impossible when writing to a sink")
    }

    /// Print a well-formatted human-readable version of the object
    fn pretty_print(&self) -> String;
}

/// Common read operation for all dag object types
///
/// This trait is separate from `ObjectCommon` so that `ObjectCommon` can be
/// used as a trait object.
pub trait ReadObjectContent: Sized {
    /// Read and parse content bytes from reader (after header)
    fn read_content<R: io::BufRead>(reader: &mut R) -> Result<Self>;
}

/// A container that holds an object of any type
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub enum Object {
    Blob(Blob),
    ChunkedBlob(ChunkedBlob),
    Tree(Tree),
    Commit(Commit),
}

impl Object {
    /// Reads the entire object, header and content, from the given file
    pub fn read_from<R: io::BufRead>(reader: &mut R) -> Result<Self> {
        let header = try!(ObjectHeader::read_from(reader));
        header.read_content(reader)
    }
}

impl From<Blob> for Object {
    fn from(o: Blob) -> Self { Object::Blob(o) }
}
impl From<ChunkedBlob> for Object {
    fn from(o: ChunkedBlob) -> Self { Object::ChunkedBlob(o) }
}
impl From<Tree> for Object {
    fn from(o: Tree) -> Self { Object::Tree(o) }
}
impl From<Commit> for Object {
    fn from(o: Commit) -> Self { Object::Commit(o) }
}

/// An object that can be wrapped in an Object enum
///
/// From is already implemented for the different object types, but this trait
/// provides a convenient `to_object` chain method.
///
/// ```
/// use prototype::dag;
/// use prototype::dag::ToObject;
///
/// let blob = dag::Blob::from("Hello!".as_bytes().to_owned());
///
/// let object_by_from = dag::Object::from(blob.clone());
/// let object_by_chain = blob.clone().to_object();
///
/// assert_eq!(object_by_from, object_by_chain);
/// ```
pub trait ToObject {
    fn to_object(self) -> Object;
}

impl<O: Into<Object>> ToObject for O {
    fn to_object(self) -> Object { self.into() }
}

macro_rules! for_all_object_types {
    ($match_expr:expr, $inner_pat:pat, $return_expr:expr) => {
        match $match_expr {
            Object::Blob($inner_pat) => $return_expr,
            Object::ChunkedBlob($inner_pat) => $return_expr,
            Object::Tree($inner_pat) => $return_expr,
            Object::Commit($inner_pat) => $return_expr,
        }
    }
}

impl ObjectCommon for Object {
    fn object_type(&self) -> ObjectType {
        for_all_object_types!(*self, ref o, o.object_type())
    }
    fn content_size(&self) -> ObjectSize {
        for_all_object_types!(*self, ref o, o.content_size())
    }
    fn write_content(&self, writer: &mut io::Write) -> io::Result<()> {
        for_all_object_types!(*self, ref o, o.write_content(writer))
    }
    fn pretty_print(&self) -> String {
        for_all_object_types!(*self, ref o, o.pretty_print())
    }
}
