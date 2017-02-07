use error::*;
use std::io::BufRead;
use std::io::Write;
use super::*;

/// A handle to an open Object file, with the header parsed but not the content
///
/// After destructuring, each inner handle will have a parse() method that will
/// read the content as the appropriate object.
///
/// Specific object types may have other options. For instance, a BlobHandle can
/// copy its data in a streaming fashion without loading it all into memory.
///
/// That decision of streaming vs loading is the main motivation behind this
/// type.
///
/// ```
/// use prototypelib::dag::{ObjectHandle, ObjectCommon, Blob};
/// use std::io::Cursor;
///
/// let blob = Blob::from(Vec::from("12345"));
/// let mut file = Vec::<u8>::new();
/// blob.write_to(&mut file).unwrap();
///
/// let handle = ObjectHandle::read(Box::new(Cursor::new(file))).unwrap();
/// match handle {
///     ObjectHandle::Blob(bh) => {
///         let mut copy = Vec::<u8>::new();
///         bh.copy_content(&mut copy).unwrap();
///     }
///     ObjectHandle::Tree(th) => {
///         let tree = th.parse().unwrap();
///         // Do something with tree...
///     }
///     _ => unimplemented!(),
/// }
/// ```
///
pub enum ObjectHandle {
    Blob(BlobHandle),
    ChunkedBlob(ChunkedBlobHandle),
    Tree(TreeHandle),
    Commit(CommitHandle),
}

struct RawObjectHandle {
    header: ObjectHeader,
    file: Box<BufRead>,
}

pub struct BlobHandle(RawObjectHandle);
pub struct ChunkedBlobHandle(RawObjectHandle);
pub struct TreeHandle(RawObjectHandle);
pub struct CommitHandle(RawObjectHandle);

impl ObjectHandle {
    /// Parse the header of the given file to give an ObjectHandle
    pub fn read(mut file: Box<BufRead>) -> Result<Self> {
        let header = ObjectHeader::read_from(&mut file)?;
        let raw = RawObjectHandle {
            header: header,
            file: file,
        };
        let wrapped = match raw.header.object_type {
            ObjectType::Blob => ObjectHandle::Blob(BlobHandle(raw)),
            ObjectType::ChunkedBlob => {
                ObjectHandle::ChunkedBlob(ChunkedBlobHandle(raw))
            }
            ObjectType::Tree => ObjectHandle::Tree(TreeHandle(raw)),
            ObjectType::Commit => ObjectHandle::Commit(CommitHandle(raw)),
        };
        Ok(wrapped)
    }

    fn raw(&self) -> &RawObjectHandle {
        match *self {
            ObjectHandle::Blob(BlobHandle(ref raw)) |
            ObjectHandle::ChunkedBlob(ChunkedBlobHandle(ref raw)) |
            ObjectHandle::Tree(TreeHandle(ref raw)) |
            ObjectHandle::Commit(CommitHandle(ref raw)) => raw,
        }
    }

    /// Get a reference to the header data
    pub fn header(&self) -> &ObjectHeader { &self.raw().header }

    /// Load and parse the rest of the file to get a complete Object
    pub fn parse(self) -> Result<Object> {
        let obj = match self {
            ObjectHandle::Blob(handle) => Object::Blob(handle.parse()?),
            ObjectHandle::ChunkedBlob(handle) => {
                Object::ChunkedBlob(handle.parse()?)
            }
            ObjectHandle::Tree(handle) => Object::Tree(handle.parse()?),
            ObjectHandle::Commit(handle) => Object::Commit(handle.parse()?),
        };
        Ok(obj)
    }
}

pub trait ReadObjectHandle: Sized {
    type Parsed: ReadObjectContent;
    fn parse(self) -> Result<Self::Parsed>;
}

impl BlobHandle {
    pub fn copy_content<W: ?Sized + Write>(mut self,
                                           writer: &mut W)
                                           -> Result<()> {
        use std::io::copy;
        let copied = copy(&mut self.0.file, writer)?;
        assert_eq!(copied, self.0.header.content_size);
        Ok(())
    }
}

impl ReadObjectHandle for BlobHandle {
    type Parsed = Blob;
    fn parse(mut self) -> Result<Blob> {
        Blob::read_content(&mut self.0.file)
    }
}

impl ReadObjectHandle for ChunkedBlobHandle {
    type Parsed = ChunkedBlob;
    fn parse(mut self) -> Result<Self::Parsed> {
        ChunkedBlob::read_content(&mut self.0.file)
    }
}

impl ReadObjectHandle for TreeHandle {
    type Parsed = Tree;
    fn parse(mut self) -> Result<Self::Parsed> {
        Tree::read_content(&mut self.0.file)
    }
}

impl ReadObjectHandle for CommitHandle {
    type Parsed = Commit;
    fn parse(mut self) -> Result<Self::Parsed> {
        Commit::read_content(&mut self.0.file)
    }
}
