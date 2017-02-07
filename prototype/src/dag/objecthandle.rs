use error::*;
use std::io::BufRead;
use std::io::Write;
use std::marker::PhantomData;
use super::*;

/// A handle to an open Object file, with the header parsed but not the content
///
/// After destructuring, each inner handle will have a `read_content` method
/// that will read the content as the appropriate object.
///
/// Specific object types may have other options. For instance, a BlobHandle can
/// copy its data in a streaming fashion without loading it all into memory.
///
/// That decision of streaming vs loading is the main motivation behind this
/// type.
///
/// ```
/// use prototypelib::dag::{ObjectHandle, Object, ObjectCommon, Blob};
/// use std::io::Cursor;
///
/// // Create a test blob "file"
/// let mut file = Vec::new();
/// Blob::empty().write_to(&mut file);
/// let file = Box::new(Cursor::new(file));
///
/// // Read file
/// let handle = ObjectHandle::read_header(file).unwrap();
/// match handle {
///     ObjectHandle::Blob(bh) => {
///         // Blob can copy content as a stream
///         let mut copy = Vec::<u8>::new();
///         bh.copy_content(&mut copy).unwrap();
///     }
///     ObjectHandle::Tree(th) => {
///         // Others can be read in a type-safe manner
///         let tree = th.read_content().unwrap();
///         for (k, v) in tree.iter() {
///             // ...
///         }
///     }
///     other => {
///         // Can be read to an Object enum as well
///         let object = other.read_content().unwrap();
///         object.pretty_print();
///     }
/// }
/// ```
///
pub enum ObjectHandle {
    Blob(RawHandle<Blob>),
    ChunkedBlob(RawHandle<ChunkedBlob>),
    Tree(RawHandle<Tree>),
    Commit(RawHandle<Commit>),
}

/// Type-differentiated object handle, inner type of each ObjectHandle variant
///
/// All have a `read_content` method which reads the rest of the file to give
/// the appropriate DAG object.
///
/// Specific types may have additional methods, such as the `copy_content`
/// method when working with a Blob.
///
pub struct RawHandle<O: ReadObjectContent> {
    header: ObjectHeader,
    file: Box<BufRead>,
    phantom: PhantomData<O>,
}

impl ObjectHandle {
    /// Create an ObjectHandle by reading the header of the given file
    pub fn read_header(mut file: Box<BufRead>) -> Result<Self> {
        let header = ObjectHeader::read_from(&mut file)?;
        let handle = match header.object_type {
            ObjectType::Blob => {
                ObjectHandle::Blob(RawHandle::new(header, file))
            }
            ObjectType::ChunkedBlob => {
                ObjectHandle::ChunkedBlob(RawHandle::new(header, file))
            }
            ObjectType::Tree => {
                ObjectHandle::Tree(RawHandle::new(header, file))
            }
            ObjectType::Commit => {
                ObjectHandle::Commit(RawHandle::new(header, file))
            }
        };
        Ok(handle)
    }

    /// Get the header
    pub fn header(&self) -> &ObjectHeader {
        match *self {
            ObjectHandle::Blob(ref raw) => &raw.header,
            ObjectHandle::ChunkedBlob(ref raw) => &raw.header,
            ObjectHandle::Tree(ref raw) => &raw.header,
            ObjectHandle::Commit(ref raw) => &raw.header,
        }
    }

    /// Read and parse the rest of the file, returning an Object enum
    pub fn read_content(self) -> Result<Object> {
        let obj = match self {
            ObjectHandle::Blob(raw) => Object::Blob(raw.read_content()?),
            ObjectHandle::ChunkedBlob(raw) => {
                Object::ChunkedBlob(raw.read_content()?)
            }
            ObjectHandle::Tree(raw) => Object::Tree(raw.read_content()?),
            ObjectHandle::Commit(raw) => Object::Commit(raw.read_content()?),
        };
        Ok(obj)
    }
}

impl<O: ReadObjectContent> RawHandle<O> {
    fn new(header: ObjectHeader, file: Box<BufRead>) -> Self {
        RawHandle {
            header: header,
            file: file,
            phantom: PhantomData,
        }
    }

    /// Read and parse the rest of the file, returning the appropriate object
    pub fn read_content(mut self) -> Result<O> {
        O::read_content(&mut self.file)
    }
}

impl RawHandle<Blob> {
    /// Stream-copy the contents of the blob to the given writer
    pub fn copy_content<W: ?Sized + Write>(mut self,
                                           writer: &mut W)
                                           -> Result<()> {
        use std::io::copy;
        let copied = copy(&mut self.file, writer)?;
        assert_eq!(copied, self.header.content_size);
        Ok(())
    }
}
