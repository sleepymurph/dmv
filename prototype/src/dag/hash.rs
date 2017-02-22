use crypto::digest::Digest;
use std::io;
use std::ops::Deref;
use super::*;


/// Wraps an existing writer and computes a hash of the bytes going into it
pub struct HashWriter<W: io::Write> {
    writer: W,
    hasher: Hasher,
}

impl<W: io::Write> HashWriter<W> {
    pub fn wrap(writer: W) -> Self {
        HashWriter {
            writer: writer,
            hasher: Hasher::new(),
        }
    }

    pub fn hash(&mut self) -> ObjectKey {
        let mut bytes = [0u8; KEY_SIZE_BYTES];
        self.hasher.result(&mut bytes);
        ObjectKey::from(bytes)
    }
}

impl<W: io::Write> io::Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = try!(self.writer.write(buf));
        self.hasher.input(&buf[0..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> { self.writer.flush() }
}


/// An object and its corresponding known hash
#[derive(Clone,Eq,PartialEq,Hash,Debug)]
pub struct HashedObject {
    hash: ObjectKey,
    object: Object,
}

impl_deref!(HashedObject => Object, object);

impl<O: Into<Object>> From<O> for HashedObject {
    fn from(obj: O) -> Self {
        let obj = obj.into();
        HashedObject {
            hash: obj.calculate_hash(),
            object: obj,
        }
    }
}

/// An object that can be hashed to yield a HashedObject
///
/// From is already implemented for the different object types, but this trait
/// provides a convenient `to_hashed` chain method.
///
/// ```
/// use prototype::dag;
/// use prototype::dag::ToHashed;
///
/// let blob = dag::Blob::from("Hello!".as_bytes().to_owned());
///
/// let hashed_by_from = dag::HashedObject::from(blob.clone());
/// let hashed_by_chain = blob.clone().to_hashed();
///
/// assert_eq!(hashed_by_from, hashed_by_chain);
/// ```
pub trait ToHashed {
    fn to_hashed(self) -> HashedObject;
}

impl<O: Into<Object>> ToHashed for O {
    fn to_hashed(self) -> HashedObject { HashedObject::from(self.into()) }
}

impl HashedObject {
    /// Get the object's hash
    pub fn hash(&self) -> &ObjectKey { &self.hash }
    /// Get the object itself. Also available via Deref
    pub fn object(&self) -> &Object { &self.object }
    /// Unwrap and return as (key,object) tuple
    pub fn to_kv(self) -> (ObjectKey, Object) { (self.hash, self.object) }
}

impl ObjectCommon for HashedObject {
    fn object_type(&self) -> ObjectType { self.deref().object_type() }
    fn content_size(&self) -> ObjectSize { self.deref().content_size() }
    fn write_content(&self, writer: &mut io::Write) -> io::Result<()> {
        self.deref().write_content(writer)
    }
    fn calculate_hash(&self) -> ObjectKey { self.hash().to_owned() }
    fn pretty_print(&self) -> String { self.deref().pretty_print() }
}


#[cfg(test)]
mod test {

    use std::io::Write;
    use super::*;

    #[test]
    fn test_hash_write() {
        let input = b"Hello world!";
        let expected_hash = "d3486ae9136e7856bc42212385ea797094475802";

        let mut output: Vec<u8> = Vec::new();
        {
            let mut hasher = HashWriter::wrap(&mut output);
            hasher.write(input).expect("write input");
            assert_eq!(hasher.hash().to_hex(), expected_hash);
            hasher.flush().expect("flush hash writer");
        }

        assert_eq!(output, input);
    }
}
