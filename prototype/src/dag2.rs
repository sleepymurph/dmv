//! Implementation of the directed acyclic graph (DAG)

extern crate byteorder;
use self::byteorder::WriteBytesExt;
use self::byteorder::ByteOrder;

extern crate crypto;
use self::crypto::digest::Digest;

use std::convert;
use std::fmt;
use std::io;
use std::io::Write;

/// Type used for sizing and seeking in objects
pub type ObjectSize = u64;

/// Hash type
type Hasher = crypto::sha1::Sha1;

/// Object key size in bits
pub const KEY_SIZE_BITS: usize = 160;

/// Object key size in bytes
pub const KEY_SIZE_BYTES: usize = KEY_SIZE_BITS / 8;

type ObjectKeyByteArray = [u8; KEY_SIZE_BYTES];

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

/// Hash key for an object
#[derive(Copy,Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct ObjectKey {
    hash: ObjectKeyByteArray,
}

impl ObjectKey {
    /// Creates a new all-zero key
    fn zero() -> Self {
        ObjectKey { hash: [0; KEY_SIZE_BYTES] }
    }

    pub fn from_hex(hexstr: &str) -> Result<Self, DagError> {
        if hexstr.len() != KEY_SIZE_BYTES * 2 {
            return Err(DagError::ParseKey { bad_key: hexstr.to_string() });
        }
        let mut buf = [0u8; KEY_SIZE_BYTES];
        let mut i = 0;
        let mut high = true;

        for char in hexstr.chars() {
            let mut nibble = match char {
                '0'...'9' => char as u8 - b'0',
                'a'...'f' => char as u8 - b'a' + 10,
                'A'...'F' => char as u8 - b'A' + 10,
                _ => {
                    return Err(DagError::ParseKey {
                        bad_key: hexstr.to_string(),
                    })
                }
            };
            if high {
                nibble <<= 4;
            }
            buf[i] += nibble;
            high = !high;
            if high {
                i += 1;
            }
        }
        Ok(ObjectKey { hash: buf })
    }
}

impl fmt::LowerHex for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for byte in &self.hash {
            try!(write!(f, "{:02x}", byte))
        }
        Ok(())
    }
}

impl fmt::UpperHex for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        for byte in &self.hash {
            try!(write!(f, "{:02X}", byte))
        }
        Ok(())
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        // Delegate to LowerHex
        <Self as fmt::LowerHex>::fmt(self, f)
    }
}

impl convert::From<ObjectKey> for String {
    fn from(key: ObjectKey) -> String {
        format!("{}", key)
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

/// Wraps an existing writer and computes a hash of the bytes going into it
pub struct HashWriter<W: io::Write> {
    writer: W,
    hasher: Hasher,
}

impl<W: io::Write> HashWriter<W> {
    fn wrap(writer: W) -> Self {
        HashWriter {
            writer: writer,
            hasher: Hasher::new(),
        }
    }

    fn hash(&mut self) -> ObjectKey {
        let mut key = ObjectKey::zero();
        self.hasher.result(&mut key.hash);
        key
    }
}

impl<W: io::Write> io::Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = try!(self.writer.write(buf));
        self.hasher.input(&buf[0..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}


#[cfg(test)]
mod test {
    use super::*;

    use std::io;
    use std::io::Write;

    #[test]
    fn test_key_hex_conversions() {
        let hex = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let upperhex = hex.to_uppercase();
        let key = ObjectKey::from_hex(hex).expect("parse lower key");
        let upperkey = ObjectKey::from_hex(&upperhex).expect("parse upper key");

        assert_eq!(upperkey, key, "Parse upper hex vs lower hex");
        assert_eq!(format!("{}", key), hex, "Display mismatch");
        assert_eq!(format!("{:x}", key), hex, "LowerHex mismatch");
        assert_eq!(format!("{:X}", key), upperhex, "UpperHex mismatch");

        let from = String::from(key);
        assert_eq!(from, hex, "From conversion");

        let into: String = key.into();
        assert_eq!(into, hex, "Into converstion");
    }

    #[test]
    fn test_key_bad_hex() {
        let bad_inputs =
            [("bad characters", "da39a3ee5e6b4b0_32+5bfef95601890afd80709"),
             ("too short", "da39a3ee5e6b4b0d3255bfef95601890afd807"),
             ("too long", "da39a3ee5e6b4b0d3255bfef95601890afd807090000")];

        for &(desc, bad) in bad_inputs.into_iter() {
            match ObjectKey::from_hex(&bad) {
                Err(DagError::ParseKey { ref bad_key }) if bad_key == bad => (),
                other => {
                    panic!(format!("parsing \"{}\" input. Expected error. \
                                    Got: {:?}",
                                   desc,
                                   other))
                }
            }
        }
    }

    #[test]
    fn test_hash_write() {
        let input = b"Hello world!";
        let expected_hash =
            ObjectKey::from_hex("d3486ae9136e7856bc42212385ea797094475802")
                .unwrap();

        let mut output: Vec<u8> = Vec::new();
        {
            let mut hasher = HashWriter::wrap(&mut output);
            hasher.write(input).expect("write input");
            assert_eq!(hasher.hash(), expected_hash);
            hasher.flush().expect("flush hash writer");
        }

        assert_eq!(output, input);
    }

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
