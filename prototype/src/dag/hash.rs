//! Code relating to the object hashes used to identify objects in the DAG

use error::*;
use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use std::convert;
use std::fmt;
use std::io;

extern crate crypto;
use self::crypto::digest::Digest;

/// Hash type
pub type Hasher = crypto::sha1::Sha1;

/// Object key size in bits
pub const KEY_SIZE_BITS: usize = 160;

/// Object key size in bytes
pub const KEY_SIZE_BYTES: usize = KEY_SIZE_BITS / 8;

type ObjectKeyByteArray = [u8; KEY_SIZE_BYTES];

/// Hash key for an object
#[derive(Copy,Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct ObjectKey {
    hash: ObjectKeyByteArray,
}

impl ObjectKey {
    /// Creates a new all-zero key
    pub fn zero() -> Self {
        ObjectKey { hash: [0; KEY_SIZE_BYTES] }
    }

    pub fn from_hex(hexstr: &str) -> Result<Self> {
        if hexstr.len() != KEY_SIZE_BYTES * 2 {
            bail!(ErrorKind::ParseKey(hexstr.to_owned()));
        }
        let mut buf = [0u8; KEY_SIZE_BYTES];
        let mut i = 0;
        let mut high = true;

        for char in hexstr.chars() {
            let mut nibble = match char {
                '0'...'9' => char as u8 - b'0',
                'a'...'f' => char as u8 - b'a' + 10,
                'A'...'F' => char as u8 - b'A' + 10,
                _ => bail!(ErrorKind::ParseKey(hexstr.to_owned())),
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

    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(2 * KEY_SIZE_BYTES);
        for byte in &self.hash {
            hex.push_str(&format!("{:02x}", byte));
        }
        hex
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != KEY_SIZE_BYTES {
            bail!(ErrorKind::BadKeyLength(bytes.to_vec()));
        }
        let mut key = Self::zero();
        key.hash.clone_from_slice(bytes);
        Ok(key)
    }
}

impl fmt::LowerHex for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.hash {
            try!(write!(f, "{:02x}", byte))
        }
        Ok(())
    }
}

impl fmt::UpperHex for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.hash {
            try!(write!(f, "{:02X}", byte))
        }
        Ok(())
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Delegate to LowerHex
        <Self as fmt::LowerHex>::fmt(self, f)
    }
}

impl convert::From<ObjectKey> for String {
    fn from(key: ObjectKey) -> String {
        format!("{}", key)
    }
}

impl AsRef<[u8]> for ObjectKey {
    fn as_ref(&self) -> &[u8] {
        &self.hash
    }
}

impl Encodable for ObjectKey {
    fn encode<S: Encoder>(&self,
                          s: &mut S)
                          -> ::std::result::Result<(), S::Error> {
        self.to_hex().encode(s)
    }
}

impl Decodable for ObjectKey {
    fn decode<D: Decoder>(d: &mut D) -> ::std::result::Result<Self, D::Error> {
        let hex = try!(<String>::decode(d));
        ObjectKey::from_hex(&hex).map_err(|_e| unimplemented!())
    }
}

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

    use error::*;
    use rustc_serialize::json;
    use std::io::Write;
    use super::*;

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
                Err(Error(ErrorKind::ParseKey(ref bad_key), _)) if bad_key ==
                                                                   bad => (),
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
    fn test_serialize_objectkey() {
        let obj =
            ObjectKey::from_hex("d3486ae9136e7856bc42212385ea797094475802")
                .unwrap();
        let encoded = json::encode(&obj).unwrap();
        assert_eq!(encoded, "\"d3486ae9136e7856bc42212385ea797094475802\"");
        let decoded: ObjectKey = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}