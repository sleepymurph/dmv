use crypto::sha1::Sha1;
use regex::Regex;
use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use std::convert;
use std::fmt;
use std::io::Read;
use std::str::FromStr;
use super::*;
#[cfg(test)]
use testutil::rand;

/// Hash type
pub type Hasher = Sha1;

/// Object key size in bits
const KEY_SIZE_BITS: usize = 160;

/// Object key size in bytes
pub const KEY_SIZE_BYTES: usize = KEY_SIZE_BITS / 8;

/// Number of hex digits required to represent an object key
pub const KEY_SIZE_HEX_DIGITS: usize = KEY_SIZE_BITS / 4;

pub const KEY_SHORT_LEN: usize = 8;

lazy_static!{
    pub static ref OBJECT_KEY_PAT:Regex = Regex::new(
            &format!("^[[:xdigit:]]{{ {} }}$",KEY_SIZE_HEX_DIGITS)).unwrap();
}

/// Hash key for an object
///
/// On formatting:
///
/// - Display ({}) gives a short hash (this is a lossy operation)
/// - Hex ({:x}) gives the full hash
/// - The conversion From<ObjectKey> for String also gives the full hash
///
/// ```
/// use dmv::dag::ObjectKey;
///
/// let id = ObjectKey::parse("da39a3ee5e6b4b0d3255bfef95601890afd80709")
///             .unwrap();
///
/// assert_eq!(format!("{}", id), "da39a3ee");
///
/// assert_eq!(format!("{:x}", id),
///             "da39a3ee5e6b4b0d3255bfef95601890afd80709");
///
/// assert_eq!(String::from(id),
///             "da39a3ee5e6b4b0d3255bfef95601890afd80709");
/// ```
///
#[derive(Copy,Clone,Eq,PartialEq,Ord,PartialOrd,Hash)]
pub struct ObjectKey([u8; KEY_SIZE_BYTES]);

impl ObjectKey {
    pub fn parse(hexstr: &str) -> Result<Self> {
        if !OBJECT_KEY_PAT.is_match(hexstr) {
            bail!(ErrorKind::BadObjectKey(hexstr.to_owned()));
        }
        let mut buf = [0u8; KEY_SIZE_BYTES];
        let mut i = 0;
        let mut high = true;

        for char in hexstr.chars() {
            let mut nibble = match char {
                '0'...'9' => char as u8 - b'0',
                'a'...'f' => char as u8 - b'a' + 10,
                'A'...'F' => char as u8 - b'A' + 10,
                _ => bail!(ErrorKind::BadObjectKey(hexstr.to_owned())),
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
        Ok(ObjectKey(buf))
    }

    /// Give full hex string for this ObjectKey
    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(KEY_SIZE_HEX_DIGITS);
        for byte in &self.0 {
            hex.push_str(&format!("{:02x}", byte));
        }
        hex
    }

    /// Give a shortened hex string for this ObjectKey
    pub fn to_short(&self) -> String {
        self.to_hex()[..KEY_SHORT_LEN].to_owned()
    }

    /// Creates an array from a byte slice (copy)
    ///
    /// Will panic if the slice is the wrong length.
    ///
    /// If the input is a byte array of the correct length (`[u8;
    /// KEY_SIZE_BYTES]`), you can use From<[u8; KEY_SIZE_BYTES]> instead. That
    /// conversion is guaranteed to succeed, and it consumes and uses the array,
    /// rather than performing another allocation.
    ///
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut key = [0; KEY_SIZE_BYTES];
        key.copy_from_slice(bytes);
        ObjectKey(key)
    }

    /// Read the next bytes as an ObjectKey
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        let mut hash_buf = [0u8; KEY_SIZE_BYTES];
        reader.read_exact(&mut hash_buf)
            .chain_err(|| "Error reading ObjectKey")?;
        Ok(ObjectKey::from(hash_buf))
    }
}

impl<'a> From<&'a ObjectKey> for ObjectKey {
    fn from(hash: &'a ObjectKey) -> Self { hash.to_owned() }
}

impl From<[u8; KEY_SIZE_BYTES]> for ObjectKey {
    fn from(arr: [u8; KEY_SIZE_BYTES]) -> Self { ObjectKey(arr) }
}

impl FromStr for ObjectKey {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> { ObjectKey::parse(s) }
}

/// Create an ObjectKey with a recognizable pattern
#[cfg(test)]
pub fn object_key(num: u8) -> ObjectKey {
    ObjectKey::from([num; KEY_SIZE_BYTES])
}

// In test, a shortcut to parse a String literal and unwrap it
#[cfg(test)]
impl From<&'static str> for ObjectKey {
    fn from(s: &'static str) -> Self {
        Self::parse(s).expect("String literal is not a valid ObjectKey")
    }
}

#[cfg(test)]
impl rand::Rand for ObjectKey {
    fn rand<R: rand::Rng>(rng: &mut R) -> Self {
        let mut buf = [0u8; KEY_SIZE_BYTES];
        rng.fill_bytes(&mut buf);
        ObjectKey::from(buf)
    }
}

impl fmt::LowerHex for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.0 {
            try!(write!(f, "{:02x}", byte))
        }
        Ok(())
    }
}

impl fmt::UpperHex for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for byte in &self.0 {
            try!(write!(f, "{:02X}", byte))
        }
        Ok(())
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_short())
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("ObjectKey")
            .field(&self.to_short())
            .finish()
    }
}

impl convert::From<ObjectKey> for String {
    fn from(key: ObjectKey) -> String { format!("{:x}", key) }
}

impl AsRef<[u8]> for ObjectKey {
    fn as_ref(&self) -> &[u8] { &self.0 }
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
        ObjectKey::parse(&hex).map_err(|_e| unimplemented!())
    }
}


#[cfg(test)]
mod test {

    use rustc_serialize::json;
    use super::*;

    #[test]
    fn test_key_hex_conversions() {
        let hex = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let upperhex = "DA39A3EE5E6B4B0D3255BFEF95601890AFD80709";
        let key = ObjectKey::from(hex);
        let upperkey = ObjectKey::from(upperhex);
        let short = key.to_short();

        assert_eq!(upperkey, key, "Parse upper hex vs lower hex");
        assert_eq!(format!("{}", key), short, "Display mismatch");
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
            match ObjectKey::parse(&bad) {
                Err(Error(ErrorKind::BadObjectKey(ref bad_key), _))
                    if bad_key == bad => (),
                other => {
                    panic!("parsing \"{}\" input. Expected error. Got: {:?}",
                           desc,
                           other)
                }
            }
        }
    }

    #[test]
    fn test_serialize_objectkey() {
        let obj = ObjectKey::from("d3486ae9136e7856bc42212385ea797094475802");
        let encoded = json::encode(&obj).unwrap();
        assert_eq!(encoded, "\"d3486ae9136e7856bc42212385ea797094475802\"");
        let decoded: ObjectKey = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
