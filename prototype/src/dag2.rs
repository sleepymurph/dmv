use std::fmt;
use std::convert;

/// Object key size in bits
pub const KEY_SIZE_BITS: usize = 160;

/// Object key size in bytes
pub const KEY_SIZE_BYTES: usize = KEY_SIZE_BITS / 8;

type ObjectKeyByteArray = [u8; KEY_SIZE_BYTES];

#[derive(Debug,PartialEq,Eq)]
pub enum DagError {
    ParseKey { bad_key: String },
}

#[derive(Copy,Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct ObjectKey {
    hash: ObjectKeyByteArray,
}

impl ObjectKey {
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

#[cfg(test)]
mod test {
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
            assert_eq!(ObjectKey::from_hex(&bad),
                       Err(DagError::ParseKey { bad_key: bad.into() }),
                       "{}",
                       desc);
        }
    }
}
