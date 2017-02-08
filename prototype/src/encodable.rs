//! Wrappers that allow common types to implement Encodable and Decodable

use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use std::ffi;
use std::hash::{Hash, Hasher};
use std::path;
use std::time;


wrapper_struct!{
/// Encodable wrapper for `std::path::PathBuf` that encodes to a string
///
/// Rustc Serialize does implement Encodable for Path and PathBuf, but it
/// serializes them as arrays of bytes. This accommodates bad UTF-8 sequences,
/// but it makes the output hard to read, and it makes it impossible to use
/// paths as map/object keys.
///
/// This wrapper sacrifices UTF-8 error tolerance for clear serialization as a
/// string. It will panic if it attempts to encode a path with a bad Unicode
/// sequence.
///
/// ```
/// extern crate prototype;
/// extern crate rustc_serialize;
///
/// use rustc_serialize::json;
/// use prototype::encodable;
/// use std::path;
///
/// fn main() {
///     let path = path::PathBuf::from("hello/world");
///     let encoded = json::encode(&path).unwrap();
///     assert_eq!(encoded, "[104,101,108,108,111,47,119,111,114,108,100]");
///
///     let path = encodable::PathBuf::from("hello/world");
///     let encoded = json::encode(&path).unwrap();
///     assert_eq!(encoded, "\"hello/world\"");
/// }
/// ```
#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct PathBuf(path::PathBuf);
}

impl From<ffi::OsString> for PathBuf {
    fn from(s: ffi::OsString) -> Self { PathBuf(s.into()) }
}

impl<'a, P: ?Sized + AsRef<path::Path>> From<&'a P> for PathBuf {
    fn from(p: &'a P) -> Self { PathBuf(p.as_ref().to_path_buf()) }
}

impl Encodable for PathBuf {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        self.to_str()
            .expect("path cannot be encoded to because it contains invalid \
                     unicode")
            .encode(s)
    }
}

impl Decodable for PathBuf {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let s = try!(String::decode(d));
        Ok(PathBuf::from(&s))
    }
}


wrapper_struct!{
/// Encodable wrapper for `std::time::SystemTime`
///
/// Serializes the system time as an array of [secs,nanos], relative to the Unix
/// epoch.
///
/// ```
/// extern crate prototype;
/// extern crate rustc_serialize;
///
/// use rustc_serialize::json;
/// use prototype::encodable;
/// use std::time;
///
/// fn main() {
///     let time = encodable::SystemTime::from(
///         time::UNIX_EPOCH + time::Duration::new(120, 55));
///     let encoded = json::encode(&time).unwrap();
///     assert_eq!(encoded, "[120,55]");
/// }
/// ```
///
#[derive(Clone,Eq,PartialEq,Debug)]
pub struct SystemTime(time::SystemTime);
}

impl SystemTime {
    /// Construct a test system time relative to the Unix epoch
    ///
    /// This is a convenience method for testing, since constructing a specific
    /// SystemTime instance is quite verbose.
    ///
    #[cfg(test)]
    pub fn unix_epoch_plus(secs: u64, nanos: u32) -> Self {
        SystemTime(time::UNIX_EPOCH + time::Duration::new(secs, nanos))
    }

    fn secs_nanos_since_epoch(&self) -> (u64, u32) {
        let since_epoch = self.duration_since(time::UNIX_EPOCH)
            .expect("mod time was before the Unix Epoch");
        (since_epoch.as_secs(), since_epoch.subsec_nanos())
    }
}

impl Encodable for SystemTime {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        self.secs_nanos_since_epoch().encode(s)
    }
}

impl Decodable for SystemTime {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let (secs, nanos) = try!(<(u64, u32)>::decode(d));
        Ok(SystemTime(time::UNIX_EPOCH + time::Duration::new(secs, nanos)))
    }
}

impl Hash for SystemTime {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.secs_nanos_since_epoch().hash(state)
    }
}


#[cfg(test)]
mod test {
    use rustc_serialize::json;
    use std::ffi;
    use super::*;

    #[test]
    #[should_panic]
    fn test_serialize_bad_uft8() {
        use std::os::unix::ffi::OsStringExt;

        let obj = PathBuf::from(ffi::OsString::from_vec(vec![0xc3, 0x28]));
        let _ = json::encode(&obj);
        // Panics
    }
}
