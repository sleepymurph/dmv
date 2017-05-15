//! Wrappers to implement/adjust serialization for common types

use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use std::borrow::Borrow;
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
/// extern crate dmv;
/// extern crate rustc_serialize;
///
/// use rustc_serialize::json;
/// use dmv::encodable;
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
///
/// # Panics
///
/// Will panic when attempting to encode a path that contains a bad UTF-8
/// sequence.
///
#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct PathBuf(path::PathBuf);
}

impl From<ffi::OsString> for PathBuf {
    fn from(s: ffi::OsString) -> Self { PathBuf(s.into()) }
}

impl<'a, P: ?Sized + AsRef<path::Path>> From<&'a P> for PathBuf {
    fn from(p: &'a P) -> Self { PathBuf(p.as_ref().to_path_buf()) }
}

impl Borrow<path::Path> for PathBuf {
    fn borrow(&self) -> &path::Path { &self.0 }
}

// You may be tempted to implement Borrow<ffi::OsStr> for PathBuf. DO NOT DO IT.
// OsStr and PathBuf can be equal and still have different hashes. So HashMaps
// will not work properly.
//
// impl Borrow<ffi::OsStr> for PathBuf {
//    fn borrow(&self) -> &ffi::OsStr { self.0.as_os_str() }
// }

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
/// extern crate dmv;
/// extern crate rustc_serialize;
///
/// use rustc_serialize::json;
/// use dmv::encodable;
/// use std::time;
///
/// fn main() {
///     let time = encodable::SystemTime::from(
///         time::UNIX_EPOCH + time::Duration::new(120, 55));
///     let encoded = json::encode(&time).unwrap();
///     assert_eq!(encoded, "[120,55]");
/// }
/// ```
/// # Panics
///
/// Panics when attempting to encode a SystemTime that is before the Unix epoch.
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
    pub fn unix_epoch_plus(secs: u64, nanos: u32) -> Self {
        SystemTime(time::UNIX_EPOCH + time::Duration::new(secs, nanos))
    }

    /// Gives a tuple containing the seconds and nanos since the Unix epoch
    ///
    /// # Panics
    ///
    /// Panics if the inner SystemTime is before the Unix epoch.
    ///
    pub fn secs_nanos_since_epoch(&self) -> (u64, u32) {
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
        Ok(SystemTime::unix_epoch_plus(secs, nanos))
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
    use std::collections::hash_map::DefaultHasher;
    use std::ffi;
    use std::hash::Hash;
    use std::hash::Hasher;
    use super::*;

    #[test]
    #[should_panic]
    fn test_serialize_bad_uft8() {
        use std::os::unix::ffi::OsStringExt;

        let obj = PathBuf::from(ffi::OsString::from_vec(vec![0xc3, 0x28]));
        let _ = json::encode(&obj);
        // Panics
    }

    fn hash<T: ?Sized + Hash>(t: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        t.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_path_buf_os_string() {
        let pb = PathBuf::from("hello");

        let as_path = path::PathBuf::from("hello");
        let as_path = as_path.as_path();
        assert_eq!(hash(&pb), hash(as_path));

        let as_os_str = ffi::OsString::from("hello");
        let as_os_str = as_os_str.as_os_str();
        assert_eq!(as_path, as_os_str);
        assert_ne!(hash(as_path), hash(as_os_str));
        assert_ne!(hash(&pb), hash(as_os_str));
    }
}
