use dag;
use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use std::collections;
use std::path;
use std::time;

/// Status used to detect file changes
#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct FileStats {
    size: dag::ObjectSize,
    mtime: CacheTime,
}

#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct CacheEntry {
    filestats: FileStats,
    hash: dag::ObjectKey,
}

type CacheMap = collections::HashMap<path::PathBuf, CacheEntry>;

#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct FileCache {
    map: CacheMap,
}

#[derive(Clone,Eq,PartialEq,Debug)]
pub struct CacheTime(time::SystemTime);

impl Encodable for CacheTime {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let since_epoch = self.0.duration_since(time::UNIX_EPOCH).unwrap();
        s.emit_tuple(2, |s| {
            s.emit_tuple_arg(0, |s| s.emit_u64(since_epoch.as_secs())).and(
            s.emit_tuple_arg(1, |s| s.emit_u32(since_epoch.subsec_nanos())))
        })
    }
}

impl Decodable for CacheTime {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let (secs, nanos) = try!(d.read_tuple(2, |d| {
            let secs = try!(d.read_tuple_arg(0, |d| d.read_u64()));
            let nanos = try!(d.read_tuple_arg(1, |d| d.read_u32()));
            Ok((secs, nanos))
        }));
        Ok(CacheTime(time::UNIX_EPOCH + time::Duration::new(secs, nanos)))
    }
}

mod test {
    use rustc_serialize::json;
    use std::time;
    use super::*;

    #[test]
    fn test_serialize() {
        let cache = CacheTime(time::UNIX_EPOCH + time::Duration::new(120, 55));
        let encoded = json::encode(&cache).unwrap();
        assert_eq!(encoded, "[120,55]");
        let decoded: CacheTime = json::decode(&encoded).unwrap();
        assert_eq!(decoded, cache);
    }
}
