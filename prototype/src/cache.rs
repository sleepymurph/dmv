use dag;
use fsutil;
use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use std::collections;
use std::path;
use std::time;

pub type CacheMap = collections::HashMap<CachePath, CacheEntry>;

#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct FileCache(CacheMap);

#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct CacheEntry {
    filestats: FileStats,
    hash: dag::ObjectKey,
}

/// Status used to detect file changes
#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct FileStats {
    size: dag::ObjectSize,
    mtime: CacheTime,
}

#[derive(Clone,Eq,PartialEq,Debug)]
pub struct CacheTime(time::SystemTime);

#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Hash,Debug)]
pub struct CachePath(path::PathBuf);

impl FileCache {
    pub fn new() -> Self {
        FileCache(CacheMap::new())
    }
}

impl Encodable for CacheTime {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        let since_epoch = self.0.duration_since(time::UNIX_EPOCH).unwrap();
        let secs_nanos = (since_epoch.as_secs(), since_epoch.subsec_nanos());
        secs_nanos.encode(s)
    }
}

impl Decodable for CacheTime {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let (secs, nanos) = try!(<(u64, u32)>::decode(d));
        Ok(CacheTime(time::UNIX_EPOCH + time::Duration::new(secs, nanos)))
    }
}

impl CachePath {
    pub fn from_str(s: &str) -> Self {
        CachePath(fsutil::new_path_buf(s))
    }
}

impl<'a> From<&'a str> for CachePath {
    fn from(s: &'a str) -> Self {
        Self::from_str(s)
    }
}

impl Encodable for CachePath {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        self.0.to_str().unwrap().encode(s)
    }
}

impl Decodable for CachePath {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        let s = try!(String::decode(d));
        Ok(CachePath::from_str(&s))
    }
}


#[cfg(test)]
mod test {
    use dag;
    use fsutil;
    use rustc_serialize::json;
    use std::path;
    use std::time;
    use super::*;

    #[test]
    fn test_serialize_cachetime() {
        let obj = CacheTime(time::UNIX_EPOCH + time::Duration::new(120, 55));
        let encoded = json::encode(&obj).unwrap();
        assert_eq!(encoded, "[120,55]");
        let decoded: CacheTime = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }

    /// PathBufs are serialized as byte arrays instead of strings. Booo.
    #[test]
    fn test_serialize_pathbuf() {
        let obj = fsutil::new_path_buf("hello");
        let encoded = json::encode(&obj).unwrap();
        assert_eq!(encoded, "[104,101,108,108,111]");
        let decoded: path::PathBuf = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }

    #[test]
    fn test_serialize_cachepath() {
        let obj = CachePath::from_str("hello/world");
        let encoded = json::encode(&obj).unwrap();
        assert_eq!(encoded, "\"hello/world\"");
        let decoded: CachePath = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }

    #[test]
    fn test_serialize_filecache() {
        let mut obj = FileCache::new();
        obj.0.insert(CachePath::from_str("patha/x"), CacheEntry{
            filestats: FileStats{
                mtime: CacheTime(
                           time::UNIX_EPOCH + time::Duration::new(120, 55)),
                size: 12345,
            },
            hash: dag::ObjectKey
                ::from_hex("d3486ae9136e7856bc42212385ea797094475802").unwrap(),
        });
        let encoded = json::encode(&obj).unwrap();
        let decoded: FileCache = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
