use constants::CACHE_FILE_NAME;
use dag;
use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use rustc_serialize::json;
use std::collections;
use std::convert;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::ops;
use std::path;
use std::time;

#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct HashCache(CacheMap);

pub type CacheMap = collections::HashMap<CachePath, CacheEntry>;

#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct CacheEntry {
    pub filestats: FileStats,
    pub hash: dag::ObjectKey,
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


impl HashCache {
    pub fn new() -> Self {
        HashCache(CacheMap::new())
    }

    pub fn save_in_dir(&self, dir_path: &path::Path) -> io::Result<()> {
        let encoded = json::encode(self).unwrap();

        let cache_file_path = HashCache::cache_file_path(dir_path);
        let mut cache_file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(cache_file_path.clone())
            .map_err(|e| {
                io::Error::new(e.kind(),
                               format!("{}", &cache_file_path.display()))
            }));

        cache_file.write_all(encoded.as_bytes())
    }

    pub fn load_in_dir(dir_path: &path::Path) -> io::Result<Self> {
        let cache_file_path = HashCache::cache_file_path(dir_path);

        if !cache_file_path.exists() {
            return Ok(HashCache::new());
        }

        let mut cache_file = fs::File::open(cache_file_path).unwrap();

        let mut json_str = String::new();
        cache_file.read_to_string(&mut json_str).unwrap();
        let decoded: HashCache = json::decode(&json_str).unwrap();
        Ok(decoded)
    }

    fn cache_file_path(dir_path: &path::Path) -> path::PathBuf {
        dir_path.join(CACHE_FILE_NAME)
    }

    pub fn insert<P: Into<CachePath>>(&mut self,
                                      file_path: P,
                                      file_stats: FileStats,
                                      hash: dag::ObjectKey) {
        self.0.insert(file_path.into(),
                      CacheEntry {
                          filestats: file_stats,
                          hash: hash,
                      });
    }
}

impl ops::Deref for HashCache {
    type Target = CacheMap;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl convert::Into<CacheMap> for HashCache {
    fn into(self) -> CacheMap {
        self.0
    }
}

impl convert::AsRef<CacheMap> for HashCache {
    fn as_ref(&self) -> &CacheMap {
        &self.0
    }
}

impl convert::AsMut<CacheMap> for HashCache {
    fn as_mut(&mut self) -> &mut CacheMap {
        &mut self.0
    }
}


impl FileStats {
    pub fn read(file_path: &path::Path) -> io::Result<Self> {
        fs::metadata(file_path).map(|x| x.into())
    }
}

impl From<fs::Metadata> for FileStats {
    fn from(metadata: fs::Metadata) -> FileStats {
        FileStats {
            size: metadata.len(),
            mtime: CacheTime(metadata.modified().unwrap()),
        }
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
        CachePath(path::PathBuf::from(s))
    }
}

impl<P: Into<path::PathBuf>> From<P> for CachePath {
    fn from(p: P) -> Self {
        CachePath(p.into())
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
    use rustc_serialize::json;
    use std::path;
    use std::time;
    use super::*;
    use testutil;

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
        let obj = path::PathBuf::from("hello");
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
        let mut obj = HashCache::new();
        obj.as_mut().insert(CachePath::from_str("patha/x"), CacheEntry{
            filestats: FileStats{
                mtime: CacheTime(
                           time::UNIX_EPOCH + time::Duration::new(120, 55)),
                size: 12345,
            },
            hash: dag::ObjectKey
                ::from_hex("d3486ae9136e7856bc42212385ea797094475802").unwrap(),
        });
        let encoded = json::encode(&obj).unwrap();
        let decoded: HashCache = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }

    #[test]
    fn test_save_load() {
        let mut obj = HashCache::new();
        obj.as_mut().insert(CachePath::from_str("patha/x"), CacheEntry{
            filestats: FileStats{
                mtime: CacheTime(
                           time::UNIX_EPOCH + time::Duration::new(120, 55)),
                size: 12345,
            },
            hash: dag::ObjectKey
                ::from_hex("d3486ae9136e7856bc42212385ea797094475802").unwrap(),
        });

        let tempdir = testutil::in_mem_tempdir("cache_test").unwrap();
        obj.save_in_dir(tempdir.path()).unwrap();
        assert!(tempdir.path().join(".prototype_cache").exists());

        let decoded = HashCache::load_in_dir(tempdir.path()).unwrap();
        assert_eq!(decoded, obj);
    }

    #[test]
    fn test_load_nonexistent_as_empty() {
        let empty = HashCache::new();

        let tempdir = testutil::in_mem_tempdir("cache_test").unwrap();
        let decoded = HashCache::load_in_dir(tempdir.path()).unwrap();
        assert_eq!(decoded, empty);
    }
}
