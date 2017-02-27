use constants;
use dag;
use disk_backed::DiskBacked;
use encodable;
use error::*;
use rustc_serialize;
use std::collections;
use std::fs;
use std::hash::Hash;
use std::hash::Hasher;
use std::io;
use std::path;


/// Status of a file's cached hash
#[derive(Clone,Eq,PartialEq,Debug)]
pub enum CacheStatus {
    /// File's hash is not cached
    NotCached { size: dag::ObjectSize },
    /// File's hash is cached, but it has been modified since
    Modified { size: dag::ObjectSize },
    /// File's hash is cached
    Cached { hash: dag::ObjectKey },
}

/// Does an early return with the cached hash value if it is present
///
/// Like a `try!` for caching.
#[macro_export]
macro_rules! return_if_cached {
    (do_check; $path:expr, $cache_check:expr) => {
        if let Ok($crate::cache::CacheStatus::Cached{ hash }) = $cache_check {
                debug!("Already hashed: {} {}", hash, $path.display());
                return Ok(hash);
        }
    };
    ($cache:expr, $path:expr) => {
        return_if_cached!{do_check; $path, $cache.check($path)};
    };
    ($cache:expr, $path:expr, $metadata:expr) => {
        return_if_cached!{do_check; $path, $cache.check_with($path, $metadata)};
    };
}

/// Does an early return if the cached value matches
///
/// Like a `try!` for caching.
#[macro_export]
macro_rules! return_if_cache_matches {
    (do_check; $path:expr, $hash:expr, $cache_check:expr) => {
        if $path.exists() {
            match $cache_check {
                Ok($crate::cache::CacheStatus::Cached { hash: ref cache_hash })
                    if cache_hash == $hash => {
                        debug!("Already at state: {} {}",
                                cache_hash, $path.display());
                        return Ok(());
                }
                _ => {}
            }
        }
    };
    ($cache:expr, $path:expr, $hash:expr) => {
        return_if_cache_matches!{do_check; $path, $hash,
                                    $cache.check($path)};
    };
    ($cache:expr, $path:expr, $metadata:expr, $hash:expr) => {
        return_if_cache_matches!{do_check; $path, $hash,
                                    $cache.check_with($path, $metadata)};
    };
}

type CacheMap = collections::HashMap<encodable::PathBuf, CacheEntry>;

wrapper_struct!{
/// A cache of known file hashes
#[derive(Clone,Eq,PartialEq,Debug,Default)]
pub struct HashCache(CacheMap);
}

/// Data stored in the cache for each file
#[derive(Clone,Hash,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct CacheEntry {
    pub filestats: FileStats,
    pub hash: dag::ObjectKey,
}

/// Subset of file metadata used to determine if file has been modified
#[derive(Clone,Hash,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct FileStats {
    size: dag::ObjectSize,
    mtime: encodable::SystemTime,
}

/// Cache of caches
pub struct AllCaches {
    // TODO: Use an actual cache that can purge entries
    directory_caches: collections::HashMap<path::PathBuf,
                                           DiskBacked<HashCache>>,
}

// HashCache

impl HashCache {
    pub fn new() -> Self { HashCache(CacheMap::new()) }

    pub fn insert_entry(&mut self,
                        file_path: path::PathBuf,
                        file_stats: FileStats,
                        hash: dag::ObjectKey) {

        debug!("Caching file hash: {} => {}", file_path.display(), hash);
        self.0.insert(file_path.into(),
                      CacheEntry {
                          filestats: file_stats,
                          hash: hash,
                      });
    }

    pub fn get<'a, P: ?Sized + AsRef<path::Path>>(&self,
                                                  file_path: &'a P)
                                                  -> Option<&CacheEntry> {
        self.0.get(&file_path.into())
    }

    pub fn check<'a, P: ?Sized + AsRef<path::Path>>(&self,
                                                    file_path: &'a P,
                                                    file_stats: &FileStats)
                                                    -> CacheStatus {
        match self.0.get(&file_path.into()) {
            Some(cache_entry) => {
                if cache_entry.filestats == *file_stats {
                    CacheStatus::Cached { hash: cache_entry.hash }
                } else {
                    CacheStatus::Modified { size: file_stats.size }
                }
            }
            None => CacheStatus::NotCached { size: file_stats.size },
        }
    }
}

impl rustc_serialize::Encodable for HashCache {
    fn encode<S: rustc_serialize::Encoder>
        (&self,
         s: &mut S)
         -> ::std::result::Result<(), S::Error> {
        rustc_serialize::Encodable::encode(&self.0, s)
    }
}

impl rustc_serialize::Decodable for HashCache {
    fn decode<D: rustc_serialize::Decoder>
        (d: &mut D)
         -> ::std::result::Result<Self, D::Error> {
        let cache_map =
            try!(<CacheMap as rustc_serialize::Decodable>::decode(d));
        Ok(HashCache(cache_map))
    }
}

impl Hash for HashCache {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for entry in &self.0 {
            entry.hash(state);
        }
    }
}

// FileStats

impl FileStats {
    pub fn read(file_path: &path::Path) -> io::Result<Self> {
        fs::metadata(file_path).map(|x| x.into())
    }
}

impl From<fs::Metadata> for FileStats {
    fn from(metadata: fs::Metadata) -> FileStats {
        FileStats {
            size: metadata.len(),
            mtime: metadata.modified()
                .expect("system has no mod time in file stats")
                .into(),
        }
    }
}

// AllCaches

impl AllCaches {
    pub fn new() -> Self {
        AllCaches { directory_caches: collections::HashMap::new() }
    }

    fn cache_for_dir(&mut self,
                     dir_path: &path::Path)
                     -> Result<&mut DiskBacked<HashCache>> {
        if self.directory_caches.get(dir_path).is_none() {
            let cache_path = dir_path.join(constants::CACHE_FILE_NAME);
            let cache_file = DiskBacked::read_or_default("cache", cache_path)?;
            self.directory_caches.insert(dir_path.into(), cache_file);
        }
        Ok(self.directory_caches.get_mut(dir_path).expect("just inserted"))
    }

    pub fn check(&mut self, file_path: &path::Path) -> Result<CacheStatus> {
        let metadata = try!(file_path.metadata());
        self.check_with(file_path, &metadata.into())
    }

    pub fn check_with(&mut self,
                      file_path: &path::Path,
                      stats: &FileStats)
                      -> Result<CacheStatus> {

        let dir_path = try!(file_path.parent_or_err());
        let dir_cache = try!(self.cache_for_dir(dir_path));

        let file_name = try!(file_path.file_name_or_err());
        Ok(dir_cache.check(file_name, stats))
    }

    pub fn insert(&mut self,
                  file_path: path::PathBuf,
                  stats: FileStats,
                  hash: dag::ObjectKey)
                  -> Result<()> {

        let dir_path = try!(file_path.parent_or_err());
        let dir_cache = try!(self.cache_for_dir(dir_path));

        let file_name = try!(file_path.file_name_or_err());
        Ok(dir_cache.insert_entry(file_name.into(), stats, hash))
    }

    pub fn flush(&mut self) { self.directory_caches.clear() }
}

#[cfg(test)]
mod test {
    use dag::ObjectKey;
    use encodable;
    use rustc_serialize::json;
    use super::*;

    #[test]
    fn test_serialize_filecache() {
        let mut obj = HashCache::new();
        obj.insert(encodable::PathBuf::from("patha/x"), CacheEntry{
            filestats: FileStats{
                mtime: encodable::SystemTime::unix_epoch_plus(120, 55),
                size: 12345,
            },
            hash: ObjectKey::from("d3486ae9136e7856bc42212385ea797094475802"),
        });
        let encoded = json::encode(&obj).unwrap();
        let decoded: HashCache = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
