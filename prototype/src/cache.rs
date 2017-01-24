use constants;
use dag;
use encodable;
use error::*;
use rustc_serialize;
use rustc_serialize::json;
use std::collections;
use std::ffi;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::ops;
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

type CacheMap = collections::HashMap<encodable::PathBuf, CacheEntry>;

wrapper_struct!{
/// A cache of known file hashes
#[derive(Clone,Eq,PartialEq,Debug)]
pub struct HashCache(CacheMap);
}

/// Data stored in the cache for each file
#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct CacheEntry {
    pub filestats: FileStats,
    pub hash: dag::ObjectKey,
}

/// Subset of file metadata used to determine if file has been modified
#[derive(Clone,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct FileStats {
    size: dag::ObjectSize,
    mtime: encodable::SystemTime,
}


/// A file-backed cache that saves updates on drop
pub struct HashCacheFile {
    /// Path to the file that stores the cache
    cache_file_path: path::PathBuf,
    /// Open File object that stores the cache
    cache_file: fs::File,
    /// The cache map itself
    cache: HashCache,
}

// HashCache

impl HashCache {
    pub fn new() -> Self {
        HashCache(CacheMap::new())
    }

    pub fn insert_entry(&mut self,
                        file_path: path::PathBuf,
                        file_stats: FileStats,
                        hash: dag::ObjectKey) {
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

// HashCacheFile

impl_deref!(HashCacheFile => HashCache, cache);

impl HashCacheFile {
    /// Create/open a cache file at a specific location
    pub fn open(cache_file_path: path::PathBuf) -> Result<Self> {
        let cache_file_exists = cache_file_path.exists();

        let mut cache_file = try!(fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&cache_file_path));

        let cache_map = if cache_file_exists {
            let mut json_str = String::new();
            try!(cache_file.read_to_string(&mut json_str));
            try!(json::decode(&json_str).map_err(|e| {
                ErrorKind::CorruptCacheFile {
                    cache_file: cache_file_path.to_owned(),
                    cause: e,
                    bad_json: json_str,
                }
            }))
        } else {
            CacheMap::new()
        };

        Ok(HashCacheFile {
            cache_file_path: cache_file_path,
            cache_file: cache_file,
            cache: HashCache(cache_map),
        })
    }

    /// Create/open a cache file in the given directory
    ///
    /// The file will be named according to `constants::CACHE_FILE_NAME`.
    pub fn open_in_dir(dir_path: &path::Path) -> Result<Self> {
        Self::open(dir_path.join(constants::CACHE_FILE_NAME))
    }

    /// Create/open a cache file in the parent directory of the given file
    ///
    /// For this app, cache files for files in a directory are stored in that
    /// directory. This is a convenience method to find/create the cache file
    /// responsible for the given file.
    pub fn open_in_parent_dir(child_path: &path::Path) -> Result<Self> {
        let dir_path = try!(child_path.parent_or_err());
        Self::open_in_dir(dir_path)
    }

    /// Create/open the appropriate cache and check the given file's status
    ///
    /// Returns a tuple: (cache_status, cache, file_name, file_stats)
    ///
    /// - cache_status: the status of the given file
    /// - cache: the cache itself
    /// - file_name: the file_name of the file as looked up in the cache
    /// - file_stats: the file stats used to determine if it has been modified
    ///
    /// The extra values are returned so that you can update the cache
    ///
    /// ```
    /// extern crate prototypelib;
    ///
    /// use prototypelib::dag;
    /// use prototypelib::cache;
    /// use std::io;
    /// use std::path;
    ///
    /// pub fn store_file_with_caching(file_path: &path::Path)
    ///                                -> io::Result<dag::ObjectKey> {
    ///
    ///     let (cache_status, mut cache, basename, file_stats) =
    ///         cache::HashCacheFile::open_and_check_file(file_path)
    ///             .expect("could not check file cache status");
    ///
    ///     if let cache::CacheStatus::Cached { hash } = cache_status {
    ///         return Ok(hash);
    ///     }
    ///
    ///     let result = store_file(file_path);
    ///
    ///     if let Ok(key) = result {
    ///         cache.insert_entry(basename.into(), file_stats, key.clone());
    ///     }
    ///
    ///     result
    /// }
    ///
    /// # pub fn store_file(path: &path::Path) -> io::Result<dag::ObjectKey> {
    /// #   unimplemented!();
    /// # }
    /// # pub fn main() {}
    /// ```
    pub fn open_and_check_file
        (file_path: &path::Path)
         -> Result<(CacheStatus, Self, &ffi::OsStr, FileStats)> {

        let file_stats = try!(FileStats::read(file_path));
        let basename = try!(file_path.file_name_or_err());

        let file_cache = try!(HashCacheFile::open_in_parent_dir(file_path));
        let cache_status = file_cache.check(&basename, &file_stats);
        Ok((cache_status, file_cache, basename, file_stats))
    }

    pub fn flush(&mut self) -> Result<()> {
        use std::io::Seek;

        let encoded = try!(json::encode(&self.cache.0).map_err(|e| {
            ErrorKind::CacheSerializeError {
                cause: e,
                bad_cache: self.cache.clone(),
            }
        }));
        try!(self.cache_file.seek(io::SeekFrom::Start(0)));
        try!(self.cache_file.set_len(0));
        try!(self.cache_file.write_all(encoded.as_bytes()));
        Ok(())
    }
}

impl ops::Drop for HashCacheFile {
    fn drop(&mut self) {
        self.flush().expect("Could not flush hash file")
    }
}


#[cfg(test)]
mod test {
    use dag;
    use encodable;
    use rustc_serialize::json;
    use std::path;
    use super::*;
    use testutil;

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
    fn test_serialize_filecache() {
        let mut obj = HashCache::new();
        obj.insert(encodable::PathBuf::from("patha/x"), CacheEntry{
            filestats: FileStats{
                mtime: encodable::SystemTime::unix_epoch_plus(120, 55),
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
    fn test_hash_cache_file() {
        // Define some test values to use later
        let path0 = path::PathBuf::from("patha/x");
        let stats0 = FileStats {
            mtime: encodable::SystemTime::unix_epoch_plus(120, 55),
            size: 12345,
        };
        let hash0 =
            dag::ObjectKey::from_hex("d3486ae9136e7856bc42212385ea797094475802")
                .unwrap();

        let path1 = path::PathBuf::from("pathb/y");
        let stats1 = FileStats {
            mtime: encodable::SystemTime::unix_epoch_plus(60, 22),
            size: 54321,
        };
        let hash1 =
            dag::ObjectKey::from_hex("e030a4b3fdc15cdcbf9026d83b84c2b4b93309af")
                .unwrap();

        // Create temporary directory

        let tempdir = testutil::in_mem_tempdir("cache_test").unwrap();
        let cache_file_path = tempdir.path().join("cache");

        {
            // Open nonexistent cache file
            let mut cache_file = HashCacheFile::open(cache_file_path.clone())
                .expect("Open non-existent cache file");
            assert!(cache_file.is_empty(), "New cache should be empty");

            // Insert a value and let the destructor flush the file
            cache_file
                .insert_entry(path0.clone(), stats0.clone(), hash0.clone());
        }

        assert!(cache_file_path.is_file(), "New cache should be saved");

        {
            // Open the existing cache file
            let mut cache_file = HashCacheFile::open(cache_file_path.clone())
                .expect("Re-open cache file for firts time");
            assert!(!cache_file.is_empty(), "Read cache should not be empty");
            {
                let entry = cache_file.get(&path0).unwrap();

                assert_eq!(entry.filestats, stats0);
                assert_eq!(entry.hash, hash0);
            }

            // Insert another value and let the destructor flush the file
            cache_file
                .insert_entry(path1.clone(), stats1.clone(), hash1.clone());
        }

        {
            // Re-open the existing cache file
            let cache_file = HashCacheFile::open(cache_file_path.clone())
                .expect("Re-open cache file for second time");
            {
                let entry = cache_file.get(&path1).unwrap();

                assert_eq!(entry.filestats, stats1);
                assert_eq!(entry.hash, hash1);
            }
        }

    }
}
