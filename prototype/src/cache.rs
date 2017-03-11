use constants;
use dag::ObjectKey;
use dag::ObjectSize;
use disk_backed::DiskBacked;
use encodable;
use error::*;
use rustc_serialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::hash::Hash;
use std::hash::Hasher;
use std::path;


/// Status of a file's cached hash
#[derive(Clone,Eq,PartialEq,Debug)]
pub enum CacheStatus {
    NotCached,
    Modified,
    Cached(ObjectKey),
}

impl fmt::Display for CacheStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &CacheStatus::Cached(ref hash) => write!(f, "{}", hash),
            &CacheStatus::Modified => write!(f, "modified"),
            &CacheStatus::NotCached => write!(f, "        "),
        }
    }
}



/// Data stored in the cache for each file
#[derive(Clone,Hash,Eq,PartialEq,Debug,RustcEncodable,RustcDecodable)]
pub struct CacheEntry {
    pub mtime: encodable::SystemTime,
    pub size: ObjectSize,
    pub hash: ObjectKey,
}

impl CacheEntry {
    fn new(meta: &fs::Metadata, hash: ObjectKey) -> Self {
        CacheEntry {
            mtime: meta.modified().expect("metadata has no mod time").into(),
            size: meta.len(),
            hash: hash,
        }
    }
    fn meta_match(&self, meta: &fs::Metadata) -> bool {
        self.size == meta.len() &&
        *self.mtime == meta.modified().expect("metadata has no mod time")
    }
    fn status(entry: Option<&CacheEntry>, meta: &fs::Metadata) -> CacheStatus {
        match entry {
            Some(ref entry) if entry.meta_match(meta) => {
                CacheStatus::Cached(entry.hash)
            }
            Some(_) => CacheStatus::Modified,
            None => CacheStatus::NotCached,
        }
    }
    fn check(entry: Option<&CacheEntry>,
             meta: &fs::Metadata)
             -> Option<ObjectKey> {
        match entry {
            Some(ref entry) if entry.meta_match(meta) => Some(entry.hash),
            _ => None,
        }
    }
}



wrapper_struct!{
    /// A cache of known file hashes
    #[derive(Clone,Eq,PartialEq,Debug,Default)]
    pub struct HashCache(HashMap<encodable::PathBuf, CacheEntry>);
}

impl HashCache {
    pub fn new() -> Self { HashCache(HashMap::new()) }

    pub fn insert_entry(&mut self,
                        file_path: path::PathBuf,
                        meta: &fs::Metadata,
                        hash: ObjectKey) {

        debug!("Caching file hash: {} => {}", file_path.display(), hash);
        self.0.insert(file_path.into(), CacheEntry::new(meta, hash));
    }

    pub fn get<'a, P: ?Sized + AsRef<path::Path>>(&self,
                                                  file_path: &'a P)
                                                  -> Option<&CacheEntry> {
        self.0.get(file_path.as_ref())
    }

    pub fn check<'a, P: ?Sized + AsRef<path::Path>>(&self,
                                                    file_path: &'a P,
                                                    meta: &fs::Metadata)
                                                    -> Option<ObjectKey> {
        CacheEntry::check(self.0.get(file_path.as_ref()), meta)
    }

    pub fn status<'a, P: ?Sized + AsRef<path::Path>>(&self,
                                                     file_path: &'a P,
                                                     meta: &fs::Metadata)
                                                     -> CacheStatus {
        CacheEntry::status(self.0.get(file_path.as_ref()), meta)
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
        let cache_map = <HashMap<_,_> as rustc_serialize::Decodable>::decode(d)?;
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



/// Cache of caches
pub struct AllCaches(// TODO: Use an actual cache that can purge entries
                     RefCell<HashMap<path::PathBuf, DiskBacked<HashCache>>>);

impl AllCaches {
    pub fn new() -> Self { AllCaches(RefCell::new(HashMap::new())) }

    fn read_dir_cache(&self, dir_path: &path::Path) -> Result<()> {
        if self.0.borrow().get(dir_path).is_none() {
            let cache_path = dir_path.join(constants::CACHE_FILE_NAME);
            let cache_file = DiskBacked::read_or_default("cache", cache_path)?;
            self.0.try_borrow_mut()?.insert(dir_path.into(), cache_file);
        }
        Ok(())
    }

    fn get(&self, file_path: &path::Path) -> Result<Option<CacheEntry>> {
        let dir_path = file_path.parent_or_err()?;
        let file_name = file_path.file_name_or_err()?;
        self.read_dir_cache(dir_path)?;
        let caches = self.0.try_borrow()?;
        let cache = caches.get(dir_path).expect("just read cache");
        Ok(cache.get(file_name).cloned())
    }

    pub fn status(&self,
                  file_path: &path::Path,
                  meta: &fs::Metadata)
                  -> Result<CacheStatus> {
        let entry = self.get(file_path)?;
        Ok(CacheEntry::status(entry.as_ref(), meta))
    }

    pub fn check(&self,
                 file_path: &path::Path,
                 meta: &fs::Metadata)
                 -> Result<Option<ObjectKey>> {
        let entry = self.get(file_path)?;
        Ok(CacheEntry::check(entry.as_ref(), meta))
    }

    pub fn insert(&mut self,
                  file_path: path::PathBuf,
                  meta: &fs::Metadata,
                  hash: ObjectKey)
                  -> Result<()> {

        let dir_path = file_path.parent_or_err()?;
        let file_name = file_path.file_name_or_err()?;
        self.read_dir_cache(dir_path)?;
        let mut caches = self.0.try_borrow_mut()?;
        let cache = caches.get_mut(dir_path).expect("just read cache");
        Ok(cache.insert_entry(file_name.into(), meta, hash))
    }

    pub fn flush(&mut self) { self.0.borrow_mut().clear() }
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
                mtime: encodable::SystemTime::unix_epoch_plus(120, 55),
                size: 12345,
            hash: ObjectKey::from("d3486ae9136e7856bc42212385ea797094475802"),
        });
        let encoded = json::encode(&obj).unwrap();
        let decoded: HashCache = json::decode(&encoded).unwrap();
        assert_eq!(decoded, obj);
    }
}
