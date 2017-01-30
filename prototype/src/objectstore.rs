use cache;
use constants;
use dag;
use error::*;
use fsutil;
use rollinghash;
use std::fs;
use std::io;
use std::path;

pub struct ObjectStore {
    path: path::PathBuf,
}

impl ObjectStore {
    pub fn init(path: path::PathBuf) -> Result<Self> {
        try!(fs::create_dir_all(&path));
        Self::open(path)
    }

    pub fn open(path: path::PathBuf) -> Result<Self> {
        Ok(ObjectStore { path: path })
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }

    fn object_path(&self, key: &dag::ObjectKey) -> path::PathBuf {
        let key = key.to_hex();
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }

    pub fn has_object(&self, key: &dag::ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    pub fn open_object_file(&self, key: &dag::ObjectKey) -> Result<fs::File> {
        fs::File::open(self.object_path(key)).err_into()
    }

    pub fn store_object(&mut self,
                        obj: &dag::ObjectCommon)
                        -> Result<dag::ObjectKey> {

        // Create temporary file
        let temp_path = self.path.join("tmp");
        try!(fsutil::create_parents(&temp_path));
        let mut file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));

        // Write object to temporary file
        let key = try!(obj.write_to(&mut file));

        // Move file to permanent path
        let permpath = self.object_path(&key);
        try!(fsutil::create_parents(&permpath));
        try!(fs::rename(&temp_path, &permpath));

        Ok(key)
    }

    pub fn store_file(&mut self, path: &path::Path) -> Result<dag::ObjectKey> {

        let file = try!(fs::File::open(path));
        let file = io::BufReader::new(file);

        let mut last_key = Ok(dag::ObjectKey::zero());

        for object in rollinghash::read_file_objects(file) {
            last_key = self.store_object(&object?);
        }

        last_key
    }

    pub fn store_file_with_caching(&mut self,
                                   file_path: &path::Path)
                                   -> Result<dag::ObjectKey> {

        let (cache_status, mut cache, basename, file_stats) =
            cache::HashCacheFile::open_and_check_file(file_path)
                .expect("could not check file cache status");

        if let cache::CacheStatus::Cached { hash } = cache_status {
            return Ok(hash);
        }

        let result = self.store_file(file_path);

        if let Ok(key) = result {
            cache.insert_entry(basename.into(), file_stats, key.clone());
        }

        result
    }

    pub fn store_directory<P: AsRef<path::Path>>(&mut self,
                                                 dir_path: &P)
                                                 -> Result<dag::ObjectKey> {

        let dir_path = dir_path.as_ref();
        let mut tree = dag::Tree::new();
        for entry in try!(fs::read_dir(dir_path)) {
            let subpath = entry?.path();

            // TODO: ignore list
            if subpath.ends_with(constants::HIDDEN_DIR_NAME) ||
               subpath.ends_with(constants::CACHE_FILE_NAME) {
                continue;
            }

            let key = if subpath.is_file() {
                try!(self.store_file_with_caching(&subpath))
            } else if subpath.is_dir() {
                unimplemented!()
            } else {
                unimplemented!()
            };

            let name = try!(subpath.strip_prefix(&dir_path));
            tree.insert(name.to_owned(), key);
        }
        self.store_object(&tree).err_into()
    }
}

#[cfg(test)]
pub mod test {
    use dag;
    use dag::ObjectCommon;
    use dag::ReadObjectContent;
    use error::*;
    use rollinghash;
    use std::fs;
    use std::io;
    use super::*;
    use testutil;

    fn create_temp_repository() -> Result<(testutil::TempDir, ObjectStore)> {
        let wd_temp = try!(testutil::in_mem_tempdir("test_directory"));
        let wd_path = wd_temp.path().to_path_buf();
        try!(fs::create_dir_all(&wd_path));
        let os_path = wd_path.join("object_store");
        let os = try!(ObjectStore::init(os_path));

        Ok((wd_temp, os))
    }

    #[test]
    fn test_store_and_retrieve() {
        let obj = dag::Object::blob_from_vec("Hello!".as_bytes().to_owned());
        let key = obj.calculate_hash();

        let (_tempdir, mut store) = create_temp_repository().unwrap();

        assert!(!store.has_object(&key),
                "Store should not have key at first");

        let stored_key = store.store_object(&obj).unwrap();
        assert_eq!(stored_key,
                   key,
                   "Key when stored should be the same as given by \
                    calculate_hash");
        assert!(store.has_object(&stored_key),
                "Store should report that key is
        present");

        let reader = store.open_object_file(&stored_key).unwrap();
        let retrieved = dag::Object::read_from(&mut io::BufReader::new(reader))
            .unwrap();
        assert_eq!(retrieved,
                   obj,
                   "Retrieved object should be the same as
        stored \
                    object");
    }

    #[test]
    fn test_store_file_empty() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = objectstore.store_file(&filepath).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = dag::Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_store_file_small() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = objectstore.store_file(&filepath).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);

        let blob = dag::Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_store_file_chunked() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");
        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = objectstore.store_file(&filepath).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);
        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, dag::ObjectType::ChunkedBlob);

        let chunked = dag::ChunkedBlob::read_content(&mut obj).unwrap();
        assert_eq!(chunked.total_size, filesize);
        assert_eq!(chunked.chunks.len(), 5);

        for chunkrecord in chunked.chunks {
            let obj = objectstore.open_object_file(&chunkrecord.hash).unwrap();
            let mut obj = io::BufReader::new(obj);
            let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, dag::ObjectType::Blob);

            let blob = dag::Blob::read_content(&mut obj).unwrap();
            assert_eq!(blob.content_size(), chunkrecord.size);
        }
    }

    #[test]
    fn test_store_directory() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let mut rng = testutil::RandBytes::new();

        let wd_path = temp.path().join("dir_to_store");

        testutil::write_str_file(&wd_path.join("foo"), "foo").unwrap();
        testutil::write_str_file(&wd_path.join("bar"), "bar").unwrap();

        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;
        rng.write_file(&wd_path.join("baz"), filesize).unwrap();

        let hash = objectstore.store_directory(&wd_path).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);
        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, dag::ObjectType::Tree);

        let tree = dag::Tree::read_content(&mut obj).unwrap();
        // assert_eq!(tree, dag::Tree::new());
        assert_eq!(tree.len(), 3);

        // TODO: nested directories
        // TODO: consistent sort order
    }

}
