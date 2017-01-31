use cache::AllCaches;
use dag::ObjectKey;
use error::*;
use objectstore::ObjectStore;
use rollinghash;
use std::fs;
use std::io;
use std::path;
use std::path::PathBuf;
use walkdir;
use walkdir::WalkDirIterator;

pub fn dirs_depth_first
    (path: &path::Path)
     -> Box<Iterator<Item = walkdir::Result<walkdir::DirEntry>>> {

    Box::new(walkdir::WalkDir::new(path)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|d| d.file_type().is_dir()))
}

pub struct HashSetup {
    pub objectstore: ObjectStore,
    pub cache: AllCaches,
}

impl HashSetup {
    pub fn with_repo_path(repo_path: PathBuf) -> Result<Self> {
        let objectstore = try!(ObjectStore::open(repo_path));
        let cache = AllCaches::new();
        Ok(HashSetup {
            objectstore: objectstore,
            cache: cache,
        })
    }

    pub fn hash_file_no_cache(&mut self,
                              path: &path::Path)
                              -> Result<(ObjectKey, fs::Metadata)> {
        let file = try!(fs::File::open(path));
        let metadata = try!(file.metadata());

        let file = io::BufReader::new(file);

        let mut last_key = Err("No objects produced by file".into());

        for object in rollinghash::read_file_objects(file) {
            last_key = self.objectstore.store_object(&object?);
        }

        last_key.map(|k| (k, metadata))
    }

    /// Breaks a file into chunks and stores it
    ///
    /// Returns a tuple containing the hash of the file object (single blob or
    /// chunk index) and the metadata of the file. The metadata is returned so
    /// that it can be used for caching.
    pub fn hash_file(&mut self, file_path: &path::Path) -> Result<ObjectKey> {

        use cache::CacheStatus::*;
        if let Ok(Cached { hash }) = self.cache.check(&file_path) {
            return Ok(hash);
        }

        let (hash, metadata) = try!(self.hash_file_no_cache(&file_path));
        try!(self.cache.insert(
                file_path.to_owned(), metadata.into(), hash.clone()));

        Ok(hash)
    }
}

#[cfg(test)]
mod test {
    use dag;
    use dag::ObjectCommon;
    use dag::ReadObjectContent;
    use error::*;
    use rollinghash;
    use std::fs;
    use std::io;
    use super::*;
    use testutil;

    fn create_hash_setup() -> Result<(testutil::TempDir, HashSetup)> {
        let wd_temp = try!(testutil::in_mem_tempdir("test_directory"));
        let wd_path = wd_temp.path().to_path_buf();
        try!(fs::create_dir_all(&wd_path));
        let os_path = wd_path.join("object_store");
        let hs = try!(HashSetup::with_repo_path(os_path));

        Ok((wd_temp, hs))
    }

    #[test]
    fn test_hash_file_empty() {
        let (temp, mut hash_setup) = create_hash_setup().unwrap();
        let filepath = temp.path().join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = hash_setup.hash_file_no_cache(&filepath).unwrap().0;

        let obj = hash_setup.objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = dag::Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_hash_file_small() {
        let (temp, mut hash_setup) = create_hash_setup().unwrap();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = hash_setup.hash_file_no_cache(&filepath).unwrap().0;

        let obj = hash_setup.objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);

        let blob = dag::Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_hash_file_chunked() {
        let (temp, mut hash_setup) = create_hash_setup().unwrap();
        let filepath = temp.path().join("foo");
        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = hash_setup.hash_file_no_cache(&filepath).unwrap().0;

        let obj = hash_setup.objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);
        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, dag::ObjectType::ChunkedBlob);

        let chunked = dag::ChunkedBlob::read_content(&mut obj).unwrap();
        assert_eq!(chunked.total_size, filesize);
        assert_eq!(chunked.chunks.len(), 5);

        for chunkrecord in chunked.chunks {
            let obj = hash_setup.objectstore
                .open_object_file(&chunkrecord.hash)
                .unwrap();
            let mut obj = io::BufReader::new(obj);
            let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, dag::ObjectType::Blob);

            let blob = dag::Blob::read_content(&mut obj).unwrap();
            assert_eq!(blob.content_size(), chunkrecord.size);
        }
    }

    // #[test]
    // fn test_store_directory() {
    // let (temp, mut objectstore) = create_temp_repository().unwrap();
    // let mut rng = testutil::RandBytes::new();
    //
    // let wd_path = temp.path().join("dir_to_store");
    //
    // testutil::write_str_file(&wd_path.join("foo"), "foo").unwrap();
    // testutil::write_str_file(&wd_path.join("bar"), "bar").unwrap();
    //
    // let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;
    // rng.write_file(&wd_path.join("baz"), filesize).unwrap();
    //
    // let hash = objectstore.store_directory(&wd_path).unwrap();
    //
    // let obj = objectstore.open_object_file(&hash).unwrap();
    // let mut obj = io::BufReader::new(obj);
    // let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
    //
    // assert_eq!(header.object_type, dag::ObjectType::Tree);
    //
    // let tree = dag::Tree::read_content(&mut obj).unwrap();
    // assert_eq!(tree, dag::Tree::new());
    // assert_eq!(tree.len(), 3);
    //
    // TODO: nested directories
    // TODO: consistent sort order
    // }
    //

}
