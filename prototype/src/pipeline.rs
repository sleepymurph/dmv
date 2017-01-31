use cache::AllCaches;
use dag::ObjectKey;
use error::*;
use objectstore::ObjectStore;
use rollinghash;
use std::fs;
use std::io;
use std::path;
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

/// Breaks a file into chunks and stores it
///
/// Returns a tuple containing the hash of the file object (single blob or
/// chunk index) and the metadata of the file. The metadata is returned so
/// that it can be used for caching.
pub fn hash_file(object_store: &mut ObjectStore,
                 path: &path::Path)
                 -> Result<(ObjectKey, fs::Metadata)> {
    let file = try!(fs::File::open(path));
    let metadata = try!(file.metadata());

    let file = io::BufReader::new(file);

    let mut last_key = Err("No objects produced by file".into());

    for object in rollinghash::read_file_objects(file) {
        last_key = object_store.store_object(&object?);
    }

    last_key.map(|k| (k, metadata))
}

pub fn hash_file_with_cache(object_store: &mut ObjectStore,
                            cache: &mut AllCaches,
                            file_path: &path::Path)
                            -> Result<ObjectKey> {

    use cache::CacheStatus::*;
    if let Ok(Cached { hash }) = cache.check(&file_path) {
        return Ok(hash);
    }

    let (hash, metadata) = try!(hash_file(object_store, &file_path));
    try!(cache.insert(file_path.to_owned(), metadata.into(), hash.clone()));

    Ok(hash)
}

#[cfg(test)]
mod test {
    use dag;
    use dag::ObjectCommon;
    use dag::ReadObjectContent;
    use objectstore::test::create_temp_repository;
    use rollinghash;
    use std::io;
    use super::*;
    use testutil;

    #[test]
    fn test_hash_file_empty() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = hash_file(&mut objectstore, &filepath).unwrap().0;

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = dag::Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_hash_file_small() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = hash_file(&mut objectstore, &filepath).unwrap().0;

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);

        let blob = dag::Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_hash_file_chunked() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");
        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = hash_file(&mut objectstore, &filepath).unwrap().0;

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
