use cache::AllCaches;
use dag::ObjectKey;
use error::*;
use objectstore::ObjectStore;
use rollinghash::read_file_objects;
use std::fs::File;
use std::io::BufReader;
use std::path;
use walkdir;
use walkdir::DirEntry;
use walkdir::WalkDir;
use walkdir::WalkDirIterator;

pub fn dirs_depth_first(path: &path::Path)
                        -> Box<Iterator<Item = walkdir::Result<DirEntry>>> {

    Box::new(WalkDir::new(path)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|d| d.file_type().is_dir()))
}

pub fn hash_file(file_path: path::PathBuf,
                 cache: &mut AllCaches,
                 object_store: &mut ObjectStore)
                 -> Result<ObjectKey> {

    let file = try!(File::open(&file_path));
    let metadata = try!(file.metadata());

    use cache::CacheStatus::*;
    if let Ok(Cached { hash }) = cache.check(&file_path) {
        return Ok(hash);
    }

    let file = BufReader::new(file);

    let mut last_hash = ObjectKey::zero();

    for object in read_file_objects(file) {
        last_hash = try!(object_store.store_object(&object?));
    }

    try!(cache.insert(file_path, metadata.into(), last_hash.clone()));

    Ok(last_hash)
}

#[cfg(test)]
mod test {
    use cache::AllCaches;
    use dag::Blob;
    use dag::ChunkedBlob;
    use dag::ObjectCommon;
    use dag::ObjectHeader;
    use dag::ObjectType;
    use dag::ReadObjectContent;
    use objectstore::test::create_temp_repository;
    use rollinghash::CHUNK_TARGET_SIZE;
    use std::io::BufReader;
    use super::*;
    use testutil;

    #[test]
    fn test_hash_file_empty() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = hash_file(filepath, &mut cache, &mut objectstore).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = BufReader::new(obj);

        let header = ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_hash_file_small() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = hash_file(filepath, &mut cache, &mut objectstore).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = BufReader::new(obj);

        let header = ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, ObjectType::Blob);

        let blob = Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_hash_file_chunked() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");
        let filesize = 3 * CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = hash_file(filepath, &mut cache, &mut objectstore).unwrap();

        let obj = objectstore.open_object_file(&hash).unwrap();
        let mut obj = BufReader::new(obj);
        let header = ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, ObjectType::ChunkedBlob);

        let chunked = ChunkedBlob::read_content(&mut obj).unwrap();
        assert_eq!(chunked.total_size, filesize);
        assert_eq!(chunked.chunks.len(), 5);

        for chunkrecord in chunked.chunks {
            let obj = objectstore.open_object_file(&chunkrecord.hash).unwrap();
            let mut obj = BufReader::new(obj);
            let header = ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, ObjectType::Blob);

            let blob = Blob::read_content(&mut obj).unwrap();
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
    // let filesize = 3 * CHUNK_TARGET_SIZE as u64;
    // rng.write_file(&wd_path.join("baz"), filesize).unwrap();
    //
    // let hash = objectstore.store_directory(&wd_path).unwrap();
    //
    // let obj = objectstore.open_object_file(&hash).unwrap();
    // let mut obj = BufReader::new(obj);
    // let header = ObjectHeader::read_from(&mut obj).unwrap();
    //
    // assert_eq!(header.object_type, ObjectType::Tree);
    //
    // let tree = Tree::read_content(&mut obj).unwrap();
    // assert_eq!(tree, Tree::new());
    // assert_eq!(tree.len(), 3);
    //
    // TODO: nested directories
    // TODO: consistent sort order
    // }
    //

}
