use cache::AllCaches;
use cache::FileStats;
use constants;
use dag::ObjectKey;
use dag::PartialTree;
use dag::UnhashedPath;
use error::*;
use objectstore::ObjectStore;
use rollinghash::read_file_objects;
use std::fs::File;
use std::fs::read_dir;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use walkdir;
use walkdir::DirEntry;
use walkdir::WalkDir;
use walkdir::WalkDirIterator;

pub fn dirs_depth_first(path: &Path)
                        -> Box<Iterator<Item = walkdir::Result<DirEntry>>> {

    Box::new(WalkDir::new(path)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|d| d.file_type().is_dir()))
}

pub fn hash_file(file_path: PathBuf,
                 cache: &mut AllCaches,
                 object_store: &mut ObjectStore)
                 -> Result<ObjectKey> {

    let file = try!(File::open(&file_path));
    let file_stats = FileStats::from(file.metadata()?);
    let file = BufReader::new(file);

    return_if_cached!(cache.check_with(&file_path, &file_stats));

    let mut last_hash = ObjectKey::zero();
    for object in read_file_objects(file) {
        last_hash = try!(object_store.store_object(&object?));
    }

    try!(cache.insert(file_path, file_stats, last_hash.clone()));

    Ok(last_hash)
}

/// Read filesystem to construct a PartialTree
pub fn dir_to_partial_tree(dir_path: &Path,
                           cache: &mut AllCaches)
                           -> Result<PartialTree> {
    let ignored: Vec<PathBuf> = vec![constants::HIDDEN_DIR_NAME,
                                     constants::CACHE_FILE_NAME]
        .iter()
        .map(|x| PathBuf::from(x))
        .collect();

    if dir_path.is_dir() {
        let mut partial = PartialTree::new();

        for entry in try!(read_dir(dir_path)) {
            let entry = try!(entry);

            let ch_path = entry.path();
            let ch_name = PathBuf::from(ch_path.file_name_or_err()?);
            let ch_metadata = try!(entry.metadata());

            if ignored.contains(&ch_name) {
                continue;
            }

            if ch_metadata.is_file() {

                let cache_status =
                    try!(cache.check_with(&ch_path, &ch_metadata.into()));
                partial.insert(ch_name, cache_status);

            } else if ch_metadata.is_dir() {

                let subpartial = try!(dir_to_partial_tree(&ch_path, cache));
                partial.insert(ch_name, subpartial);

            } else {
                unimplemented!()
            }
        }

        Ok(partial)
    } else {
        bail!(ErrorKind::NotADirectory(dir_path.to_owned()))
    }
}

pub fn hash_partial_tree<P>(dir_path: &P,
                            mut partial: PartialTree,
                            cache: &mut AllCaches,
                            object_store: &mut ObjectStore)
                            -> Result<ObjectKey>
    where P: AsRef<Path>
{
    let dir_path = dir_path.as_ref();

    for (ch_name, unknown) in partial.unhashed().clone() {
        let ch_path = dir_path.join(&ch_name);

        let hash = match unknown {
            UnhashedPath::File(_) => hash_file(ch_path, cache, object_store),
            UnhashedPath::Dir(partial) => {
                hash_partial_tree(&ch_path, partial, cache, object_store)
            }
        };
        partial.insert(ch_name, hash?);
    }

    assert!(partial.is_complete());
    object_store.store_object(partial.tree())
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
    use super::*;
    use testutil;

    #[test]
    fn test_hash_file_empty() {
        let (temp, mut object_store) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = hash_file(filepath, &mut cache, &mut object_store).unwrap();

        let mut obj = object_store.open_object_file(&hash).unwrap();

        let header = ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_hash_file_small() {
        let (temp, mut object_store) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = hash_file(filepath, &mut cache, &mut object_store).unwrap();

        let mut obj = object_store.open_object_file(&hash).unwrap();

        let header = ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, ObjectType::Blob);

        let blob = Blob::read_content(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_hash_file_chunked() {
        let (temp, mut object_store) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");
        let filesize = 3 * CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = hash_file(filepath, &mut cache, &mut object_store).unwrap();

        let mut obj = object_store.open_object_file(&hash).unwrap();
        let header = ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, ObjectType::ChunkedBlob);

        let chunked = ChunkedBlob::read_content(&mut obj).unwrap();
        assert_eq!(chunked.total_size, filesize);
        assert_eq!(chunked.chunks.len(), 5);

        for chunkrecord in chunked.chunks {
            let mut obj = object_store.open_object_file(&chunkrecord.hash)
                .unwrap();
            let header = ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, ObjectType::Blob);

            let blob = Blob::read_content(&mut obj).unwrap();
            assert_eq!(blob.content_size(), chunkrecord.size);
        }
    }

    // #[test]
    // fn test_store_directory() {
    // let (temp, mut object_store) = create_temp_repository().unwrap();
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
    // let hash = object_store.store_directory(&wd_path).unwrap();
    //
    // let mut obj = object_store.open_object_file(&hash).unwrap();
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
