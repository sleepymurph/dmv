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

pub fn hash_partial_tree(dir_path: &Path,
                         mut partial: PartialTree,
                         cache: &mut AllCaches,
                         object_store: &mut ObjectStore)
                         -> Result<ObjectKey> {
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
    use dag::HashedOrNot;
    use dag::Object;
    use dag::ObjectCommon;
    use dag::ObjectType;
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

        let mut objfile = object_store.open_object_file(&hash).unwrap();
        let obj = Object::read_from(&mut objfile).unwrap();

        assert_eq!(Object::Blob(Blob::empty()), obj);
    }

    #[test]
    fn test_hash_file_small() {
        let (temp, mut object_store) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = hash_file(filepath, &mut cache, &mut object_store).unwrap();

        let mut objfile = object_store.open_object_file(&hash).unwrap();
        let obj = Object::read_from(&mut objfile).unwrap();

        assert_eq!(Object::Blob(Blob::from("foo")), obj);
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
        let obj = Object::read_from(&mut obj).unwrap();

        if let Object::ChunkedBlob(chunked) = obj {
            assert_eq!(chunked.total_size, filesize);
            assert_eq!(chunked.chunks.len(), 5);

            for chunkrecord in chunked.chunks {
                let mut obj = object_store.open_object_file(&chunkrecord.hash)
                    .unwrap();
                let obj = Object::read_from(&mut obj).unwrap();
                assert_eq!(obj.object_type(), ObjectType::Blob);
                assert_eq!(obj.content_size(), chunkrecord.size);
            }

        } else {
            panic!("Not a ChunkedBlob: {:?}", obj);
        }

    }

    #[test]
    fn test_store_directory_shallow() {
        let (temp, mut object_store) = create_temp_repository().unwrap();
        let mut cache = AllCaches::new();
        let wd_path = temp.path().join("work_dir");

        write_str_files!{
            wd_path;
            "foo" => "123",
            "bar" => "1234",
            "baz" => "12345",
        };

        let expected_partial = partial_tree!{
                "foo" => HashedOrNot::UnhashedFile(3),
                "bar" => HashedOrNot::UnhashedFile(4),
                "baz" => HashedOrNot::UnhashedFile(5),
        };

        let expected_tree = tree_object!{
            "foo" => Blob::from("123").calculate_hash(),
            "bar" => Blob::from("1234").calculate_hash(),
            "baz" => Blob::from("12345").calculate_hash(),
        };

        // Build partial tree

        let partial = dir_to_partial_tree(&wd_path, &mut cache).unwrap();
        assert_eq!(partial, expected_partial);

        // Hash and store files

        let hash =
            hash_partial_tree(&wd_path, partial, &mut cache, &mut object_store)
                .unwrap();

        let mut file = object_store.open_object_file(&hash).unwrap();
        let tree = Object::read_from(&mut file).unwrap();

        assert_eq!(tree, Object::Tree(expected_tree.clone()));

        // Check that files are stored as blobs

        for (name, hash) in expected_tree.iter() {
            assert!(object_store.has_object(&hash),
                    "Object for '{}' was not stored",
                    name.display());
        }

        // Check again that files are cached now

        let partial = dir_to_partial_tree(&wd_path, &mut cache).unwrap();
        assert!(partial.is_complete());
        assert_eq!(partial.tree(), &expected_tree);
    }

}
