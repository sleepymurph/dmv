//! Library for master's thesis prototype

#![allow(dead_code)]

pub mod rollinghash;
pub mod testutil;
pub mod dag;
pub mod objectstore;

pub mod fsutil {

    use std::fs;
    use std::io;
    use std::path;

    pub fn create_parents(path: &path::Path)
                          -> io::Result<Option<&path::Path>> {
        match path.parent() {
            Some(parent) => fs::create_dir_all(parent).and(Ok(Some(parent))),
            None => Ok(None),
        }
    }

}

pub mod repo {

    use std::fs;
    use std::io;
    use std::path;

    use dag;
    use objectstore;
    use rollinghash;

    pub struct WorkDir {
        path: path::PathBuf,
    }

    pub struct Repo {
        objectstore: objectstore::ObjectStore,
        workdir: Option<WorkDir>,
    }

    impl Repo {
        pub fn workdir_path(&self) -> Option<&path::Path> {
            self.workdir.as_ref().map(|wd| wd.path.as_ref())
        }

        pub fn workdir_join(&self, addition: &str) -> Option<path::PathBuf> {
            self.workdir_path().map(|p| p.join(addition))
        }

        pub fn store_object<O: dag::Object>(&mut self,
                                            obj: &O)
                                            -> io::Result<dag::ObjectKey> {
            let mut incoming = try!(self.objectstore.new_object());
            let key = try!(obj.write_to(&mut incoming));
            try!(self.objectstore.save_object(key, incoming));
            Ok(key)
        }

        pub fn store_file(&mut self,
                          path: &path::Path)
                          -> io::Result<dag::ObjectKey> {

            let file = try!(fs::File::open(path));
            let file = io::BufReader::new(file);

            let mut chunker = rollinghash::ChunkReader::wrap(file);
            let chunk1 = chunker.next();
            let chunk2 = chunker.next();

            match (chunk1, chunk2) {
                (None, None) => unimplemented!(),
                (Some(v1), None) => {
                    let blob = dag::Blob::from_vec(v1?);
                    self.store_object(&blob)
                }
                (Some(v1), Some(v2)) => {
                    let mut chunkedblob = dag::ChunkedBlob::new();

                    for chunk in vec![v1, v2].into_iter().chain(chunker) {
                        let blob = dag::Blob::from_vec(chunk?);
                        let key = try!(self.store_object(&blob));
                        chunkedblob.add_chunk(blob.size(), key);
                    }

                    self.store_object(&chunkedblob)
                }
                (None, Some(_)) => unreachable!(),
            }
        }
    }


    #[cfg(test)]
    mod test {
        extern crate tempdir;

        use std::fs;
        use std::io;

        use dag;
        use dag::Object;
        use objectstore;
        use rollinghash;
        use testutil;

        use super::*;

        fn create_temp_repository() -> io::Result<(tempdir::TempDir, Repo)> {
            let wd_temp = try!(tempdir::TempDir::new_in("/dev/shm",
                                                        "test_repository"));
            let wd_path = wd_temp.path().to_path_buf();
            try!(fs::create_dir_all(&wd_path));
            let wd = WorkDir { path: wd_path.clone() };

            let os_path = wd_path.join(".prototype");
            let os = objectstore::ObjectStore::new(&os_path);
            try!(os.init());

            let repo = Repo {
                objectstore: os,
                workdir: Some(wd),
            };

            Ok((wd_temp, repo))
        }

        #[test]
        fn test_store_file_small() {
            let (_temp, mut repo) = create_temp_repository().unwrap();
            let filepath = repo.workdir_join("foo").unwrap();

            testutil::write_str_file(&filepath, "foo").unwrap();

            let hash = repo.store_file(&filepath).unwrap();

            let obj = repo.objectstore.read_object(&hash).unwrap();
            let mut obj = io::BufReader::new(obj);

            let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, dag::ObjectType::Blob);

            let blob = dag::Blob::read_from(&mut obj).unwrap();
            assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
        }

        #[test]
        fn test_store_file_chunked() {
            let (_temp, mut repo) = create_temp_repository().unwrap();
            let filepath = repo.workdir_join("foo").unwrap();
            let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;

            let mut rng = testutil::RandBytes::new();
            rng.write_file(&filepath, filesize)
                .unwrap();

            let hash = repo.store_file(&filepath).unwrap();

            let obj = repo.objectstore.read_object(&hash).unwrap();
            let mut obj = io::BufReader::new(obj);
            let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

            assert_eq!(header.object_type, dag::ObjectType::ChunkedBlob);

            let chunked = dag::ChunkedBlob::read_from(&mut obj).unwrap();
            assert_eq!(chunked.total_size, filesize);
            assert_eq!(chunked.chunks.len(), 5);

            for chunkrecord in chunked.chunks {
                let obj =
                    repo.objectstore.read_object(&chunkrecord.hash).unwrap();
                let mut obj = io::BufReader::new(obj);
                let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
                assert_eq!(header.object_type, dag::ObjectType::Blob);

                let blob = dag::Blob::read_from(&mut obj).unwrap();
                assert_eq!(blob.size(), chunkrecord.size);
            }
        }
    }
}
