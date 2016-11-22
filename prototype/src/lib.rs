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
    use std::io::Read;
    use std::path;

    use dag;
    use dag::Object;
    use objectstore;

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

        pub fn store_file(&mut self,
                          path: &path::Path)
                          -> io::Result<dag::ObjectKey> {

            let mut file = try!(fs::File::open(path));
            let mut contents:Vec<u8> = Vec::new();
            try!(file.read_to_end(&mut contents));

            let blob = dag::Blob::from_vec(contents);

            let mut incoming = try!(self.objectstore.new_object());
            let key = try!(blob.write_to(&mut incoming));
            try!(self.objectstore.save_object(key, incoming));
            Ok(key)
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

            let blob = repo.objectstore.read_object(&hash).unwrap();
            let mut blob = io::BufReader::new(blob);
            let header = dag::ObjectHeader::read_from(&mut blob).unwrap();

            assert_eq!(header.object_type, dag::ObjectType::Blob);

            let blob = dag::Blob::read_from(&mut blob).unwrap();
            assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
        }
    }
}
