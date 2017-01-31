use dag;
use error::*;
use fsutil;
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

    pub fn path(&self) -> &path::Path { &self.path }

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

    /// Writes a single object into the object store
    ///
    /// Returns the hash key of the object
    pub fn store_object(&mut self,
                        obj: &dag::ObjectCommon)
                        -> Result<dag::ObjectKey> {

        // If object already exists, no need to store
        let key = obj.calculate_hash();
        if self.has_object(&key) {
            return Ok(key);
        }

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
}

#[cfg(test)]
pub mod test {
    use dag;
    use dag::AsHashed;
    use error::*;
    use std::fs;
    use std::io;
    use super::*;
    use testutil;

    pub fn create_temp_repository
        ()
        -> Result<(testutil::TempDir, ObjectStore)>
    {
        let wd_temp = try!(testutil::in_mem_tempdir("test_directory"));
        let wd_path = wd_temp.path().to_path_buf();
        try!(fs::create_dir_all(&wd_path));
        let os_path = wd_path.join("object_store");
        let os = try!(ObjectStore::init(os_path));

        Ok((wd_temp, os))
    }

    #[test]
    fn test_store_and_retrieve() {
        let obj = dag::Blob::from("Hello!").as_hashed();

        let (_tempdir, mut store) = create_temp_repository().unwrap();

        assert!(!store.has_object(obj.hash()),
                "Store should not have key at first");

        let stored_key = store.store_object(&obj).unwrap();
        assert_eq!(stored_key,
                   *obj.hash(),
                   "Key when stored should be the same as given by \
                    calculate_hash");
        assert!(store.has_object(&stored_key),
                "Store should report that key is present");

        let reader = store.open_object_file(&stored_key).unwrap();
        let retrieved = dag::Object::read_from(&mut io::BufReader::new(reader))
            .unwrap();
        assert_eq!(retrieved,
                   *obj,
                   "Retrieved object should be the same as stored object");
    }

}
