use dag::ObjectCommon;
use dag::ObjectHandle;
use dag::ObjectKey;
use error::*;
use fsutil;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

pub struct ObjectStore {
    path: PathBuf,
}

impl ObjectStore {
    pub fn init(path: PathBuf) -> Result<Self> {
        try!(fs::create_dir_all(&path));
        Self::open(path)
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        Ok(ObjectStore { path: path })
    }

    pub fn path(&self) -> &Path { &self.path }

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        let key = key.to_hex();
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }

    pub fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    pub fn open_object_file(&self,
                            key: &ObjectKey)
                            -> Result<io::BufReader<fs::File>> {

        if !self.has_object(&key) {
            bail!(ErrorKind::ObjectNotFound(key.to_owned()))
        }

        let file = try!(fs::File::open(self.object_path(key)).err_into());
        Ok(io::BufReader::new(file))
    }

    pub fn open_object(&self, key: &ObjectKey) -> Result<ObjectHandle> {
        let file = self.open_object_file(key)?;
        ObjectHandle::read_header(Box::new(file))
    }

    /// Writes a single object into the object store
    ///
    /// Returns the hash key of the object
    pub fn store_object(&mut self, obj: &ObjectCommon) -> Result<ObjectKey> {

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
    use dag::Blob;
    use dag::Object;
    use dag::ToHashed;
    use super::*;
    use testutil::tempdir::TempDir;

    pub fn create_temp_repository() -> Result<(TempDir, ObjectStore)> {
        let wd_temp = in_mem_tempdir!();
        let os_path = wd_temp.path().join("object_store");
        let os = try!(ObjectStore::init(os_path));

        Ok((wd_temp, os))
    }

    #[test]
    fn test_store_and_retrieve() {
        let obj = Blob::from("Hello!").to_hashed();

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

        let mut reader = store.open_object_file(&stored_key).unwrap();
        let retrieved = Object::read_from(&mut reader).unwrap();
        assert_eq!(retrieved,
                   *obj,
                   "Retrieved object should be the same as stored object");
    }

}
