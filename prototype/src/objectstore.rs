

use dag::ObjectKey;
use fsutil;
use std::fs;
use std::io;
use std::io::Write;
use std::path;

pub struct ObjectStore {
    path: path::PathBuf,
}

pub struct IncomingObject {
    temp_path: path::PathBuf,
    file: fs::File,
}

impl ObjectStore {
    pub fn new(path: &path::Path) -> Self {
        ObjectStore { path: path.to_owned() }
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }

    pub fn init(&self) -> io::Result<()> {
        fs::create_dir_all(&self.path)
    }

    fn object_path(&self, key: &ObjectKey) -> path::PathBuf {
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

    pub fn read_object(&self, key: &ObjectKey) -> io::Result<fs::File> {
        fs::File::open(self.object_path(key))
    }

    pub fn new_object(&mut self) -> io::Result<IncomingObject> {
        let temp_path = self.path.join("tmp");
        try!(fsutil::create_parents(&temp_path));
        let file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));
        Ok(IncomingObject {
            temp_path: temp_path,
            file: file,
        })
    }

    pub fn save_object(&mut self,
                       key: ObjectKey,
                       mut object: IncomingObject)
                       -> io::Result<()> {

        try!(object.flush());
        let permpath = self.object_path(&key);
        try!(fsutil::create_parents(&permpath));
        fs::rename(&object.temp_path, &permpath)
    }
}

impl io::Write for IncomingObject {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
pub mod test {
    extern crate tempdir;

    use dag::ObjectKey;
    use self::tempdir::TempDir;
    use std::io::Read;

    use std::io::Write;
    use super::*;

    pub fn create_temp_object_store() -> ObjectStore {
        let tmp = TempDir::new_in("/dev/shm", "object_store_test")
            .expect("create tempdir");
        let object_store = ObjectStore::new(tmp.path());
        object_store.init().expect("initialize object store");
        object_store
    }

    #[test]
    fn test_object_store() {
        let (key, data) =
            (ObjectKey::from_hex("69342c5c39e5ae5f0077aecc32c0f81811fb8193")
                .unwrap(),
             "Hello!".to_string());

        let mut store = create_temp_object_store();

        assert_eq!(store.has_object(&key), false);
        {
            let mut writer = store.new_object().expect("new incoming object");
            writer.write(data.as_bytes()).expect("write to incoming");
            store.save_object(key.clone(), writer).expect("store incoming");
        }
        {
            let mut reader = store.read_object(&key).expect("open object");
            let mut read_string = String::new();
            reader.read_to_string(&mut read_string).expect("read object");
            assert_eq!(read_string, data);
        }
    }
}
