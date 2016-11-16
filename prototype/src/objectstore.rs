use std::io::{Read, Write, Result, Error};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions, create_dir_all, rename};

use dag::ObjectKey;

pub trait ObjectStore {
    type ObjectRead: Read;
    type ObjectWrite: Write;

    fn has_object(&self, key: &ObjectKey) -> bool;
    fn read_object(&self, key: &ObjectKey) -> Result<Self::ObjectRead>;

    fn new_object(&mut self) -> Result<Self::ObjectWrite>;
    fn save_object(&mut self,
                   key: ObjectKey,
                   object: Self::ObjectWrite)
                   -> Result<()>;
}


pub struct DiskObjectStore {
    path: PathBuf,
}

pub struct DiskIncomingObject {
    temp_path: PathBuf,
    file: File,
}

impl DiskObjectStore {
    pub fn new(path: &Path) -> Self {
        DiskObjectStore { path: path.to_owned() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn init(&self) -> Result<()> {
        create_dir_all(&self.path)
    }

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }
}

impl ObjectStore for DiskObjectStore {
    type ObjectRead = File;
    type ObjectWrite = DiskIncomingObject;

    fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    fn read_object(&self, key: &ObjectKey) -> Result<Self::ObjectRead> {
        File::open(self.object_path(key))
    }

    fn new_object(&mut self) -> Result<Self::ObjectWrite> {
        let temp_path = self.path.join("tmp");
        try!(create_parents(&temp_path));
        let file = try!(OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));
        Ok(DiskIncomingObject {
            temp_path: temp_path,
            file: file,
        })
    }

    fn save_object(&mut self,
                   key: ObjectKey,
                   mut object: Self::ObjectWrite)
                   -> Result<()> {

        try!(object.flush());
        let permpath = self.object_path(&key);
        try!(create_parents(&permpath));
        rename(&object.temp_path, &permpath)
    }
}

impl Write for DiskIncomingObject {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.file.flush()
    }
}

fn create_parents(path: &Path) -> Result<Option<&Path>> {
    match path.parent() {
        Some(parent) => create_dir_all(parent).and(Ok(Some(parent))),
        None => Ok(None),
    }
}


#[cfg(test)]
pub mod test {
    extern crate tempdir;
    use self::tempdir::TempDir;

    use std::collections::HashMap;
    use std::io::{Write, Read, Result, Error, ErrorKind};

    use dag::ObjectKey;
    use super::{ObjectStore, DiskObjectStore};

    pub struct InMemoryObjectStore {
        map: HashMap<ObjectKey, Vec<u8>>,
    }

    impl InMemoryObjectStore {
        pub fn new() -> Self {
            InMemoryObjectStore { map: HashMap::new() }
        }
    }

    pub struct VecRead {
        vec: Vec<u8>,
        readpos: usize,
    }

    impl ObjectStore for InMemoryObjectStore {
        type ObjectRead = VecRead;
        type ObjectWrite = Vec<u8>;

        fn has_object(&self, key: &ObjectKey) -> bool {
            self.map.contains_key(key)
        }

        fn read_object(&self, key: &ObjectKey) -> Result<Self::ObjectRead> {
            self.map
                .get(key)
                .map(|v| {
                    VecRead {
                        vec: v.clone(),
                        readpos: 0,
                    }
                })
                .ok_or(Error::new(ErrorKind::Other, "Key not found"))
        }

        fn new_object(&mut self) -> Result<Self::ObjectWrite> {
            Ok(Vec::new())
        }

        fn save_object(&mut self,
                       key: ObjectKey,
                       object: Self::ObjectWrite)
                       -> Result<()> {

            self.map.insert(key, object);
            Ok(())
        }
    }

    impl Read for VecRead {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
            let result = (&self.vec[self.readpos..]).read(buf);
            if let Ok(count) = result {
                self.readpos += count;
            }
            result
        }
    }


    fn do_object_store_trait_tests<F, O>(create_temp_object_store: F)
        where F: Fn() -> O,
              O: ObjectStore
    {
        let (key, data) = ("69342c5c39e5ae5f0077aecc32c0f81811fb8193"
            .to_string(),
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

    #[test]
    fn test_in_mem_object_store() {
        do_object_store_trait_tests(InMemoryObjectStore::new);
    }

    fn create_temp_disk_object_store() -> DiskObjectStore {
        let tmp = TempDir::new_in("/dev/shm", "object_store_test")
            .expect("create tempdir");
        let object_store = DiskObjectStore::new(tmp.path());
        object_store.init().expect("initialize object store");
        object_store
    }

    #[test]
    fn test_disk_object_store() {
        do_object_store_trait_tests(create_temp_disk_object_store);
    }
}
