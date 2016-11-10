use std::io::{Read, Write, Result};
use std::path::{Path, PathBuf};
use std::fs::File;

use dag::ObjectKey;

/// Content addressable storage for blobs
///
/// Note that this trait's methods consume `self`. This is because the trait is meant to be
/// implemented on references, so that you can use references for the associated types,
/// `ObjectRead` and `ObjectWrite`. This is to get around the lack of syntax for higher-kinded
/// types. See the ["Lack of iterator methods" section of this RFC](
/// https://github.com/aturon/rfcs/blob/collections-conventions/text/0000-collection-conventions.md#lack-of-iterator-methods)
/// for more information.
///
pub trait ObjectStore {
    type ObjectRead: Read;
    type ObjectWrite: IncomingObject;

    fn has_object(self, key: &ObjectKey) -> bool;
    fn read_object(self, key: &ObjectKey) -> Result<Self::ObjectRead>;

    fn new_object(self) -> Result<Self::ObjectWrite>;
}

pub trait IncomingObject: Write {
    fn store(self, key: ObjectKey) -> Result<()>;
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

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }
}


#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::io::{Write, Read, Result, Error, ErrorKind};

    use dag::ObjectKey;
    use super::{ObjectStore, IncomingObject};

    pub struct InMemObjectStore {
        map: HashMap<ObjectKey, Vec<u8>>,
    }

    impl InMemObjectStore {
        pub fn new() -> Self {
            InMemObjectStore { map: HashMap::new() }
        }
    }

    pub struct InMemIncomingObject<'a> {
        object_store: &'a mut InMemObjectStore,
        byte_vec: Vec<u8>,
    }

    impl<'a> ObjectStore for &'a mut InMemObjectStore {
        type ObjectRead = &'a [u8];
        type ObjectWrite = InMemIncomingObject<'a>;

        fn has_object(self, key: &ObjectKey) -> bool {
            self.map.contains_key(key)
        }

        fn read_object(self, key: &ObjectKey) -> Result<Self::ObjectRead> {
            self.map
                .get(key)
                .map(|v| v.as_ref())
                .ok_or(Error::new(ErrorKind::Other, "Key not found"))
        }

        fn new_object(self) -> Result<Self::ObjectWrite> {
            Ok(InMemIncomingObject{
                object_store: self,
                byte_vec: Vec::new(),
            })
        }
    }

    impl<'a> Write for InMemIncomingObject<'a> {
        fn write(&mut self, buf: &[u8]) -> Result<usize> {
            self.byte_vec.write(buf)
        }
        fn flush(&mut self) -> Result<()> {
            self.byte_vec.flush()
        }
    }

    impl<'a> IncomingObject for InMemIncomingObject<'a> {
        fn store(self, key: ObjectKey) -> Result<()> {
            self.object_store.map.insert(key, self.byte_vec);
            Ok(())
        }
    }



    fn do_object_store_trait_tests<F, O>(create_temp_object_store: F)
        where F: Fn() -> O,
              for<'a> &'a mut O: ObjectStore
    {
        let (key, data) = ("69342c5c39e5ae5f0077aecc32c0f81811fb8193"
            .to_string(),
                           "Hello!".to_string());
        let mut store = create_temp_object_store();
        assert_eq!(store.has_object(&key), false);
        {
            let mut writer = store.new_object().expect("new incoming object");
            writer.write(data.as_bytes()).expect("write to incoming");
            writer.store(key.clone()).expect("store incoming");
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
        do_object_store_trait_tests(InMemObjectStore::new);
    }
}
