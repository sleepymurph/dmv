use std::fmt;
use std::ops::{Index, Range};
use std::io;
use std::error;
use std::io::{Cursor, Write, Read, Result, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::fs::{rename, create_dir_all, OpenOptions, File};
use std::collections::HashMap;
use std::hash::Hash;
use dag::*;

pub trait Repo<'a> {
    type ObjectRead: 'a + Read;
    type IncomingObject: 'a + Write;

    fn init(&mut self) -> Result<()>;
    fn has_object<K>(&self, key: &K) -> bool where K: AsRef<String>;
    fn read_object<K>(&mut self, key: &K) -> Result<Self::ObjectRead>
        where K: AsRef<String>;
    fn incoming(&mut self) -> Result<Self::IncomingObject>;
    fn store_incoming<K>(&mut self,
                         mut incoming: Self::IncomingObject,
                         key: &K)
                         -> Result<()>
        where K: AsRef<String>;
}

pub struct DiskRepo {
    path: PathBuf,
}

pub struct DiskIncomingObject {
    temp_path: PathBuf,
    file: File,
}

impl DiskRepo {
    pub fn new(path: &Path) -> Self {
        DiskRepo { path: path.to_owned() }
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }

    fn object_path<K>(&self, key: &K) -> PathBuf
        where K: AsRef<String>
    {
        let key = key.as_ref();
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }
}

impl<'a> Repo<'a> for DiskRepo {
    type ObjectRead = File;
    type IncomingObject = DiskIncomingObject;

    fn init(&mut self) -> Result<()> {
        create_dir_all(&self.path)
    }

    fn has_object<K>(&self, key: &K) -> bool
        where K: AsRef<String>
    {
        self.object_path(key).is_file()
    }

    fn read_object<K>(&mut self, key: &K) -> Result<Self::ObjectRead>
        where K: AsRef<String>
    {
        File::open(self.object_path(key))
    }

    fn incoming(&mut self) -> Result<Self::IncomingObject> {
        let temp_path = &self.path.join("tmp");
        try!(create_parents(&temp_path));
        let file = try!(OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));
        Ok(DiskIncomingObject {
            temp_path: temp_path.to_owned(),
            file: file,
        })
    }

    fn store_incoming<K>(&mut self,
                         mut incoming: Self::IncomingObject,
                         key: &K)
                         -> Result<()>
        where K: AsRef<String>
    {
        try!(incoming.file.flush());
        let permpath = self.object_path(key);
        try!(create_parents(&permpath));
        rename(&incoming.temp_path, &permpath)
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

type MemBlob = Vec<u8>;

pub struct MemRepo {
    object_map: HashMap<ObjectKey, MemBlob>,
}

impl MemRepo {
    fn new() -> Self {
        MemRepo { object_map: HashMap::new() }
    }
}

impl<'a> Repo<'a> for MemRepo {
    type ObjectRead = &'a [u8];
    type IncomingObject = Cursor<MemBlob>;

    fn init(&mut self) -> Result<()> {
        Ok(())
    }
    fn has_object<K>(&self, key: &K) -> bool
        where K: AsRef<String>
    {
        self.object_map.contains_key(key.as_ref())
    }
    fn read_object<K>(&mut self, key: &K) -> Result<&'a Self::ObjectRead>
        where K: AsRef<String>
    {
        let poop = self.object_map.get(key.as_ref());
        match poop {
            Some(blob) => Ok(blob),
            None => Err(Error::new(ErrorKind::Other, "Key not found: {}")),
        }
    }
    fn incoming(&mut self) -> Result<Self::IncomingObject> {
        unimplemented!();
    }
    fn store_incoming<K>(&mut self,
                         mut incoming: Self::IncomingObject,
                         key: &K)
                         -> Result<()>
        where K: AsRef<String>
    {

        unimplemented!();
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::*;
    use std::io::{Write, Read, Result};
    use std::path::{Path, PathBuf};

    fn mem_temp_repo() -> DiskRepo {
        let tempdir = tempdir::TempDir::new_in("/dev/shm/", "rust_test")
            .expect("create temporary directory in /dev/shm/");

        let mut repo = DiskRepo::new(&tempdir.path().join("repo"));
        repo.init().expect("initialize temporary repo");

        assert_eq!(repo.path().file_name().unwrap(), "repo");
        assert_eq!(repo.path().is_dir(), true);

        repo
    }

    #[test]
    fn test_object_path() {
        let mut repo = DiskRepo::new(Path::new(".prototype"));
        assert_eq!(repo.object_path("a9c3334cfee4083a36bf1f9d952539806fff50e2"
                       .as_ref()),
                   Path::new(".prototype/objects/")
                       .join("a9/c3/334cfee4083a36bf1f9d952539806fff50e2"));
    }

    #[test]
    fn test_disk_implementation() {
        do_repo_trait_test(mem_temp_repo);
    }

    #[test]
    fn test_hashmap_implementation() {
        do_repo_trait_test(MemRepo::new);
    }

    fn do_repo_trait_test<'a, F, T>(create_temp_repo: F)
        where F: Fn() -> T,
              T: Repo<'a>
    {
        let mut repo = create_temp_repo();
        let data = "here be content";
        let key = "9cac8e6ad1da3212c89b73fdbb2302180123b9ca".as_ref();

        let mut incoming = repo.incoming().expect("open incoming");
        incoming.write(data.as_bytes()).expect("write to incoming");
        repo.store_incoming(incoming, key).expect("set key");

        assert_eq!(repo.has_object(key), true);

        let mut reader = repo.read_object(key).expect("open saved object");
        let mut read_data = String::new();
        reader.read_to_string(&mut read_data).expect("read saved object");
        assert_eq!(read_data, data);
    }
}
