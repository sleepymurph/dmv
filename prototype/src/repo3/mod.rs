use std::fmt;
use std::io;
use std::error;
use std::io::{Write, Read, Result};
use std::path::{Path, PathBuf};
use std::fs::{rename, create_dir_all, OpenOptions, File};
use dag::*;

pub trait Repo {
    fn init(&mut self) -> Result<()>;
    fn has_object(&self, key: &ObjectKey) -> bool;
    fn read_object(&mut self, key: &ObjectKey) -> Result<File>;
    fn incoming(&mut self) -> Result<IncomingObject>;
    fn store_incoming(&mut self,
                      mut incoming: IncomingObject,
                      key: &ObjectKey)
                      -> Result<()>;
}

pub struct DiskRepo {
    path: PathBuf,
}

pub struct IncomingObject {
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

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }
}

impl Repo for DiskRepo {
    fn init(&mut self) -> Result<()> {
        create_dir_all(&self.path)
    }

    fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    fn read_object(&mut self, key: &ObjectKey) -> Result<File> {
        File::open(self.object_path(key))
    }

    fn incoming(&mut self) -> Result<IncomingObject> {
        let temp_path = &self.path.join("tmp");
        try!(create_parents(&temp_path));
        let file = try!(OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));
        Ok(IncomingObject {
            temp_path: temp_path.to_owned(),
            file: file,
        })
    }

    fn store_incoming(&mut self,
                      mut incoming: IncomingObject,
                      key: &ObjectKey)
                      -> Result<()> {
        try!(incoming.file.flush());
        let permpath = self.object_path(key);
        try!(create_parents(&permpath));
        rename(&incoming.temp_path, &permpath)
    }
}

impl Write for IncomingObject {
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
        assert_eq!(
            repo.object_path("a9c3334cfee4083a36bf1f9d952539806fff50e2"),
            Path::new(".prototype/objects/")
                        .join("a9/c3/334cfee4083a36bf1f9d952539806fff50e2"));
    }

    #[test]
    fn test_add_object() {
        let mut repo = mem_temp_repo();
        let data = "here be content";
        let key = "9cac8e6ad1da3212c89b73fdbb2302180123b9ca";

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
