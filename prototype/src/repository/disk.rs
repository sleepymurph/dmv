use std::io;
use std::fs;
use std::path;
use std::ops;
use super::*;
use dag::*;

pub struct DiskRepository<'a> {
    path: &'a path::Path,
}

impl<'a> DiskRepository<'a> {
    pub fn new(path: &'a path::Path) -> Self {
        DiskRepository { path: path }
    }

    fn object_path(&self, key: &ObjectKey) -> path::PathBuf {
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }
}

pub struct DiskIncoming<'a> {
    path: &'a path::Path,
    file: fs::File,
}

impl<'a> Repository for DiskRepository<'a> {
    type IncomingType = DiskIncoming<'a>;
    fn has_object(&mut self, key: &ObjectKey) -> bool {
        return self.object_path(key).is_file();
    }
    fn stat_object(&mut self, key: &ObjectKey) -> ObjectStat {
        unimplemented!();
    }
    fn read_object(&mut self, key: &ObjectKey) -> &mut io::Read {
        unimplemented!();
    }
    fn add_object(&mut self) -> DiskIncoming<'a> {
        unimplemented!();
    }
}

impl<'a> DiskIncoming<'a> {
    fn new(path: &'a path::Path) -> io::Result<Self> {
        let file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path));
        Ok(DiskIncoming{path: path, file:file})
    }
}

impl<'a> IncomingObject for DiskIncoming<'a> {
    fn set_key(self, _key: &ObjectKey) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> io::Write for DiskIncoming<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

mod test {
    use std::path;
    use super::*;

    #[test]
    fn test_object_path() {
        let mut repo = DiskRepository::new(path::Path::new(".prototype"));
        assert_eq!(
            repo.object_path("a9c3334cfee4083a36bf1f9d952539806fff50e2"),
            path::Path::new(".prototype/objects/")
                        .join("a9/c3/334cfee4083a36bf1f9d952539806fff50e2"));
    }

    #[test]
    fn test_store_object() {
    }
}
