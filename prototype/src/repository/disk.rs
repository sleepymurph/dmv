use std::io;
use std::fs;
use std::io::Write;
use std::path;
use std::ops;
use super::*;
use dag::*;

pub struct DiskRepository {
    path: path::PathBuf,
}

pub struct DiskIncoming<'a> {
    repo: &'a DiskRepository,
    path: path::PathBuf,
    file: fs::File,
}

impl DiskRepository {
    pub fn new(path: &path::Path) -> Self {
        DiskRepository { path: path.to_owned() }
    }

    fn path(&self) -> &path::PathBuf {
        &self.path
    }

    fn object_path(&self, key: &ObjectKey) -> path::PathBuf {
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }
}

impl<'a> Repository<'a> for DiskRepository {
    type IncomingType = DiskIncoming<'a>;

    fn init(&mut self) -> io::Result<()> {
        fs::create_dir_all(&self.path)
    }

    fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }
    fn stat_object(&mut self, key: &ObjectKey) -> ObjectStat {
        unimplemented!();
    }
    fn read_object(&mut self, key: &ObjectKey) -> &mut io::Read {
        unimplemented!();
    }
    fn add_object(&'a mut self) -> io::Result<DiskIncoming<'a>> {
        let temp_path = &self.path.join("tmp");
        let file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path));
        Ok(DiskIncoming {
            repo: self,
            path: temp_path.to_owned(),
            file: file,
        })
    }
}

impl<'a> IncomingObject<'a> for DiskIncoming<'a> {
    fn set_key(mut self, key: &ObjectKey) -> io::Result<()> {
        try!(self.file.flush());
        let permpath = self.repo.object_path(key);
        if let Some(parent) = permpath.parent() {
            try!(fs::create_dir_all(parent));
        }
        fs::rename(self.path, permpath)
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
    extern crate tempdir;

    use std::path;
    use std::io;
    use std::io::Write;
    use std::fs;
    use std::ffi;
    use super::*;
    use super::super::*;

    fn mem_temp_repo() -> (tempdir::TempDir, DiskRepository) {
        let tempdir = tempdir::TempDir::new_in("/dev/shm/", "rust_test")
            .expect("could not create temporary directory in /dev/shm/");

        let mut repo = DiskRepository::new(&tempdir.path().join("repo"));
        repo.init().expect("could not initialize temporary repo");

        assert_eq!(repo.path().file_name().unwrap(), "repo");
        assert_eq!(repo.path().is_dir(), true);

        (tempdir, repo)
    }

    #[test]
    fn test_object_path() {
        let mut repo = DiskRepository::new(path::Path::new(".prototype"));
        assert_eq!(
            repo.object_path("a9c3334cfee4083a36bf1f9d952539806fff50e2"),
            path::Path::new(".prototype/objects/")
                        .join("a9/c3/334cfee4083a36bf1f9d952539806fff50e2"));
    }

    #[test]
    fn test_add_object() {
        let (dir, mut repo) = mem_temp_repo();
        let key = "9cac8e6ad1da3212c89b73fdbb2302180123b9ca";
        {
        let mut incoming = repo.add_object().expect("could not open incoming");
        incoming.write(b"here be content")
            .expect("could not write to incoming");
        incoming.flush().expect("could not flush incoming");
        incoming.set_key(key)
            .expect("could not set key");
        }
        assert_eq!(repo.has_object(key), true);
    }
}


mod toy_tests {

    use std::path;

    struct Parent {
    }

    struct Spawn<'a> {
        parent: &'a Parent,
        name: String,
    }

    impl Parent {
        fn say_hi(&self, spawn: &Spawn) -> String {
            format!("Hello {}", &spawn.name)
        }
        fn create_spawn(&self, name: String) -> Spawn {
            Spawn{ parent: &self, name: name }
        }
    }

    impl<'a> Spawn<'a> {
        fn nag_parent(&self) -> String {
            self.parent.say_hi(&self)
        }
    }

    #[test]
    fn test_spawn() {
        let parent = Parent{};
        let spawn = parent.create_spawn("duder".into());
        assert_eq!(spawn.nag_parent(), "Hello duder");
    }
}
