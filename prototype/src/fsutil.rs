use std::ffi;
use std::fs;
use std::io;
use std::path;

pub fn new_path_buf<S: AsRef<ffi::OsStr> + ?Sized>(s: &S) -> path::PathBuf {
    path::Path::new(s).to_path_buf()
}

pub fn create_parents(path: &path::Path) -> io::Result<Option<&path::Path>> {
    match path.parent() {
        Some(parent) => fs::create_dir_all(parent).and(Ok(Some(parent))),
        None => Ok(None),
    }
}
