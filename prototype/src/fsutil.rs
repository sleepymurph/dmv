use std::fs;
use std::io;
use std::path;

pub fn create_parents(path: &path::Path)
                      -> io::Result<Option<&path::Path>> {
    match path.parent() {
        Some(parent) => fs::create_dir_all(parent).and(Ok(Some(parent))),
        None => Ok(None),
    }
}
