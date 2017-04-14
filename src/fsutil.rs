//! Convenience methods for working with the filesystem

use std::fs;
use std::io;
use std::path;

/// Create all parent directories of a path
pub fn create_parents<P: AsRef<path::Path>>
    (path: &P)
     -> io::Result<Option<&path::Path>> {
    match path.as_ref().parent() {
        Some(parent) => fs::create_dir_all(parent).and(Ok(Some(parent))),
        None => Ok(None),
    }
}

/// Creates an iterator over the current directory and its parents
pub fn up_from<P: AsRef<path::Path>>(path: &P) -> Parents {
    Parents(Some(path.as_ref()))
}

/// Iterator over the parents of a path, created by the `up_from` function
pub struct Parents<'a>(Option<&'a path::Path>);

impl<'a> Iterator for Parents<'a> {
    type Item = &'a path::Path;
    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0;
        self.0 = self.0.and_then(|p| p.parent());
        next
    }
}

pub fn is_empty_dir<P: AsRef<path::Path>>(path: &P) -> Result<bool, io::Error> {
    let path = path.as_ref();
    if !path.is_dir() {
        return Ok(false);
    }
    for _ in path.read_dir()? {
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod test {
    use std::path;
    use super::*;

    #[test]
    pub fn test_up_from_iterator() {
        let path = path::PathBuf::from("/a/b/c/d");
        let mut iter = up_from(&path);
        assert_eq!(iter.next().and_then(|p| p.to_str()), Some("/a/b/c/d"));
        assert_eq!(iter.next().and_then(|p| p.to_str()), Some("/a/b/c"));
        assert_eq!(iter.next().and_then(|p| p.to_str()), Some("/a/b"));
        assert_eq!(iter.next().and_then(|p| p.to_str()), Some("/a"));
        assert_eq!(iter.next().and_then(|p| p.to_str()), Some("/"));
        assert_eq!(iter.next().and_then(|p| p.to_str()), None);
    }
}
