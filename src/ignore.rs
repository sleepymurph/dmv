use constants;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;

pub struct IgnoreList(BTreeSet<PathBuf>);

impl IgnoreList {
    pub fn empty() -> Self { IgnoreList(BTreeSet::new()) }

    pub fn ignores<P: ?Sized>(&self, path: &P) -> bool
        where P: AsRef<Path>
    {
        let path = path.as_ref();

        for pattern in &self.0 {
            // Match full paths
            if pattern == path {
                debug!("Ignoring '{}' (full match)", path.display());
                return true;
            }
            // Match single component names
            for component in path.iter() {
                if component == pattern {
                    debug!("Ignoring '{}' (component match: {:?})",
                           path.display(),
                           Path::new(component).display());
                    return true;
                }
            }
        }
        false
    }

    pub fn insert<P>(&mut self, pattern: P) -> bool
        where P: Into<PathBuf>
    {
        let pattern = pattern.into();
        self.0.insert(pattern)
    }
}

impl Default for IgnoreList {
    fn default() -> Self {
        let mut list = IgnoreList::empty();
        list.insert(constants::HIDDEN_DIR_NAME);
        list.insert(constants::CACHE_FILE_NAME);
        list
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::path::PathBuf;
    use super::*;

    #[test]
    fn test_ignore() {
        let mut ignore = IgnoreList::empty();

        assert!(!ignore.ignores("foo"));
        assert!(!ignore.ignores(Path::new("foo")));
        assert!(!ignore.ignores(&PathBuf::from("foo")));

        ignore.insert("foo");

        assert!(ignore.ignores("foo"));
        assert!(ignore.ignores("./foo"));
        assert!(ignore.ignores("./subdir/foo"));
        assert!(!ignore.ignores("./subdir/sfoo"));
        assert!(!ignore.ignores("./subdir/foos"));
        assert!(ignore.ignores("./subdir/foo/child"));

        assert!(!ignore.ignores("bar"));

        ignore.insert("./fully/specified/path");
        assert!(ignore.ignores("./fully/specified/path"));
    }

    #[test]
    fn test_default_ignore_hidden_dir() {
        let ignore = IgnoreList::default();

        // Specific hidden names by themselves
        assert!(ignore.ignores(".prototype"));
        assert!(ignore.ignores(".prototype_cache"));

        // Specific hidden names with leading directory
        assert!(ignore.ignores("./.prototype"));
        assert!(ignore.ignores("./.prototype_cache"));
    }
}
