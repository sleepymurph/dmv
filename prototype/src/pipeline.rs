use std::path;
use walkdir;
use walkdir::WalkDirIterator;

pub fn dirs_depth_first
    (path: &path::Path)
     -> Box<Iterator<Item = walkdir::Result<walkdir::DirEntry>>> {

    Box::new(walkdir::WalkDir::new(path)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|d| d.file_type().is_dir()))
}
