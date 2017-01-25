use cache;
use error::*;
use std::iter;
use std::path;
use walkdir;
use walkdir::WalkDirIterator;

type DirEntry = walkdir::DirEntry;
type DirEntryResult = walkdir::Result<DirEntry>;
type DirEntryIterator = Iterator<Item = DirEntryResult>;

pub fn dirs_depth_first(path: &path::Path) -> Box<DirEntryIterator> {

    Box::new(walkdir::WalkDir::new(path)
        .sort_by(|a, b| a.cmp(b))
        .into_iter()
        .filter_entry(|d| d.file_type().is_dir()))
}

pub struct CacheCheck<I>
    where I: Iterator<Item = walkdir::Result<DirEntry>>
{
    dirs_in: I,
}

pub struct CacheEntry {
    path: path::PathBuf,
    status: cache::CacheStatus,
}

impl<I> iter::Iterator for CacheCheck<I>
    where I: Iterator<Item = walkdir::Result<DirEntry>>
{
    type Item = Result<CacheEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.dirs_in.next().map(|dir_entry_result| {
            dir_entry_result.err_into().and_then(|dir_entry| {

                if dir_entry.file_type().is_file() {
                    let path = dir_entry.path();
                    cache::HashCacheFile::open_and_check_file(path)
                        .map(|(cache_status, _, _, _)| {
                            CacheEntry {
                                path: path.to_owned(),
                                status: cache_status,
                            }
                        })
                } else {
                    Err(ErrorKind::NotAFile(dir_entry.path().to_owned()).into())
                }

            })
        })
    }
}
