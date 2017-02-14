//! High-level commands

use cache::AllCaches;
use dag::ObjectCommon;
use dag::ObjectHandle;
use dag::ObjectKey;
use error::*;
use fs_transfer::ObjectFsTransfer;
use humanreadable::human_bytes;
use objectstore::ObjectStore;
use std::path::Path;
use std::path::PathBuf;

pub fn init(repo_path: PathBuf) -> Result<()> {
    try!(ObjectStore::init(repo_path));
    Ok(())
}

pub fn hash_object(repo_path: PathBuf, file_path: PathBuf) -> Result<()> {

    let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)?;

    let hash;
    if file_path.is_file() {
        hash = fs_transfer.hash_file(file_path.clone())?;

    } else if file_path.is_dir() {

        let partial = fs_transfer.dir_to_partial_tree(&file_path)?;
        println!("{} to hash. Hashing...",
                 human_bytes(partial.unhashed_size()));
        hash = fs_transfer.hash_partial_tree(&file_path, partial)?;
    } else {
        unimplemented!()
    };

    println!("{} {}", hash, file_path.display());
    Ok(())
}

pub fn show_object(repo_path: PathBuf, hash: &ObjectKey) -> Result<()> {

    let object_store = try!(ObjectStore::open(repo_path));

    if !object_store.has_object(&hash) {
        println!("No such object");
    } else {
        let handle = try!(object_store.open_object(&hash));
        match handle {
            ObjectHandle::Blob(blobhandle) => {
                println!("{}", blobhandle.header());
            }
            _ => {
                let object = try!(handle.read_content());
                println!("{}", object.pretty_print());
            }
        }
    }
    Ok(())
}

pub fn extract_object(repo_path: PathBuf,
                      hash: &ObjectKey,
                      file_path: &Path)
                      -> Result<()> {

    let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)?;
    fs_transfer.extract_object(&hash, &file_path)
}

pub fn cache_status(file_path: PathBuf) -> Result<()> {
    let mut cache = AllCaches::new();
    let cache_status = try!(cache.check(&file_path));
    println!("{:?}", cache_status);
    Ok(())
}
