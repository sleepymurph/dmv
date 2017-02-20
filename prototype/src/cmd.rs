//! High-level commands

use cache::AllCaches;
use dag::Commit;
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

pub fn hash_object(repo_path: PathBuf, path: PathBuf) -> Result<()> {

    let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)?;
    let hash = hash_object_inner(&mut fs_transfer, &path)?;
    println!("{} {}", hash, path.display());
    Ok(())
}

fn hash_object_inner(fs_transfer: &mut ObjectFsTransfer,
                     path: &Path)
                     -> Result<ObjectKey> {
    let status = fs_transfer.check_status(&path)?;
    if status.unhashed_size() > 0 {
        println!("{} to hash. Hashing...",
                 human_bytes(status.unhashed_size()));
    }
    fs_transfer.hash_object(&path, status)
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

pub fn commit(repo_path: PathBuf,
              message: String,
              path: PathBuf)
              -> Result<()> {
    let mut fs_transfer = ObjectFsTransfer::with_repo_path(repo_path)?;
    let branch = "master";
    let parents = match fs_transfer.object_store.read_ref(branch) {
        Ok(hash) => {
            debug!("Current branch: {}, {}", branch, hash);
            vec![hash]
        }
        Err(Error(ErrorKind::RefNotFound(_), _)) => {
            debug!("New branch: {}", branch);
            vec![]
        }
        Err(e) => return Err(e),
    };
    let tree_hash = hash_object_inner(&mut fs_transfer, &path)?;
    let commit = Commit {
        tree: tree_hash,
        parents: parents,
        message: message,
    };
    let commit_hash = fs_transfer.object_store.store_object(&commit)?;
    fs_transfer.object_store.update_ref(branch, &commit_hash)?;
    println!("{} is now {}", branch, commit_hash);
    Ok(())
}
