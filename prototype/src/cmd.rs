//! High-level commands

use cache::AllCaches;
use dag::Commit;
use dag::ObjectCommon;
use dag::ObjectHandle;
use error::*;
use find_repo::RepoLayout;
use find_repo::find_fs_transfer;
use find_repo::find_object_store;
use find_repo::find_work_dir;
use objectstore::RevSpec;
use std::env::current_dir;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use work_dir::WorkDir;

pub fn init() -> Result<()> {
    let layout = RepoLayout::in_work_dir(current_dir()?);
    WorkDir::init(layout)?;
    Ok(())
}

pub fn hash_object(path: PathBuf) -> Result<()> {

    let mut fs_transfer = find_fs_transfer()?;
    let hash = fs_transfer.hash_path(&path)?;
    println!("{} {}", hash, path.display());
    Ok(())
}

pub fn show_object(obj_spec: &RevSpec) -> Result<()> {

    let object_store = find_object_store()?;

    let hash = object_store.find_object(obj_spec)?;

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
    Ok(())
}

pub fn extract_object(obj_spec: &RevSpec, file_path: &Path) -> Result<()> {

    let mut fs_transfer = find_fs_transfer()?;
    let hash = fs_transfer.object_store.find_object(obj_spec)?;
    fs_transfer.extract_object(&hash, &file_path)
}

pub fn cache_status(file_path: PathBuf) -> Result<()> {
    let mut cache = AllCaches::new();
    let cache_status = try!(cache.check(&file_path));
    println!("{:?}", cache_status);
    Ok(())
}

const HARDCODED_BRANCH: &'static str = "master";

pub fn commit(message: String) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    let branch = "master";
    let parents = match work_dir.object_store().try_find_ref(branch) {
        Ok(Some(hash)) => vec![hash],
        Ok(None) => vec![],
        Err(e) => bail!(e),
    };
    debug!("Current branch: {}. Parents: {}",
           branch,
           parents.iter()
               .map(|h| h.to_short())
               .collect::<Vec<String>>()
               .join(","));
    let path = work_dir.path().to_owned();
    let tree_hash = work_dir.fs_transfer().hash_path(&path)?;
    let commit = Commit {
        tree: tree_hash,
        parents: parents,
        message: message,
    };
    let commit_hash = work_dir.object_store().store_object(&commit)?;
    work_dir.object_store().update_ref(branch, &commit_hash)?;
    println!("{} is now {}", branch, commit_hash);
    Ok(())
}

pub fn log() -> Result<()> {
    let object_store = find_object_store()?;
    let branch = RevSpec::from_str(HARDCODED_BRANCH)?;
    let hash = object_store.find_object(&branch)?;
    let mut next = Some(hash);
    while let Some(hash) = next {
        let handle = object_store.open_object(&hash)?;
        match handle {
            ObjectHandle::Commit(commit) => {
                let commit = commit.read_content()?;
                println!("{} {}", hash, commit.message);
                next = match commit.parents.len() {
                    0 => None,
                    1 => Some(commit.parents[0]),
                    _ => unimplemented!(),
                }
            }
            other => {
                bail!("{} is a {:?}. Expected a commit.",
                      hash,
                      other.header().object_type)
            }
        }
    }
    Ok(())
}
