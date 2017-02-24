//! High-level commands

use cache::AllCaches;
use constants::HARDCODED_BRANCH;
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

pub fn commit(message: String) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    let (branch, hash) = work_dir.commit(message)?;
    println!("{} is now {}", branch.unwrap_or("<detached head>"), hash);
    Ok(())
}

pub fn log() -> Result<()> {
    let object_store = find_object_store()?;
    let branch = RevSpec::from_str(HARDCODED_BRANCH)?;
    for commit in object_store.log(&branch)? {
        let (hash, commit, refs) = commit?;
        match refs.len() {
            0 => println!("{} {}", hash, commit.message),
            _ => println!("{} ({}) {}", hash, refs.join(", "), commit.message),
        }
    }
    Ok(())
}

pub fn branch_list() -> Result<()> {
    let work_dir = find_work_dir()?;
    for (name, _) in work_dir.refs() {
        if work_dir.branch() == Some(name.as_str()) {
            print!("* ");
        } else {
            print!("  ");
        }
        println!("{}", name)
    }
    Ok(())
}

pub fn branch_set(branch_name: &str, target: RevSpec) -> Result<()> {
    let mut object_store = find_object_store()?;
    let hash = object_store.find_object(&target)?;
    object_store.update_ref(branch_name, hash)
}

pub fn branch_set_to_head(branch_name: &str) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    work_dir.update_ref_to_head(branch_name)?;
    Ok(())
}
