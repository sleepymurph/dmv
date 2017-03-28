//! High-level commands

use cache::AllCaches;
use dag::ObjectCommon;
use dag::ObjectHandle;
use error::*;
use find_repo::RepoLayout;
use find_repo::find_fs_transfer;
use find_repo::find_object_store;
use find_repo::find_work_dir;
use std::env::current_dir;
use std::path::Path;
use std::path::PathBuf;
use work_dir::WorkDir;

pub fn init() -> Result<()> {
    let layout = RepoLayout::in_work_dir(current_dir()?);
    WorkDir::init(layout)?;
    Ok(())
}

pub fn hash_object(path: PathBuf) -> Result<()> {

    let mut fs_transfer = find_fs_transfer()?;
    let hash = fs_transfer.hash_obj_file(None, &path)?;
    println!("{} {}", hash, path.display());
    Ok(())
}

pub fn show_object(obj_spec: &str) -> Result<()> {

    let object_store = find_object_store()?;

    let hash = object_store.expect_ref_or_hash(obj_spec)?.into_hash();

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

pub fn parents() -> Result<()> {
    let work_dir = find_work_dir()?;
    for parent in work_dir.parents() {
        println!("{}", parent);
    }
    Ok(())
}

pub fn ls_files(obj_spec: Option<&str>, verbose: bool) -> Result<()> {

    match obj_spec {
        Some(ref r) => {
            let object_store = &find_object_store()?;
            let hash = object_store.expect_ref_or_hash(r)?.into_hash();
            print!("{}", object_store.ls_files(hash, verbose)?);
        }
        None => {
            let wd = find_work_dir()?;
            let hash = wd.head()
                .ok_or_else(|| "No commit specified and no parent commit")?;
            print!("{}", wd.ls_files(hash, verbose)?);
        }
    };
    Ok(())
}

pub fn extract_object(obj_spec: &str, file_path: &Path) -> Result<()> {

    let fs_transfer = find_fs_transfer()?;
    let hash = fs_transfer.expect_ref_or_hash(obj_spec)?.into_hash();
    fs_transfer.extract_object(&hash, &file_path)
}

pub fn cache_status(file_path: PathBuf) -> Result<()> {
    let cache = AllCaches::new();
    let cache_status = cache.status(&file_path, &file_path.metadata()?)?;
    println!("{} {}", cache_status, file_path.display());
    Ok(())
}

pub fn status(show_ignored: bool,
              rev1: Option<&str>,
              rev2: Option<&str>)
              -> Result<()> {
    let mut work_dir = find_work_dir()?;
    work_dir.status(show_ignored, rev1, rev2)
}

pub fn commit(message: String) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    let (branch, hash) = work_dir.commit(message)?;
    println!("{} is now {}", branch.unwrap_or("<detached head>"), hash);
    Ok(())
}

pub fn log() -> Result<()> {
    let work_dir = find_work_dir()?;
    work_dir.log()
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

pub fn branch_set(branch_name: &str, target: &str) -> Result<()> {
    let mut object_store = find_object_store()?;
    let hash = object_store.expect_ref_or_hash(&target)?.into_hash();
    object_store.update_ref(branch_name, hash)
}

pub fn branch_set_to_head(branch_name: &str) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    work_dir.update_ref_to_head(branch_name)?;
    Ok(())
}

pub fn fsck() -> Result<()> {
    let object_store = find_object_store()?;
    let bad = object_store.fsck()?;
    for &(expected, actual) in &bad {
        println!("Corrupt object {0}: expected {0:x}, actual {1:x}",
                 expected,
                 actual);
    }
    if bad.is_empty() {
        println!("All objects OK");
        Ok(())
    } else {
        bail!("Repository has corrupt objects")
    }
}

pub fn checkout(target: &str) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    work_dir.checkout(target)
}
