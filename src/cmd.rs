//! High-level commands

use cache::AllCaches;
use dag::ObjectCommon;
use dag::ObjectHandle;
use error::*;
use find_repo::RepoLayout;
use find_repo::find_fs_transfer;
use find_repo::find_object_store;
use find_repo::find_work_dir;
use revisions::*;
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

pub fn show_object(rev: &RevSpec, type_only: bool) -> Result<()> {

    let object_store = find_object_store()?;

    let (hash, _, _) = object_store.lookup(rev)?;

    let handle = try!(object_store.open_object(&hash));
    if type_only {
        println!("{}", handle.header().object_type);
    } else {
        match handle {
            ObjectHandle::Blob(blobhandle) => {
                println!("{}", blobhandle.header());
            }
            _ => {
                let object = try!(handle.read_content());
                print!("{}", object.pretty_print());
            }
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

pub fn ls_files(rev: Option<RevSpec>, verbose: bool) -> Result<()> {

    match rev {
        Some(ref r) => {
            let object_store = &find_object_store()?;
            let (hash, _, _) = object_store.lookup(r)?;
            print!("{}", object_store.ls_files(hash, verbose)?);
        }
        None => {
            let wd = find_work_dir()?;
            let mut hash = wd.head()
                .ok_or_else(|| "No commit specified and no parent commit")?;
            if let &Some(ref path) = &wd.state.subtree {
                hash = wd.object_store.lookup_rev_path(&hash, path)?;
            }
            print!("{}", wd.ls_files(hash, verbose)?);
        }
    };
    Ok(())
}

pub fn extract_object(rev: &RevSpec, file_path: &Path) -> Result<()> {

    let fs_transfer = find_fs_transfer()?;
    let (hash, _, _) = fs_transfer.lookup(rev)?;
    fs_transfer.extract_object(&hash, &file_path)
}

pub fn cache_status(file_path: PathBuf) -> Result<()> {
    let cache = AllCaches::new();
    let cache_status = cache.status(&file_path, &file_path.metadata()?)?;
    println!("{} {}", cache_status, file_path.display());
    Ok(())
}

pub fn status(show_ignored: bool,
              rev1: Option<RevSpec>,
              rev2: Option<RevSpec>)
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

pub fn log(hash_only: bool) -> Result<()> {
    let work_dir = find_work_dir()?;
    work_dir.log(hash_only)
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

pub fn branch_set(branch_name: RevNameBuf, target: RevSpec) -> Result<()> {
    let mut object_store = find_object_store()?;
    let (_, commit, _) = object_store.lookup(&target)?;
    object_store.update_ref(branch_name, commit)
}

pub fn branch_set_to_head(branch_name: RevNameBuf) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    work_dir.update_ref_to_head(branch_name.clone())?;
    work_dir.checkout(&branch_name.parse()?)?;
    Ok(())
}

pub fn show_ref() -> Result<()> {
    let object_store = find_object_store()?;
    for (name, hash) in object_store.refs() {
        println!("{:x} {}", hash, name);
    }
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

pub fn checkout(target: &RevSpec) -> Result<()> {
    let mut work_dir = find_work_dir()?;
    work_dir.checkout(target)
}

pub fn merge_base<'a, I: 'a>(revs: I) -> Result<()>
    where I: Iterator<Item = &'a str>
{
    let object_store = find_object_store()?;
    let ancestor = object_store.find_common_ancestor(revs)?;
    if let Some(hash) = ancestor {
        println!("{}", hash);
    }
    Ok(())
}

pub fn merge<'a, I: 'a>(revs: I) -> Result<()>
    where I: Iterator<Item = &'a RevSpec>
{
    let mut work_dir = find_work_dir()?;
    work_dir.merge(revs)
}
