//! High-level commands

use cache::AllCaches;
use dag::ObjectCommon;
use dag::ObjectHeader;
use dag::ObjectKey;
use dag::ObjectType;
use error::*;
use objectstore::ObjectStore;
use pipeline;
use std::io::BufReader;
use std::path::PathBuf;

pub fn init(repo_path: PathBuf) -> Result<()> {
    try!(ObjectStore::init(repo_path));
    Ok(())
}

pub fn hash_object(repo_path: PathBuf, file_path: PathBuf) -> Result<()> {

    let mut object_store = try!(ObjectStore::open(repo_path));
    let mut cache = AllCaches::new();

    let hash;
    if file_path.is_file() {
        hash = try!(pipeline::hash_file(file_path.clone(),
                                        &mut cache,
                                        &mut object_store));
    } else if file_path.is_dir() {
        unimplemented!()
    } else {
        unimplemented!()
    };

    println!("{} {}", hash, file_path.display());
    Ok(())
}

pub fn show_object(repo_path: PathBuf, hash: &str) -> Result<()> {

    let hash = try!(ObjectKey::from_hex(hash));
    let object_store = try!(ObjectStore::open(repo_path));

    if !object_store.has_object(&hash) {
        println!("No such object");
    } else {
        let reader = try!(object_store.open_object_file(&hash));
        let mut reader = BufReader::new(reader);

        let header = try!(ObjectHeader::read_from(&mut reader));
        match header.object_type {
            ObjectType::Blob => {
                println!("{}", header);
            }
            _ => {
                let object = try!(header.read_content(&mut reader));
                println!("{}", object.pretty_print());
            }
        }
    }
    Ok(())
}

pub fn cache_status(file_path: PathBuf) -> Result<()> {
    let mut cache = AllCaches::new();
    let cache_status = try!(cache.check(&file_path));
    println!("{:?}", cache_status);
    Ok(())
}
