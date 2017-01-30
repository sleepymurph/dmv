//! High-level commands

use cache;
use dag;
use error::*;
use objectstore;
use std::io;
use std::path;

pub fn init(repo_path: path::PathBuf) -> Result<()> {
    try!(objectstore::ObjectStore::init(repo_path));
    Ok(())
}

pub fn hash_object(repo_path: path::PathBuf,
                   file_path: path::PathBuf)
                   -> Result<()> {

    let mut objectstore = try!(objectstore::ObjectStore::open(repo_path));

    let hash;
    if file_path.is_file() {
        hash = try!(objectstore.store_file_with_caching(&file_path))
    } else if file_path.is_dir() {
        hash = try!(objectstore.store_directory(&file_path))
    } else {
        unimplemented!()
    };

    println!("{} {}", hash, file_path.display());
    Ok(())
}

pub fn show_object(repo_path: path::PathBuf, hash: &str) -> Result<()> {
    let hash = dag::ObjectKey::from_hex(hash).expect("parse key");

    let objectstore = try!(objectstore::ObjectStore::open(repo_path));

    if !objectstore.has_object(&hash) {
        println!("No such object");
    } else {
        let mut reader = io::BufReader::new(objectstore.open_object_file(&hash)
            .expect("read object"));
        let header = dag::ObjectHeader::read_from(&mut reader)
            .expect("read header");
        match header.object_type {
            dag::ObjectType::Blob => {
                println!("{}", header);
            }
            _ => {
                let object = header.read_content(&mut reader)
                    .expect("read content");
                println!("{}", object.pretty_print());
            }
        }
    }
    Ok(())
}

pub fn cache_status(file_path: path::PathBuf) -> Result<()> {
    let (cache_status, _cache, _basename, _file_stats) =
        cache::HashCacheFile::open_and_check_file(&file_path)
            .expect("could not check file cache status");

    println!("{:?}", cache_status);
    Ok(())
}
