#[macro_use]
extern crate clap;

extern crate prototypelib;

use prototypelib::cache;
use prototypelib::dag;
use prototypelib::humanreadable;
use prototypelib::workdir;
use std::env;
use std::io;
use std::path;

fn main() {

    let arg_yaml = load_yaml!("cli.yaml");
    let argmatch = clap::App::from_yaml(arg_yaml).get_matches();

    match argmatch.subcommand_name() {
        Some(name) => {
            // Match on subcommand and delegate to a subcommand handler function
            let subfn = match name {
                "init" => cmd_init,
                "hash-object" => cmd_hash_object,
                "show-object" => cmd_show_object,
                "cache-status" => cmd_cache_status,
                _ => unimplemented!(),
            };
            let submatch = argmatch.subcommand_matches(name).unwrap();
            subfn(&argmatch, submatch);
        }
        None => unimplemented!(),
    }
}

fn cmd_init(_argmatch: &clap::ArgMatches, _submatch: &clap::ArgMatches) {
    let current_dir = env::current_dir().expect("current dir");
    workdir::WorkDir::init(current_dir).expect("initialize");
}

fn cmd_hash_object(_argmatch: &clap::ArgMatches, submatch: &clap::ArgMatches) {
    let filepath = path::Path::new(submatch.value_of("filepath").unwrap());

    let mut wd = find_workdir();
    let hash;
    if filepath.is_file() {
        hash = wd.objectstore.store_file_with_caching(filepath).unwrap();
    } else if filepath.is_dir() {
        hash = wd.objectstore.store_directory(&filepath).unwrap();
    } else {
        unimplemented!()
    }
    println!("{} {}", hash, filepath.display());
}

fn cmd_show_object(_argmatch: &clap::ArgMatches, submatch: &clap::ArgMatches) {
    use prototypelib::dag::Object;

    let hash = dag::ObjectKey::from_hex(submatch.value_of("hash").unwrap());
    let hash = hash.expect("parse key");

    let wd = find_workdir();

    if !wd.objectstore.has_object(&hash) {
        println!("No such object");
    } else {
        let mut reader = io::BufReader::new(wd.objectstore
            .read_object(&hash)
            .expect("read object"));
        let header = dag::ObjectHeader::read_from(&mut reader)
            .expect("read header");

        match header.object_type {
            dag::ObjectType::Blob => {
                format!("Blob, size: {}",
                        humanreadable::human_bytes(header.content_size));
            }
            dag::ObjectType::ChunkedBlob => {
                let obj = dag::ChunkedBlob::read_from(&mut reader)
                    .expect("read");
                print!("{}", obj.pretty_print());
            }
            dag::ObjectType::Tree => {
                let obj = dag::Tree::read_from(&mut reader).expect("read");
                print!("{}", obj.pretty_print());
            }
            dag::ObjectType::Commit => {
                let obj = dag::Commit::read_from(&mut reader).expect("read");
                print!("{}", obj.pretty_print());
            }
        }
    }
}

fn cmd_cache_status(_argmatch: &clap::ArgMatches,
                    submatch: &clap::ArgMatches) {

    let file_path = path::Path::new(submatch.value_of("filepath").unwrap());

    let (cache_status, _cache, _basename, _file_stats) =
        cache::HashCacheFile::open_and_check_file(file_path)
            .expect("could not check file cache status");

    println!("{:?}", cache_status);
}

fn find_workdir() -> workdir::WorkDir {
    workdir::WorkDir::find_from_current_dir().expect("load")
}
