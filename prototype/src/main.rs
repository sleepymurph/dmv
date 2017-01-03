#[macro_use]
extern crate clap;

extern crate prototypelib;

use std::env;
use std::path;
use prototypelib::workdir;

fn main() {

    let arg_yaml = load_yaml!("cli.yaml");
    let argmatch = clap::App::from_yaml(arg_yaml).get_matches();

    match argmatch.subcommand_name() {
        Some(name) => {
            // Match on subcommand and delegate to a subcommand handler function
            let subfn = match name {
                "init" => cmd_init,
                "hash-object" => cmd_hash_object,
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

    let current_dir = env::current_dir().expect("current dir");
    let mut wd = workdir::WorkDir::load(current_dir).expect("load");
    let objectkey = wd.objectstore.store_file(filepath).expect("store");
    println!("{} {}", objectkey, filepath.display());
}
