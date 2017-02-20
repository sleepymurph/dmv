#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate prototype;

use prototype::cmd;
use prototype::constants;
use prototype::error::*;
use prototype::objectstore::RevSpec;
use std::env;
use std::path::PathBuf;
use std::str::FromStr;

// Have error_chain create a main() function that handles Results
quick_main!(run);

fn run() -> Result<()> {
    env_logger::init().unwrap();

    let arg_yaml = load_yaml!("cli.yaml");
    let argmatch = clap::App::from_yaml(arg_yaml).get_matches();

    match argmatch.subcommand_name() {
        Some(name) => {
            // Match on subcommand and delegate to a subcommand handler function
            let subfn = match name {
                "init" => cmd_init,
                "hash-object" => cmd_hash_object,
                "show-object" => cmd_show_object,
                "extract-object" => cmd_extract_object,
                "cache-status" => cmd_cache_status,
                "commit" => cmd_commit,
                _ => unimplemented!(),
            };
            let submatch = argmatch.subcommand_matches(name)
                .expect("just matched");
            subfn(&argmatch, submatch)
        }
        None => unimplemented!(),
    }
}

fn cmd_init(_argmatch: &clap::ArgMatches,
            _submatch: &clap::ArgMatches)
            -> Result<()> {
    let repo_path = repo_path();

    cmd::init(repo_path)
}

fn cmd_hash_object(_argmatch: &clap::ArgMatches,
                   submatch: &clap::ArgMatches)
                   -> Result<()> {
    let repo_path = repo_path();

    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::hash_object(repo_path, file_path)
}

fn cmd_show_object(_argmatch: &clap::ArgMatches,
                   submatch: &clap::ArgMatches)
                   -> Result<()> {
    let repo_path = repo_path();

    let obj_spec = submatch.value_of("obj-spec").expect("required");
    let obj_spec = try!(RevSpec::from_str(obj_spec));

    cmd::show_object(repo_path, &obj_spec)
}

fn cmd_extract_object(_argmatch: &clap::ArgMatches,
                      submatch: &clap::ArgMatches)
                      -> Result<()> {
    let repo_path = repo_path();

    let obj_spec = submatch.value_of("obj-spec").expect("required");
    let obj_spec = try!(RevSpec::from_str(obj_spec));

    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::extract_object(repo_path, &obj_spec, &file_path)
}

fn cmd_cache_status(_argmatch: &clap::ArgMatches,
                    submatch: &clap::ArgMatches)
                    -> Result<()> {
    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::cache_status(file_path)
}

fn cmd_commit(_argmatch: &clap::ArgMatches,
              submatch: &clap::ArgMatches)
              -> Result<()> {

    let repo_path = repo_path();
    let message = submatch.value_of("message").expect("required").to_owned();
    let path = env::current_dir().expect("current dir");
    cmd::commit(repo_path, message, path)
}

fn repo_path() -> PathBuf {
    env::current_dir().expect("current dir").join(constants::HIDDEN_DIR_NAME)
}
