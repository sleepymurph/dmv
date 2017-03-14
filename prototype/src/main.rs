#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate prototype;

use prototype::cmd;
use prototype::error::*;
use prototype::object_store::RevSpec;
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
                "ls-files" => cmd_ls_files,
                "extract-object" => cmd_extract_object,
                "cache-status" => cmd_cache_status,
                "status" => cmd_status,
                "add" => cmd_add,
                "rm" => cmd_rm,
                "commit" => cmd_commit,
                "log" => cmd_log,
                "branch" => cmd_branch,
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
    cmd::init()
}

fn cmd_hash_object(_argmatch: &clap::ArgMatches,
                   submatch: &clap::ArgMatches)
                   -> Result<()> {
    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::hash_object(file_path)
}

fn cmd_show_object(_argmatch: &clap::ArgMatches,
                   submatch: &clap::ArgMatches)
                   -> Result<()> {
    let obj_spec = submatch.value_of("obj-spec").expect("required");
    let obj_spec = try!(RevSpec::from_str(obj_spec));

    cmd::show_object(&obj_spec)
}

fn cmd_ls_files(_argmatch: &clap::ArgMatches,
                submatch: &clap::ArgMatches)
                -> Result<()> {
    let obj_spec = submatch.value_of("obj-spec")
        .and_then_try(|s| RevSpec::from_str(s))?;

    cmd::ls_files(obj_spec)
}

fn cmd_extract_object(_argmatch: &clap::ArgMatches,
                      submatch: &clap::ArgMatches)
                      -> Result<()> {
    let obj_spec = submatch.value_of("obj-spec").expect("required");
    let obj_spec = try!(RevSpec::from_str(obj_spec));

    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::extract_object(&obj_spec, &file_path)
}

fn cmd_cache_status(_argmatch: &clap::ArgMatches,
                    submatch: &clap::ArgMatches)
                    -> Result<()> {
    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::cache_status(file_path)
}

fn cmd_status(_argmatch: &clap::ArgMatches,
              submatch: &clap::ArgMatches)
              -> Result<()> {
    let show_ignored = submatch.is_present("ignored");
    cmd::status(show_ignored)
}

fn cmd_add(_argmatch: &clap::ArgMatches,
           submatch: &clap::ArgMatches)
           -> Result<()> {
    let file_path = submatch.value_of("path").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::add(file_path)
}

fn cmd_rm(_argmatch: &clap::ArgMatches,
          submatch: &clap::ArgMatches)
          -> Result<()> {
    let file_path = submatch.value_of("path").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::rm(file_path)
}

fn cmd_commit(_argmatch: &clap::ArgMatches,
              submatch: &clap::ArgMatches)
              -> Result<()> {
    let message = submatch.value_of("message").expect("required").to_owned();
    cmd::commit(message)
}

fn cmd_log(_argmatch: &clap::ArgMatches,
           _submatch: &clap::ArgMatches)
           -> Result<()> {
    cmd::log()
}

fn cmd_branch(_argmatch: &clap::ArgMatches,
              submatch: &clap::ArgMatches)
              -> Result<()> {
    let branch_name = submatch.value_of("branch-name");
    let target_rev = submatch.value_of("target-rev");
    match (branch_name, target_rev) {
        (None, None) => cmd::branch_list(),
        (Some(branch_name), None) => cmd::branch_set_to_head(branch_name),
        (Some(branch_name), Some(target)) => {
            let target = RevSpec::from_str(target)?;
            cmd::branch_set(branch_name, target)
        }
        (None, Some(_)) => unreachable!(),
    }
}
