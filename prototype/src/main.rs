#[macro_use]
extern crate clap;
#[macro_use]
extern crate error_chain;
extern crate env_logger;
extern crate prototype;

use prototype::cmd;
use prototype::constants::*;
use prototype::error::*;
use std::path::PathBuf;

// Have error_chain create a main() function that handles Results
quick_main!(run);

fn run() -> Result<()> {
    env_logger::init().unwrap();

    let argmatch = clap_app!(
        (crate_name!()) =>
            (author: crate_authors!())
            (version: format!("{} ({})",
                        crate_version!(), PROJECT_GIT_LOG).as_str())
            (about: crate_description!())
        )
        .subcommand(clap_app!(init =>
                (about: "initialize repository")
        ))
        .subcommand(clap_app!(
            ("hash-object") =>
                (about: "store a file or directory in the object store")
                (@arg filepath: +required)
        ))
        .subcommand(clap_app!(
            ("show-object") =>
                (about: "print information about an object")
                (@arg obj: +required)
        ))
        .subcommand(clap_app!(
            parents =>
                (about: "show current parent commits")
        ))
        .subcommand(clap_app!(
            ("ls-files") =>
                (about: "list files")
                (@arg verbose: -v "include additional information")
                (@arg obj:)
        ))
        .subcommand(clap_app!(
            ("extract-object") =>
                (about: "extract a file or tree")
                (@arg obj: +required)
                (@arg filepath: +required)
        ))
        .subcommand(clap_app!(
            ("cache-status") =>
                (about: "show cache status of a file")
                (@arg filepath: +required)
        ))
        .subcommand(clap_app!(
            status =>
                (about: "show status of files")
                (@arg ignored: -i --ignored "show ignored files")
                (@arg rev1:)
                (@arg rev2:)
        ))
        .subcommand(clap_app!(
            commit =>
                (about: "commit current files to the repository")
                (@arg message: -m <MESSAGE> +required)
        ))
        .subcommand(clap_app!(
            log =>
                (about: "show commit history")
        ))
        .subcommand(clap_app!(
            branch =>
                (about: "show/update branch information")
                (@arg branch:)
                (@arg rev:)
        ))
        .subcommand(clap_app!(
            fsck =>
                (about: "verify repository integrity")
        ))
        .subcommand(clap_app!(
            checkout =>
                (about: "check out another revision")
                (@arg rev:)
        ))
        .get_matches();

    match argmatch.subcommand_name() {
        Some(name) => {
            // Match on subcommand and delegate to a subcommand handler function
            let subfn = match name {
                "init" => cmd_init,
                "hash-object" => cmd_hash_object,
                "show-object" => cmd_show_object,
                "parents" => cmd_parents,
                "ls-files" => cmd_ls_files,
                "extract-object" => cmd_extract_object,
                "cache-status" => cmd_cache_status,
                "status" => cmd_status,
                "commit" => cmd_commit,
                "log" => cmd_log,
                "branch" => cmd_branch,
                "fsck" => cmd_fsck,
                "checkout" => cmd_checkout,
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
    let obj_spec = submatch.value_of("obj").expect("required");
    cmd::show_object(&obj_spec)
}

fn cmd_parents(_argmatch: &clap::ArgMatches,
               _submatch: &clap::ArgMatches)
               -> Result<()> {
    cmd::parents()
}

fn cmd_ls_files(_argmatch: &clap::ArgMatches,
                submatch: &clap::ArgMatches)
                -> Result<()> {
    let obj_spec = submatch.value_of("obj");
    let verbose = submatch.is_present("verbose");

    cmd::ls_files(obj_spec, verbose)
}

fn cmd_extract_object(_argmatch: &clap::ArgMatches,
                      submatch: &clap::ArgMatches)
                      -> Result<()> {
    let obj_spec = submatch.value_of("obj").expect("required");
    let file_path = submatch.value_of("filepath").expect("required");
    let file_path = PathBuf::from(file_path);

    cmd::extract_object(obj_spec, &file_path)
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
    let rev1 = submatch.value_of("rev1");
    let rev2 = submatch.value_of("rev2");
    cmd::status(show_ignored, rev1, rev2)
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
    let branch_name = submatch.value_of("branch");
    let target_rev = submatch.value_of("rev");
    match (branch_name, target_rev) {
        (None, None) => cmd::branch_list(),
        (Some(branch_name), None) => cmd::branch_set_to_head(branch_name),
        (Some(branch_name), Some(target)) => {
            cmd::branch_set(branch_name, target)
        }
        (None, Some(_)) => unreachable!(),
    }
}

fn cmd_fsck(_argmatch: &clap::ArgMatches,
            _submatch: &clap::ArgMatches)
            -> Result<()> {
    cmd::fsck()
}

fn cmd_checkout(_argmatch: &clap::ArgMatches,
                submatch: &clap::ArgMatches)
                -> Result<()> {
    let target = submatch.value_of("rev").expect("required");
    cmd::checkout(target)
}
