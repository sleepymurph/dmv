extern crate clap;
extern crate prototype;

use std::io::Write;

use clap::App;

use prototype::*;

fn main() {
    let app_m = App::new("Store Prototype")
        .subcommand(InitCommand::subcommand())
        .subcommand(HashObjectCommand::subcommand())
        .get_matches();

    match app_m.subcommand() {
        ("init", Some(sub_m)) => {
            InitCommand::subcommand_match(sub_m)
        }
        ("hash-object", Some(sub_m)) => {
            HashObjectCommand::subcommand_match(sub_m)
        }
        _ => {
            writeln!(std::io::stderr(), "{}", app_m.usage()).unwrap();
        }
    }
}
