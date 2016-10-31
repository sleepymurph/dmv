extern crate clap;
extern crate prototype;

use std::io::Write;

use clap::App;

use prototype::*;

fn main() {
    let app_m = App::new("Store Prototype")
        .subcommand(addfile::subcommand())
        .get_matches();

    match app_m.subcommand() {
        ("commit", Some(sub_m)) => addfile::subcommand_match(sub_m),
        _ => {
            writeln!(std::io::stderr(), "{}", app_m.usage()).unwrap();
        }
    }
}
