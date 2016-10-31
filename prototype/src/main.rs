extern crate clap;
extern crate prototype;

use std::io::Write;

use clap::App;

use prototype::*;

fn main() {
    let app_m = App::new("Store Prototype")
        .subcommand(hash_object::subcommand())
        .get_matches();

    match app_m.subcommand() {
        ("hash-object", Some(sub_m)) => hash_object::subcommand_match(sub_m),
        _ => {
            writeln!(std::io::stderr(), "{}", app_m.usage()).unwrap();
        }
    }
}
