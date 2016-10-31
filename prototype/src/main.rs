extern crate clap;
use clap::{App, Arg, SubCommand};

fn main() {
    let matches = App::new("Store Prototype")
        .subcommand(SubCommand::with_name("commit")
                    .arg(Arg::with_name("file"))
                    )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("commit") {
        println!("commit!");
        if let Some(filename) = matches.value_of("file") {
            println!("filename: {}", filename)
        } else {
            println!("no filename given")
        }
    } else {
        println!("not commit.");
    }

}

#[test]
fn meta_test() {
    assert!(true);
}
