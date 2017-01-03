#[macro_use]
extern crate clap;

fn main() {

    println!("Parsing!");

    let arg_yaml = load_yaml!("cli.yaml");
    let argmatch = clap::App::from_yaml(arg_yaml).get_matches();

    if let Some(submatch) = argmatch.subcommand_matches("hash-object") {
        println!("hash-object! - filepath: {}",
                 submatch.value_of("filepath").unwrap());
    } else {
        println!("unknown!");
    }
}
