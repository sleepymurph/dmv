#[macro_use]
extern crate clap;

extern crate prototypelib;

fn main() {

    let arg_yaml = load_yaml!("cli.yaml");
    let argmatch = clap::App::from_yaml(arg_yaml).get_matches();

    match argmatch.subcommand_name() {
        Some(name) => {
            // Match on subcommand and delegate to a subcommand handler function
            let subfn = match name {
                "init" => cmd_init,
                "hash-object" => cmd_hash_object,
                _ => unimplemented!()
            };
            let submatch = argmatch.subcommand_matches(name).unwrap();
            subfn(&argmatch, submatch);
        },
        None => unimplemented!(),
    }
}

fn cmd_init(_argmatch: &clap::ArgMatches, _submatch: &clap::ArgMatches) {
    use std::env;
    use prototypelib::workdir;

    let current = env::current_dir().expect("current dir");
    workdir::WorkDir::init(current).expect("initialize");
}

fn cmd_hash_object(_argmatch: &clap::ArgMatches, submatch: &clap::ArgMatches) {
    println!("hash-object! - filepath: {}",
             submatch.value_of("filepath").unwrap());
}
