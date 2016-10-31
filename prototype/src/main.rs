use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

extern crate clap;
use clap::{App, Arg, SubCommand};

extern crate crypto;
use crypto::digest::Digest;
use crypto::sha1::Sha1;

fn main() {
    let matches = App::new("Store Prototype")
        .subcommand(SubCommand::with_name("commit")
                    .arg(Arg::with_name("file"))
                    )
        .get_matches();

    if let Some(matches) = matches.subcommand_matches("commit") {
        println!("commit!");
        if let Some(filename) = matches.value_of("file") {
            println!("filename: {}", filename);
            let path = Path::new(filename);
            let mut file = match File::open(&path) {
                Err(why) => panic!("Couldn't open {}: {}",
                                   path.display(), why.description()),
                Ok(file) => file,
            };

            let mut buffer: [u8; 1024] = [0; 1024];
            let mut digest = Sha1::new();
            loop {
                match file.read(&mut buffer) {
                    Err(why) => panic!("Error reading {}: {}",
                                       path.display(), why.description()),
                    Ok(0) => break,
                    Ok(bytes) => {
                        digest.input(&buffer[0..bytes]);
                    }
                }
            };
            println!("{}", digest.result_str());
        } else {
            println!("no filename given");
        };
    } else {
        println!("not commit.");
    };

}

#[test]
fn test_sha1_empty() {
    let mut digest = Sha1::new();
    digest.input_str("");
    let outstr = digest.result_str();
    assert!(outstr == "da39a3ee5e6b4b0d3255bfef95601890afd80709");
}
