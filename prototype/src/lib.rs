extern crate clap;
extern crate crypto;

use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use clap::{App, Arg, ArgMatches, SubCommand};
use crypto::digest::Digest;
use crypto::sha1::Sha1;

pub trait Repository {
    fn hash_object(&self, path: &Path) -> Result<String, std::io::Error>;
}

pub struct OnDiskRepository;

impl Repository for OnDiskRepository {
    fn hash_object(&self, path: &Path) -> Result<String, std::io::Error> {

        let mut file = try!(File::open(&path));

        let mut buffer: [u8; 1024] = [0; 1024];
        let mut digest = Sha1::new();

        loop {
            match file.read(&mut buffer) {
                Err(why) => {
                    panic!("Error reading {}: {}",
                           path.display(),
                           why.description())
                }
                Ok(0) => break,
                Ok(bytes) => {
                    digest.input(&buffer[0..bytes]);
                }
            }
        }
        Ok(digest.result_str())
    }
}

pub trait PrototypeCommand {
    fn name() -> &'static str;
    fn subcommand<'a, 'b>() -> App<'a, 'b>;
    fn subcommand_match(matches: &ArgMatches);
}

pub struct HashObjectCommand;

impl PrototypeCommand for HashObjectCommand {
    fn name() -> &'static str {
        "hash-object"
    }

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("hash-object").arg(Arg::with_name("file"))
    }

    fn subcommand_match(matches: &ArgMatches) {
        if let Some(filename) = matches.value_of("file") {
            let repo = OnDiskRepository {};
            let path = Path::new(filename);
            match repo.hash_object(path) {
                Ok(hash) => println!("{}", hash),
                Err(why) => {
                    panic!("Could not open {}: {}", filename, why.description())
                }
            }
        } else {
            println!("no filename given");
        }
    }
}
