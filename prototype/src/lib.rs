extern crate clap;
extern crate crypto;

use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;

use clap::{App, Arg, ArgMatches, SubCommand};
use crypto::digest::Digest;
use crypto::sha1::Sha1;

pub trait Repository {
    fn init(&self) -> Result<(), std::io::Error>;
    fn hash_object(&self, path: &Path) -> Result<String, std::io::Error>;
    fn store_object(&self, path: &Path) -> Result<String, std::io::Error>;
}

pub struct OnDiskRepository<'a> {
    path: &'a Path,
}

const BUFSIZE: usize = 1024;

impl<'a> OnDiskRepository<'a> {
    fn new() -> Self {
        let path = Path::new(".prototype");
        OnDiskRepository { path: path }
    }
}

impl<'a> Repository for OnDiskRepository<'a> {
    fn init(&self) -> Result<(), std::io::Error> {
        std::fs::create_dir(self.path)
    }

    fn hash_object(&self, path: &Path) -> Result<String, std::io::Error> {
        let mut file = try!(File::open(&path));
        let mut buffer: [u8; BUFSIZE] = [0; BUFSIZE];
        let mut digest = Sha1::new();

        loop {
            match try!(file.read(&mut buffer)) {
                0 => break,
                bytes => digest.input(&buffer[0..bytes]),
            }
        }
        Ok(digest.result_str())
    }

    fn store_object(&self, path: &Path) -> Result<String, std::io::Error> {
        let mut file = try!(File::open(&path));

        let tmppath = self.path.join("tmp");
        let mut dest = try!(OpenOptions::new()
            .write(true)
            .create(true)
            .open(&tmppath));

        let mut buffer: [u8; BUFSIZE] = [0; BUFSIZE];
        let mut digest = Sha1::new();

        loop {
            match try!(file.read(&mut buffer)) {
                0 => break,
                bytes => {
                    digest.input(&buffer[0..bytes]);
                    try!(dest.write(&buffer[0..bytes]));
                }
            }
        }

        let hash = digest.result_str();
        let hashpath = self.path
            .join("objects")
            .join(&hash[0..2])
            .join(&hash[2..4])
            .join(&hash[4..]);
        try!(std::fs::create_dir_all(hashpath.parent().unwrap()));
        try!(std::fs::rename(tmppath, hashpath));
        Ok(hash)
    }
}

pub trait PrototypeCommand {
    fn name() -> &'static str;
    fn subcommand<'a, 'b>() -> App<'a, 'b>;
    fn subcommand_match(matches: &ArgMatches);
}

pub struct InitCommand;

impl PrototypeCommand for InitCommand {
    fn name() -> &'static str {
        "init"
    }

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("init")
    }

    fn subcommand_match(_matches: &ArgMatches) {
        let repo = OnDiskRepository::new();
        match repo.init() {
            Ok(_) => {}
            Err(why) => panic!("Could not initialize: {}", why.description()),
        }
    }
}

pub struct HashObjectCommand;

impl PrototypeCommand for HashObjectCommand {
    fn name() -> &'static str {
        "hash-object"
    }

    fn subcommand<'a, 'b>() -> App<'a, 'b> {
        SubCommand::with_name("hash-object")
            .arg(Arg::with_name("write")
                .short("w")
                .long("write")
                .help("actually write the object to the repository"))
            .arg(Arg::with_name("file"))
    }

    fn subcommand_match(matches: &ArgMatches) {
        if let Some(filename) = matches.value_of("file") {
            let repo = OnDiskRepository::new();
            let path = Path::new(filename);
            let result = match matches.occurrences_of("write") {
                0 => repo.hash_object(path),
                1 | _ => repo.store_object(path),
            };
            match result {
                Ok(hash) => println!("{}", hash),
                Err(why) => panic!("Error: {}", why.description()),
            }
        } else {
            println!("no filename given");
        }
    }
}
