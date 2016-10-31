extern crate clap;
extern crate crypto;

pub mod addfile {

    use std::error::Error;
    use std::fs::File;
    use std::io::Read;
    use std::path::Path;

    use clap::{App, Arg, ArgMatches, SubCommand};
    use crypto::digest::Digest;
    use crypto::sha1::Sha1;

    pub fn subcommand<'a>() -> App<'a, 'a> {
        SubCommand::with_name("commit").arg(Arg::with_name("file"))
    }

    pub fn subcommand_match(matches: &ArgMatches) {
        println!("commit!");
        if let Some(filename) = matches.value_of("file") {
            println!("filename: {}", filename);
            let path = Path::new(filename);
            let mut file = match File::open(&path) {
                Err(why) => {
                    panic!("Couldn't open {}: {}",
                           path.display(),
                           why.description())
                }
                Ok(file) => file,
            };

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
            println!("{}", digest.result_str());
        } else {
            println!("no filename given");
        }
    }
}
