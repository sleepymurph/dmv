//! Library for master's thesis prototype

// error_chain uses macro with a lot of recursion
#![recursion_limit = "1024"]

// Dependencies
extern crate byteorder;
extern crate crypto;
extern crate disk_backed;
#[macro_use]
extern crate error_chain;
extern crate human_readable;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate regex;
extern crate rustc_serialize;
#[macro_use]
extern crate wrapper_struct;

// Test-only dependencies
#[cfg(test)]
#[macro_use]
extern crate testutil;
#[cfg(test)]
#[macro_use]
extern crate hamcrest;


/// Write to stderr
macro_rules! stderrln {
    ( $($arg:expr),* ) => {{
        use ::std::io::Write;
        writeln!(::std::io::stderr(), $($arg),*)
            .expect("could not write to stderr")
    }};
}


// Low-level code that isn't specific to the project.
// Could potentially be spun off into their own crates.
#[macro_use]
pub mod maputil;
pub mod encodable;
pub mod fsutil;
pub mod progress;
pub mod walker;
pub mod variance;

// Project-specific code
pub mod error;
pub mod constants;
#[macro_use]
pub mod dag;
pub mod rolling_hash;
pub mod object_store;
pub mod cache;
pub mod ignore;
pub mod file_store;
pub mod status;
pub mod fs_transfer;
pub mod work_dir;
pub mod find_repo;
pub mod cmd;
