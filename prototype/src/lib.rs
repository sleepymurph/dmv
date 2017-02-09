//! Library for master's thesis prototype

// Dependencies
extern crate byteorder;
extern crate crypto;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
extern crate rustc_serialize;

// Dependencies used only in test / testutil
extern crate rand;
extern crate tempdir;


// Low-level code that isn't specific to the project.
// Could potentially be spun off into their own crates.
#[macro_use]
pub mod wrapperstruct;
pub mod humanreadable;
pub mod encodable;
#[macro_use]
pub mod testutil;
pub mod fsutil;

// Project-specific code
pub mod error;
pub mod constants;
#[macro_use]
pub mod dag;
pub mod rollinghash;
pub mod objectstore;
#[macro_use]
pub mod cache;
pub mod ignore;
pub mod pipeline;
pub mod cmd;
