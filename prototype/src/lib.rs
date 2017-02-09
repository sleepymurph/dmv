//! Library for master's thesis prototype

// Dependencies
extern crate byteorder;
extern crate crypto;
#[macro_use]
extern crate error_chain;
extern crate humanreadable;
#[macro_use]
extern crate log;
extern crate rustc_serialize;
#[macro_use]
extern crate wrapperstruct;

// Test-only dependencies
#[cfg(test)]
#[macro_use]
extern crate testutil;


// Low-level code that isn't specific to the project.
// Could potentially be spun off into their own crates.
pub mod encodable;
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
