//! Library for master's thesis prototype

extern crate byteorder;
#[macro_use]
extern crate error_chain;
extern crate rustc_serialize;
extern crate walkdir;

pub mod error;
#[macro_use]
pub mod wrapperstruct;
pub mod humanreadable;
pub mod encodable;
pub mod constants;
pub mod rollinghash;
pub mod testutil;
pub mod dag;
pub mod objectstore;
pub mod fsutil;
pub mod cache;
pub mod status;
pub mod workdir;
pub mod pipeline;
pub mod cmd;
