//! Library for master's thesis prototype

#![allow(dead_code)]

#[macro_use]
extern crate error_chain;
extern crate rustc_serialize;

pub mod error {
    //! Error types for the project
    //!
    //! This project uses the [error-chain crate](
    //! https://crates.io/crates/error-chain), and follows its conventions.

    error_chain!{
        foreign_links {
            IoError(::std::io::Error);
        }
        errors {
        }
    }

}

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
