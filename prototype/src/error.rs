//! Error types for the project
//!
//! This project uses the [error-chain crate](
//! https://crates.io/crates/error-chain), and follows its conventions.

error_chain!{
    foreign_links {
        IoError(::std::io::Error)
            #[doc = "Error caused by an underlying IO error"];
        StripPrefixError(::std::path::StripPrefixError)
            #[doc = "An error during path manipulation"];
        FromUtf8Error(::std::string::FromUtf8Error)
            #[doc = "Error converting bytes to a String"];
        JsonDecodeError(::rustc_serialize::json::DecoderError)
            #[doc = "Error while decoding json"];
        DiskBackError(::disk_backed::DiskBackError)
            #[doc = "Error from disk-backed data"];
    }
    errors {
        BadRevSpec(bad: String) {
            description("could not parse revision")
            display("could not parse revision: '{}'", bad)
        }
        ParseKey(bad_key: String) {
            description("could not parse hash key")
            display("could not parse hash key: '{}'", bad_key)
        }
        BadKeyLength(bad_key: Vec<u8>) {
            description("hash key has wrong length")
            display("hash key has wrong length ({} bytes, expected {}): '{:?}'",
                        bad_key.len(), ::dag::KEY_SIZE_BYTES, bad_key)
        }
        BadObjectHeader(msg: String) {
            description("bad object header")
            display("could not object header: {}", msg)
        }

        ObjectNotFound(h: $crate::dag::ObjectKey) {
            description("object not found in object store")
            display("object not found in object store: {}", h)
        }

        RevNotFound(r: $crate::object_store::RevSpec) {
            description("revision not found in object store")
            display("revision not found in object store: {}", r)
        }

        RefNotFound(r: String) {
            description("ref not found in repository")
            display("ref not found in repository: {}", r)
        }
    }
}


/// Extensions for Paths that work with these custom errors
pub trait PathExt {
    /// Like `parent()`, but return a Result instead of an Option
    fn parent_or_err(&self) -> Result<&::std::path::Path>;
    /// Like `file_name()`, but return a Result instead of an Option
    fn file_name_or_err(&self) -> Result<&::std::ffi::OsStr>;
}

impl PathExt for ::std::path::Path {
    fn parent_or_err(&self) -> Result<&::std::path::Path> {
        self.parent()
            .ok_or_else(|| {
                format!("path has no parent: \"{}\"", self.display()).into()
            })
    }
    fn file_name_or_err(&self) -> Result<&::std::ffi::OsStr> {
        self.file_name()
            .ok_or_else(|| {
                format!("path has no file_name part: \"{}\"", self.display())
                    .into()
            })
    }
}
