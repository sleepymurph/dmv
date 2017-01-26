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
    }
    errors {
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
        DagError(cause: ::dag::DagError)
        PathWithNoParent(p: ::std::path::PathBuf) {
            description("path has no parent")
            display("path has no parent: '{}'", p.display())
        }
        PathWithNoFileName(p: ::std::path::PathBuf) {
            description("path has no file name component")
            display("path has no file name component: '{}'", p.display())
        }
        CorruptCacheFile{
            cache_file: ::std::path::PathBuf,
            cause: ::rustc_serialize::json::DecoderError,
            bad_json: String,
        }
        CacheSerializeError{
            cause: ::rustc_serialize::json::EncoderError,
            bad_cache: ::cache::HashCache,
        }
    }
}

impl From<::dag::DagError> for Error {
    fn from(e: ::dag::DagError) -> Self {
        ErrorKind::DagError(e).into()
    }
}

pub trait ResultInto<T, E> {
    fn err_into(self) -> Result<T>;
}

impl<T, E> ResultInto<T, E> for ::std::result::Result<T, E>
    where E: Into<Error>
{
    fn err_into(self) -> Result<T> {
        self.map_err(|e| e.into())
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
            .ok_or_else(|| ErrorKind::PathWithNoParent(self.to_owned()).into())
    }
    fn file_name_or_err(&self) -> Result<&::std::ffi::OsStr> {
        self.file_name()
            .ok_or_else(|| {
                ErrorKind::PathWithNoFileName(self.to_owned()).into()
            })
    }
}
