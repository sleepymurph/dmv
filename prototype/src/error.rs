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

        PathWithNoParent(p: ::std::path::PathBuf) {
            description("path has no parent")
            display("path has no parent: '{}'", p.display())
        }
        PathWithNoFileName(p: ::std::path::PathBuf) {
            description("path has no file name component")
            display("path has no file name component: '{}'", p.display())
        }
        NotADirectory(p: ::std::path::PathBuf) {
            description("path is not a directory")
            display("path is not a directory: '{}'", p.display())
        }

        CorruptCacheFile{
            cache_file: ::std::path::PathBuf,
            cause: ::rustc_serialize::json::DecoderError,
            bad_json: String,
        }

        ObjectNotFound(h: $crate::dag::ObjectKey) {
            description("object not found in object store")
            display("object not found in object store: {}", h)
        }

        RevNotFound(r: $crate::objectstore::RevSpec) {
            description("revision not found in object store")
            display("revision not found in object store: {}", r)
        }

        RefNotFound(r: String) {
            description("ref not found in repository")
            display("ref not found in repository: {}", r)
        }
    }
}

pub trait ResultInto<T, E> {
    fn err_into(self) -> Result<T>;
}

impl<T, E> ResultInto<T, E> for ::std::result::Result<T, E>
    where E: Into<Error>
{
    fn err_into(self) -> Result<T> { self.map_err(|e| e.into()) }
}


type StdResult<T, E> = ::std::result::Result<T, E>;

/// Additional methods for working with a Result<Option<T>,E>
pub trait ResultOptionExt<T, E> {
    /// Simplifies a Result+Option to a Result by turning None into an error
    ///
    /// ```ignore
    /// Ok(Some(x)) => Ok(x),
    /// Ok(None) => Err(gen_err().into()),
    /// Err(e) => Err(e),
    /// ```
    fn err_if_none<O, F>(self, gen_err: O) -> StdResult<T, E>
        where O: FnOnce() -> F,
              F: Into<E>;
}

impl<T, E> ResultOptionExt<T, E> for StdResult<Option<T>, E> {
    fn err_if_none<O, F>(self, gen_err: O) -> StdResult<T, E>
        where O: FnOnce() -> F,
              F: Into<E>
    {
        match self {
            Ok(Some(x)) => Ok(x),
            Ok(None) => Err(gen_err().into()),
            Err(e) => Err(e),
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
            .ok_or_else(|| ErrorKind::PathWithNoParent(self.to_owned()).into())
    }
    fn file_name_or_err(&self) -> Result<&::std::ffi::OsStr> {
        self.file_name()
            .ok_or_else(|| {
                ErrorKind::PathWithNoFileName(self.to_owned()).into()
            })
    }
}
