//! Error types for the project
//!
//! This project uses the [error-chain crate](
//! https://crates.io/crates/error-chain), and follows its conventions.

error_chain!{
    foreign_links {
        BorrowError(::std::cell::BorrowError)
            #[doc = "Error caused by a failed RefCell borrow"];
        BorrowMutError(::std::cell::BorrowMutError)
            #[doc = "Error caused by a failed RefCell borrow_mut"];
        FmtError(::std::fmt::Error)
            #[doc = "Error while formatting"];
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
        BadObjectKey(bad_key: String) {
            description("could not parse hash key")
            display("could not parse hash key: '{}'", bad_key)
        }
        BadObjectHeader(msg: String) {
            description("bad object header")
            display("could not parse object header: {}", msg)
        }

        ObjectNotFound(h: $crate::dag::ObjectKey) {
            description("object not found in object store")
            display("object not found in object store: {}", h)
        }

        RefOrHashNotFound(r: String) {
            description("revision not found in object store")
            display("revision not found in object store: {}", r)
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


/// Additional convenience methods for Options
pub trait OptionExt<T> {
    /// Chain a function that returns a result
    fn and_then_try<U, E, F>(self,
                             f: F)
                             -> ::std::result::Result<Option<U>, E>
        where F: FnOnce(T) -> ::std::result::Result<U, E>;
}

impl<T> OptionExt<T> for Option<T> {
    fn and_then_try<U, E, F>(self, f: F) -> ::std::result::Result<Option<U>, E>
        where F: FnOnce(T) -> ::std::result::Result<U, E>
    {
        match self.map(f) {
            None => Ok(None),
            Some(Ok(t)) => Ok(Some(t)),
            Some(Err(e)) => Err(e),
        }
    }
}
