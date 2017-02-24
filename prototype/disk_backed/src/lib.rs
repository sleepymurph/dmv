//! A container for data that is backed by file on disk
//!
//! This crate provides the DiskBacked type, a container for data that is read
//! from disk on initialization and flushed on drop.
//!
//! ```
//! # extern crate tempdir;
//! # extern crate disk_backed;
//! # use disk_backed::DiskBacked;
//! # fn main() {
//! let temp = tempdir::TempDir::new("test_disk_backed").unwrap();
//! let path = temp.path().join("backing_file");
//!
//! {
//!     let backed_str = DiskBacked::init("backed string",
//!                                       path.to_owned(),
//!                                       "hello world!".to_owned());
//! }
//! assert!(path.is_file(), "will write backing file on drop");
//! {
//!     let backed_str = DiskBacked::<String>::read("backed string",
//!                                                 path.to_owned());
//!     assert_eq!(backed_str.unwrap(),
//!                "hello world!",
//!                "will read on initialization");
//! }
//! # }
//! ```
//!

#[macro_use]
extern crate log;
extern crate rustc_serialize;

#[cfg(test)]
extern crate tempdir;

use rustc_serialize::Decodable;
use rustc_serialize::Encodable;
use rustc_serialize::json;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Read;
use std::io::Write;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;

/// Simple enum for operations, to give more context in error messages
#[derive(Debug,Clone,Copy)]
enum Op {
    Read,
    Write,
}

/// Custom error type
#[derive(Debug)]
pub struct DiskBackError {
    during: Op,
    data_desc: String,
    path: PathBuf,
    cause: Box<Error + Send + Sync>,
}

impl DiskBackError {
    fn new<E>(during: Op, data_desc: &str, path: &Path, cause: E) -> Self
        where E: Into<Box<Error + Send + Sync>>
    {
        DiskBackError {
            during: during,
            data_desc: data_desc.to_owned(),
            path: path.to_owned(),
            cause: cause.into(),
        }
    }
}

impl fmt::Display for DiskBackError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Error while {} {} ({}): {}",
               match self.during {
                   Op::Read => "reading",
                   Op::Write => "writing",
               },
               self.data_desc,
               self.path.display(),
               self.cause)
    }
}

impl Error for DiskBackError {
    fn description(&self) -> &str { "error read/writing DiskBacked data" }
    fn cause(&self) -> Option<&Error> { Some(&*self.cause) }
}

/// Custom result type
type Result<T> = ::std::result::Result<T, DiskBackError>;

/// Convenience function to write serializable data
fn write<T>(desc: &str, path: &Path, data: &T) -> Result<()>
    where T: Encodable
{
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .and_then(|mut file| writeln!(file, "{}", json::as_pretty_json(data)))
        .map_err(|e| DiskBackError::new(Op::Write, desc, path, e))
}

/// Convenience function to read serialized data
fn read<T>(desc: &str, path: &Path) -> Result<T>
    where T: Decodable
{
    OpenOptions::new()
        .read(true)
        .open(path)
        .and_then(|mut file| {
            let mut json = String::new();
            file.read_to_string(&mut json)
                .and(Ok(json))
        })
        .map_err(|e| DiskBackError::new(Op::Read, desc, path, e))
        .and_then(|json| {
            json::decode::<T>(&json)
                .map_err(|e| DiskBackError::new(Op::Read, desc, path, e))
        })
}

/// Convenience method to hash hashable data
fn hash<T>(data: &T) -> u64
    where T: Hash
{
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

/// A container for serializable data that is backed by a file on disk
///
/// The data inside a DiskBacked container is read from the file on
/// initialization and flushed back the file on drop.
///
/// It hashes the data to detect changes, and will only flush to disk if the
/// data has changed.
///
/// There are several ways to construct a DiskBacked:
///
/// - `new()` uses the inner type's Default
/// - `init()` uses a provided value
/// - `read()` reads the file from disk
/// - `read_or()` reads the file from disk if it exists, or uses the provided
/// value
/// - `read_or_default()` reads the file from disk if it exists, or uses the
/// type's Default
///
/// Each constructor takes a `desc` argument. This should be a short
/// human-readable description of what the data is. It will be used in log and
/// error messages, in a form like `format!("Writing {}: {}", desc, path)`.
///
/// The inner data can be accessed with Deref and DerefMut.
///
/// Data is written to the given path automatically on Drop. It can also be
/// written explicitly:
///
/// - `flush()` will write the data if it has changed
/// - `write()` will write the data whether it has changed or not
///
/// Data will only be written if it has been updated since last read. Updates
/// are detected by taking a hash of the data. This is why the inner data is
/// required to implement Hash.
///
pub struct DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    desc: String,
    path: PathBuf,
    data: T,
    disk_hash: u64,
}

impl<T> DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    fn construct(desc: &str, path: PathBuf, data: T) -> Self {
        DiskBacked {
            desc: desc.to_owned(),
            path: path,
            disk_hash: hash(&data),
            data: data,
        }
    }

    /// Initialize with given data
    pub fn init(desc: &str, path: PathBuf, data: T) -> Self {
        DiskBacked {
            desc: desc.to_owned(),
            path: path,
            disk_hash: hash(&data) + 1, // Ensure dirty state
            data: data,
        }
    }

    /// Initialize from disk, or return Err if the file does not exist
    pub fn read(desc: &str, path: PathBuf) -> Result<Self> {
        debug!("Reading   {}: {}", desc, path.display());
        let data: T = read(&desc, &path)?;
        Ok(DiskBacked::construct(desc, path, data))
    }

    /// Initialize from disk, or use the given data if the file does not exist
    pub fn read_or(desc: &str, path: PathBuf, data: T) -> Result<Self> {
        match path.exists() {
            true => DiskBacked::read(desc, path),
            false => Ok(DiskBacked::init(desc, path, data)),
        }
    }

    /// Flush the data to disk, if it hash been updated
    pub fn flush(&mut self) -> Result<()> {
        let new_hash = hash(&self.data);
        if new_hash != self.disk_hash {
            debug!("Flushing  {}: {}", self.desc, self.path.display());
            write(&self.desc, &self.path, &self.data)?;
            self.disk_hash = new_hash;
        } else {
            trace!("Unchanged {}: {}", self.desc, self.path.display());
        }
        Ok(())
    }

    /// Write the data to disk, whether it has been updated or not
    pub fn write(&mut self) -> Result<()> {
        let new_hash = hash(&self.data);
        debug!("Writing   {}: {}", self.desc, self.path.display());
        write(&self.desc, &self.path, &self.data)?;
        self.disk_hash = new_hash;
        Ok(())
    }
}

impl<T> DiskBacked<T>
    where T: Encodable + Decodable + Hash + Default
{
    /// Initialize with the inner type's Default value
    pub fn new(desc: &str, path: PathBuf) -> Self {
        DiskBacked::construct(desc, path, T::default())
    }

    /// Initialize from disk, or use the Default if the file does not exist
    pub fn read_or_default(desc: &str, path: PathBuf) -> Result<Self> {
        match path.exists() {
            true => DiskBacked::read(desc, path),
            false => Ok(DiskBacked::new(desc, path)),
        }
    }
}

impl<T> Drop for DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    fn drop(&mut self) {
        self.flush().unwrap_or_else(|e| {
            error!("Could not flush {} on drop: {}", self.desc, e)
        })
    }
}

impl<T> Deref for DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    type Target = T;
    fn deref(&self) -> &T { &self.data }
}

impl<T> DerefMut for DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    fn deref_mut(&mut self) -> &mut T { &mut self.data }
}

impl<T, U> PartialEq<U> for DiskBacked<T>
    where T: Encodable + Decodable + Hash + PartialEq<U>
{
    fn eq(&self, other: &U) -> bool { self.data.eq(other) }
}

impl<T> fmt::Debug for DiskBacked<T>
    where T: Encodable + Decodable + Hash + fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("DiskBacked")
            .field("desc", &self.desc)
            .field("path", &self.path)
            .field("disk_hash", &self.disk_hash)
            .field("current_hash", &hash(&self.data))
            .field("data", &self.data)
            .finish()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn test_error_display() {
        let e = DiskBackError::new(Op::Read,
                                   "my cache",
                                   &PathBuf::from("/tmp/some_cache_file"),
                                   "(some cause)");
        let display = format!("{}", e);
        assert_eq!(display,
                   "Error while reading my cache (/tmp/some_cache_file): \
                    (some cause)");
    }

    #[test]
    fn test_backed_default() {
        let temp = TempDir::new("test_disk_backed").unwrap();
        let path = temp.path().join("backing_file");

        {
            let _db = DiskBacked::<String>::new("string", path.to_owned());
            assert!(!path.exists(), "should not write immediately");
        }

        assert!(!path.exists(),
                "should not write if initialized with default");

        {
            let mut db = DiskBacked::<String>::new("string", path.to_owned());
            db.push_str("hello world!");
        }

        assert!(path.is_file(), "should auto-write after change and drop");

        {
            let db = DiskBacked::<String>::read("string", path.to_owned());
            assert_eq!(db.unwrap(),
                       "hello world!",
                       "should read previously written value");
        }
    }

    #[test]
    fn test_backed_init() {
        let temp = TempDir::new("test_disk_backed").unwrap();
        let path = temp.path().join("backing_file");

        {
            let _db = DiskBacked::init("backed string",
                                       path.to_owned(),
                                       "hello world!".to_owned());
            assert!(!path.exists(), "should not write immediately");
        }

        assert!(path.is_file(), "should write when explicitly initialized");

        {
            let db = DiskBacked::<String>::read("string", path.to_owned());
            assert_eq!(db.unwrap(),
                       "hello world!",
                       "should read previously written value");
        }
    }

    #[test]
    fn test_read_or_default() {
        let temp = TempDir::new("test_disk_backed").unwrap();
        let path = temp.path().join("backing_file");

        {
            let db = DiskBacked::<String>::read("string", path.to_owned());
            assert!(db.is_err(), "should give error on read if no file");
        }
        {
            let db = DiskBacked::<String>::read_or_default("string",
                                                           path.to_owned());
            assert_eq!(db.unwrap(),
                       String::default(),
                       "should use default value when file does not exist");
        }
        assert!(!path.exists(), "should not write when using default");
        {
            let db = DiskBacked::init("string",
                                      path.to_owned(),
                                      "provided value".to_owned());
            assert_eq!(db,
                       "provided value",
                       "should use file value when file is present");
        }
        {
            let db = DiskBacked::<String>::read_or_default("string",
                                                           path.to_owned());
            assert_eq!(db.unwrap(),
                       "provided value",
                       "should use file value when file is present");
        }
    }

    #[test]
    fn test_read_or() {
        let temp = TempDir::new("test_disk_backed").unwrap();
        let path = temp.path().join("backing_file");

        {
            let db = DiskBacked::<String>::read("string", path.to_owned());
            assert!(db.is_err(), "should give error on read if no file");
        }
        {
            let db = DiskBacked::read_or("string",
                                         path.to_owned(),
                                         "provided value".to_owned());
            assert_eq!(db.unwrap(),
                       "provided value",
                       "should use provided value when file does not exist");
        }
        assert!(path.is_file(), "should write when explicitly initialized");
        {
            let db = DiskBacked::read_or("string",
                                         path.to_owned(),
                                         "an new value not on disk".to_owned());
            assert_eq!(db.unwrap(),
                       "provided value",
                       "should use file value when file is present");
        }
    }

    #[test]
    fn test_bad_json_error() {
        let temp = TempDir::new("test_disk_backed").unwrap();
        let path = temp.path().join("backing_file");
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            write!(file, "}} bad json! {{").unwrap();
        }
        let db = DiskBacked::<String>::read("string", path.to_owned());
        assert!(db.is_err(), "should give error on read if corrupt file");
        // panic!(format!("{}", db.err().unwrap()));
    }
}
