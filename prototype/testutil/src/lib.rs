//! Utilities for testing, including randomness and temporary files

pub extern crate rand;
pub extern crate tempdir;
#[macro_use]
extern crate wrapperstruct;

use rand::{Rng, SeedableRng, XorShiftRng};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::path::Path;
use tempdir::TempDir;

/// Test that a value matches a given pattern
///
/// ```
/// #[macro_use]
/// extern crate testutil;
///
/// fn main() {
///     // Simplest form
///     assert_match!(Some("string"), Some(_));
///
///     // Can include a match expression for further tests on the matched value
///     assert_match!(Some("string"), Some(x) => assert!(x.contains("str")));
///     assert_match!(Some("string"), Some(x) => {
///         assert!(x.contains("str"))
///     });
///
///     // Can include match guards as well
///     assert_match!(Some(20), Some(x) if x>10);
///     assert_match!(Some(20), Some(x) if x>10 => {
///         assert!(x < 25);
///     });
/// }
/// ```
///
#[macro_export]
macro_rules! assert_match {
    (@_panic $result:expr, $pat_str:expr) => {
            panic!(
        "
                Expected `{}` to match `{}`,
                Was: {:?}
        ",
                stringify!($result),
                $pat_str,
                $result)
    };
    ($result:expr, $pat:pat) => {
        assert_match!($result, $pat => {});
    };
    ($result:expr, $pat:pat => $block:expr) => {
        match $result {
            $pat => $block,
            _ => assert_match!(@_panic $result, stringify!($pat)),
        };
    };
    ($result:expr, $pat:pat if $guard:expr ) => {
        assert_match!($result, $pat if $guard => {});
    };
    ($result:expr, $pat:pat if $guard:expr => $block:expr ) => {
        match $result {
            $pat if $guard => $block,
            _ => assert_match!(@_panic $result, stringify!($pat if $guard)),
        };
    };
}


/// Test that a Result is an Err
///
/// ```
/// #[macro_use]
/// extern crate testutil;
///
/// fn main() {
///     // Simple error test
///     let err:Result<(), ()> = Err(());
///     assert_err!(err);
///
///     // Test that the error's Display/Debug/description includes some text
///     let err:Result<(), &str> = Err("Example error");
///     assert_err!(err, "Example");
/// }
/// ```
///
#[macro_export]
macro_rules! assert_err {
    ($result:expr) => {
        assert!($result.is_err());
    };
    ($result:expr, $expected:expr) => {
        match $result {
            Ok(_) => panic!("Expected error, but was Ok"),
            Err(e) => {
                let e_debug = format!("{:?}", e);
                let e_display = format!("{}", e);
                let e:Box<::std::error::Error> = e.into();

                let mut found = false;
                let mut next = Some(&*e);
                while let Some(e) = next {
                    found = e.description().contains($expected) ||
                            format!("{}",e).contains($expected) ||
                            format!("{:?}",e).contains($expected);
                    if found { break; }
                    next = e.cause();
                };
                if !found {
                    panic!(
        "
                Expected error with message containing: \"{}\"
                Error Description: {:?},
                Error Debug: {},
                Error Display: {:?}
        ",
                        $expected,
                        e.description(),
                        e_debug,
                        e_display);
                }
            }
        }
    }
}


#[test]
fn test_assert_err_walk_chain() {
    use std::error::Error;
    use std::fmt;

    #[derive(Debug)]
    struct WrappedError {
        detail: String,
        cause: Box<Error>,
    }
    impl Error for WrappedError {
        fn description(&self) -> &str { "wrapped error" }
        fn cause(&self) -> Option<&Error> { Some(self.cause.as_ref()) }
    }
    impl fmt::Display for WrappedError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.description())
        }
    }

    let result: Result<String, WrappedError> = Err(WrappedError {
        detail: "something happened".to_owned(),
        cause: "cause 1234".into(),
    });
    assert_err!(result, "cause 1234");
}


/// An RNG with a fixed seed, for deterministic random tests
///
/// ```
/// use testutil::TestRand;
///
/// let mut rng0 = TestRand::default();
/// let mut rng1 = TestRand::default();
///
/// let vec_from_rng0: Vec<u8> = rng0.gen_byte_vec(10);
///
/// assert_eq!(vec_from_rng0, rng1.gen_byte_vec(10),
///             "Same seed should produce same sequence every time");
/// ```
///
/// Implements Read (std::io) to read random bytes. If writing to a file, be
/// sure to use `Read::take()` to limit the number of bytes read.
///
pub struct TestRand(XorShiftRng);

type Seed = [u32; 4];
const DEFAULT_SEED: Seed = [255, 20, 110, 0];

impl Default for TestRand {
    fn default() -> Self { TestRand::from_seed(DEFAULT_SEED) }
}

impl TestRand {
    /// Create an instance using the given seed
    ///
    /// Default can also be used to create an instance with a default seed.
    ///
    /// ```
    /// use testutil::TestRand;
    ///
    /// let mut rng0 = TestRand::default();
    /// let mut rng1 = TestRand::from_seed([0,1,2,3]);
    /// ```
    ///
    pub fn from_seed(seed: Seed) -> Self {
        TestRand(XorShiftRng::from_seed(seed))
    }

    /// Get one random byte
    pub fn next_byte(&mut self) -> u8 { self.0.gen() }

    /// Get a random vector of the given size
    pub fn gen_byte_vec(&mut self, size: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(size);
        self.take(size as u64)
            .read_to_end(&mut vec)
            .expect("read random bytes");
        vec
    }
}

impl_deref_mut!(TestRand => XorShiftRng);

impl<'a> Read for &'a mut TestRand {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut pos = 0;
        while pos < buf.len() {
            buf[pos] = self.next_byte();
            pos += 1;
        }
        Ok(pos)
    }
}

impl Rng for TestRand {
    fn next_u32(&mut self) -> u32 { self.0.next_u32() }
}

/// Create a temporary directory in an in-memory filesystem
///
/// ```
/// use testutil::in_mem_tempdir;
///
/// let temp_path;
/// {
///     let temp = in_mem_tempdir("example").unwrap();
///     temp_path = temp.path().to_owned();
///     assert!(temp_path.is_dir(), "Directory should be created");
/// }
///
/// assert!(!temp_path.exists(), "Directory should be deleted when dropped");
/// ```
///
pub fn in_mem_tempdir(prefix: &str) -> io::Result<TempDir> {
    TempDir::new_in("/dev/shm", prefix)
}

/// Create a temporary directory in an in-memory filesystem
///
/// A wrapper for the `in_mem_tempdir` function. It makes the call more concise
/// by also doing the following:
///
/// - Creating a default directory name based on the `module_path!()` of the
/// current module.
/// - Unwrapping the result.
///
/// ```
/// #[macro_use]
/// extern crate testutil;
///
/// fn main() {
///     let temp_path;
///     {
///         let temp = in_mem_tempdir!();
///         temp_path = temp.path().to_owned();
///         assert!(temp_path.is_dir(), "Directory should be created");
///     }
///
///     assert!(!temp_path.exists(), "Directory should be deleted");
/// }
/// ```
///
#[macro_export]
macro_rules! in_mem_tempdir {
    () => {
        $crate::in_mem_tempdir(&module_path!().replace(":","_")).unwrap()
    }
}

/// Write a file from different byte sources
///
/// See the `write_files` macro for a more concise syntax.
///
/// The goal here is to get the file onto the disk with as little fuss as
/// possible. This function will...
///
/// - create the file if it does not exist.
/// - create parent directories if they do not exist.
///
/// ```
/// use testutil::{in_mem_tempdir, write_file, TestRand};
/// use std::io::Read;
///
/// let temp = in_mem_tempdir("example").unwrap();
/// write_file(temp.path().join("hello.txt"), "Hello, world!").unwrap();
/// write_file(temp.path().join("bytes.bin"), &vec![0u8,1,2,3]).unwrap();
///
/// // Combine with RandomBytes to generate deterministic psuedo-random files
/// let mut rng = TestRand::default();
/// write_file(temp.path().join("random0.bin"), rng.take(10)).unwrap();
/// write_file(temp.path().join("random1.bin"), rng.take(10)).unwrap();
///
/// assert!(temp.path().join("hello.txt").is_file());
/// assert!(temp.path().join("random0.bin").is_file());
/// ```
///
pub fn write_file<P, R, S>(path: P, source: S) -> io::Result<u64>
    where P: AsRef<Path>,
          R: io::Read,
          S: Into<ByteSource<R>>
{
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
                let msg = format!("Could not create parent dir '{}': '{:?}'",
                                  parent.display(),
                                  e);
                io::Error::new(io::ErrorKind::Other, msg)
            })?;
    }

    let mut file = OpenOptions::new().write(true)
        .create(true)
        .open(&path)
        .map_err(|e| {
            let msg = format!("Could not open/create file '{}': '{:?}'",
                              path.display(),
                              e);
            io::Error::new(io::ErrorKind::Other, msg)
        })?;

    io::copy(&mut source.into().0, &mut file)
}

/// Wrapper type for a Reader (std::io::Read)
///
/// Used to allow the `write_file` function and `write_files` macro to take
/// varied parameters, via the various From implementations.
///
/// ```
/// use testutil::{ByteSource, TestRand};
/// use std::io::BufReader;
/// use std::io::Read;
///
/// ByteSource::from("hello!");                             // strings
/// ByteSource::from(vec![0u8, 1, 2, 3].as_slice());        // byte slices
/// ByteSource::from(&vec![0u8, 1, 2, 3]);                  // byte vectors
/// ByteSource::from(BufReader::new("hello!".as_bytes()));  // other readers
///
/// // Combine with RandomBytes
/// let mut rng = TestRand::default();
/// ByteSource::from(rng.take(10));
/// ```
///
pub struct ByteSource<R: Read>(R);

impl<'a, R: 'a + Read> From<R> for ByteSource<R> {
    fn from(r: R) -> Self { ByteSource(r) }
}

impl<'a> From<&'a str> for ByteSource<&'a [u8]> {
    fn from(s: &'a str) -> Self { ByteSource(s.as_bytes()) }
}

impl<'a> From<&'a Vec<u8>> for ByteSource<&'a [u8]> {
    fn from(v: &'a Vec<u8>) -> Self { ByteSource(v) }
}


/// Read a file as a String
///
/// Shorter version of `std::io::Read::read_to_string`.
pub fn read_file_to_string(path: &Path) -> io::Result<String> {
    let mut s = String::new();
    File::open(path)?.read_to_string(&mut s)?;
    Ok(s)
}

/// Read a file as a byte vector (Vec<u8>)
///
/// Shorter version of `std::io::Read::read_to_end`.
pub fn read_file_to_end(path: &Path) -> io::Result<Vec<u8>> {
    let mut v = Vec::new();
    File::open(path)?.read_to_end(&mut v)?;
    Ok(v)
}


/// Write several files to a directory for testing
///
/// ```
/// #[macro_use]
/// extern crate testutil;
/// use testutil::{in_mem_tempdir,TestRand};
/// use std::io::Read;
///
/// fn main() {
///     let temp = in_mem_tempdir("example").unwrap();
///     let mut rng = TestRand::default();
///
///     write_files!{
///         temp.path();
///         "hello.txt" => "Hello world!",
///         "bytes.bin" => &vec![0u8,1,2,3],
///         "random.bin" => rng.take(20),
///         "subdir/subfile.txt" => "Will create subdirectories!",
///     };
///     assert!(temp.path().join("hello.txt").is_file());
///     assert!(temp.path().join("subdir").is_dir());
/// }
/// ```
///
/// # Panics
///
/// Panics if there is any error.
///
#[macro_export]
macro_rules! write_files {
    ($base_path:expr; $( $fname:expr => $contents:expr, )* ) => {
        $(
            $crate::write_file(
                &$base_path.join($fname), $contents
            ).expect("Could not write test file");
        )*
    }
}

#[test]
fn test_rand_bytes_same_every_time() {
    let rand_bytes = TestRand::default().gen_byte_vec(10);
    assert_eq!(rand_bytes, [7, 179, 173, 173, 109, 225, 168, 201, 120, 240]);
}

#[test]
fn test_rand_bytes_read() {
    let mut rng = TestRand::default();
    let mut rand_bytes: Vec<u8> = Vec::new();
    rand_bytes.resize(16, 0);
    let count = rng.take(10)
        .read(rand_bytes.as_mut())
        .expect("Read random bytes");
    assert_eq!(count, 10, "Number of read bytes by hitting read limit");
    assert_eq!(rand_bytes,
               [7, 179, 173, 173, 109, 225, 168, 201, 120, 240, 0, 0, 0, 0,
                0, 0]);

    let mut rng = TestRand::default();
    rand_bytes.clear();
    rand_bytes.resize(10, 0);
    let count = rng.take(20)
        .read(rand_bytes.as_mut())
        .expect("Read random bytes");
    assert_eq!(count, 10, "Number of read bytes by hitting end of buffer");
    assert_eq!(rand_bytes, [7, 179, 173, 173, 109, 225, 168, 201, 120, 240]);
}
