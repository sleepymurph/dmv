use fsutil;
use rand::{Rng, SeedableRng, Generator, XorShiftRng};
use std::fs;
use std::io;
use std::io::Read;
use std::iter::IntoIterator;
use std::path;
use tempdir::TempDir;

/// Generates deterministic psuedorandom bytes
pub struct RandBytes {
    rng: XorShiftRng,
}

pub struct RandBytesRead<'a> {
    rng: &'a mut RandBytes,
    count: usize,
    limit: usize,
}

impl RandBytes {
    pub fn new() -> Self {
        RandBytes { rng: XorShiftRng::from_seed([255, 20, 110, 0]) }
    }

    pub fn next(&mut self) -> u8 { self.rng.gen() }

    pub fn next_many(&mut self, size: usize) -> Vec<u8> {
        let mut vec = Vec::new();
        self.as_read(size).read_to_end(&mut vec).expect("read random bytes");
        vec
    }

    pub fn as_read(&mut self, limit: usize) -> RandBytesRead {
        RandBytesRead {
            rng: self,
            count: 0,
            limit: limit,
        }
    }

    pub fn write_file(&mut self,
                      path: &path::Path,
                      size: u64)
                      -> io::Result<u64> {

        let mut rand_bytes = self.as_read(size as usize);
        write_file(path, &mut rand_bytes)
    }
}

impl<'a> IntoIterator for &'a mut RandBytes {
    type Item = u8;
    type IntoIter = Generator<'a, u8, XorShiftRng>;

    fn into_iter(self) -> Self::IntoIter { self.rng.gen_iter::<u8>() }
}

impl<'a> Read for RandBytesRead<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut pos = 0;
        while pos < buf.len() && self.count < self.limit {
            buf[pos] = self.rng.next();
            pos += 1;
            self.count += 1;
        }
        Ok(pos)
    }
}

/// Create a temporary directory in an in-memory filesystem
///
/// ```
/// use prototypelib::testutil::in_mem_tempdir;
///
/// # fn main() {
/// let temp_path;
/// {
///     let temp = in_mem_tempdir("example").unwrap();
///     temp_path = temp.path().to_owned();
///     assert!(temp_path.is_dir(), "Directory should be created");
/// }
///
/// assert!(!temp_path.exists(), "Directory should be deleted when dropped");
/// # }
pub fn in_mem_tempdir(prefix: &str) -> io::Result<TempDir> {
    TempDir::new_in("/dev/shm", prefix)
}

/// Quickly write a file from different byte sources
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
/// use prototypelib::testutil::{in_mem_tempdir, write_file, RandBytes};
///
/// # fn main() {
/// let temp = in_mem_tempdir("example").unwrap();
/// write_file(temp.path().join("hello.txt"), "Hello, world!").unwrap();
/// write_file(temp.path().join("bytes.bin"), &vec![0u8,1,2,3]).unwrap();
///
/// // Combine with RandomBytes to generate deterministic psuedo-random files
/// let mut rng = RandBytes::new();
/// write_file(temp.path().join("random0.bin"), rng.as_read(10)).unwrap();
/// write_file(temp.path().join("random1.bin"), rng.as_read(10)).unwrap();
///
/// assert!(temp.path().join("hello.txt").is_file());
/// assert!(temp.path().join("random0.bin").is_file());
/// # }
/// ```
pub fn write_file<P, R, S>(path: P, source: S) -> io::Result<u64>
    where P: AsRef<path::Path>,
          R: io::Read,
          S: Into<ByteSource<R>>
{
    let path = path.as_ref();

    try!(fsutil::create_parents(&path));
    let mut file = try!(fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io::Error::new(e.kind(), format!("{}", &path.display()))));

    io::copy(&mut source.into().0, &mut file)
}

/// Wrapper type for a Reader (std::io::Read)
///
/// Used to allow the `write_file` function to take varied parameters.
///
/// ```
/// use prototypelib::testutil::{ByteSource, RandBytes};
/// use std::io::BufReader;
///
/// # fn main() {
/// ByteSource::from("hello!");                             // strings
/// ByteSource::from(vec![0u8, 1, 2, 3].as_slice());        // byte slices
/// ByteSource::from(&vec![0u8, 1, 2, 3]);                  // byte vectors
/// ByteSource::from(BufReader::new("hello!".as_bytes()));  // other readers
///
/// // Combine with RandomBytes
/// let mut rng = RandBytes::new();
/// ByteSource::from(rng.as_read(10));
/// # }
/// ```
pub struct ByteSource<R: Read>(R);

impl<'a, R: 'a + Read> From<R> for ByteSource<R> {
    fn from(r: R) -> Self { ByteSource(r) }
}

impl<'a> From<&'a str> for ByteSource<&'a [u8]> {
    fn from(s: &'a str) -> Self { ByteSource(s.as_bytes()) }
}

impl<'a> From<&'a Vec<u8>> for ByteSource<&'a [u8]> {
    fn from(s: &'a Vec<u8>) -> Self { ByteSource(s) }
}


/// Easily create a directory full of files for testing
///
/// ```
/// #[macro_use]
/// extern crate prototypelib;
/// use prototypelib::testutil::{in_mem_tempdir,RandBytes};
///
/// fn main() {
///     let temp = in_mem_tempdir("example").unwrap();
///     let mut rng = RandBytes::default();
///
///     write_files!{
///         temp.path();
///         "hello.txt" => "Hello world!",
///         "bytes.bin" => &vec![0u8,1,2,3],
///         "random.bin" => rng.as_read(20),
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
#[macro_export]
macro_rules! write_files {
    ($base_path:expr; $( $fname:expr => $contents:expr, )* ) => {
        $(
            $crate::testutil::write_file(
                &$base_path.join($fname), $contents
            ).expect("Could not write test file");
        )*
    }
}

#[test]
fn test_rand_bytes_same_every_time() {
    let mut rng = RandBytes::new();
    let mut rand_bytes: Vec<u8> = Vec::new();
    rand_bytes.extend(rng.into_iter().take(10));
    assert_eq!(rand_bytes, [7, 179, 173, 173, 109, 225, 168, 201, 120, 240]);
}

#[test]
fn test_rand_bytes_read() {
    let mut rng = RandBytes::new();
    let mut rand_bytes: Vec<u8> = Vec::new();
    rand_bytes.resize(16, 0);
    let count = rng.as_read(10)
        .read(rand_bytes.as_mut())
        .expect("Read random bytes");
    assert_eq!(count, 10, "Number of read bytes by hitting read limit");
    assert_eq!(rand_bytes,
               [7, 179, 173, 173, 109, 225, 168, 201, 120, 240, 0, 0, 0, 0,
                0, 0]);

    let mut rng = RandBytes::new();
    rand_bytes.clear();
    rand_bytes.resize(10, 0);
    let count = rng.as_read(20)
        .read(rand_bytes.as_mut())
        .expect("Read random bytes");
    assert_eq!(count, 10, "Number of read bytes by hitting end of buffer");
    assert_eq!(rand_bytes, [7, 179, 173, 173, 109, 225, 168, 201, 120, 240]);
}
