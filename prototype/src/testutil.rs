#![cfg(test)]

extern crate rand;
use self::rand::{Rng, SeedableRng, Generator, XorShiftRng};

use std::iter::IntoIterator;
use std::fs;
use std::io;
use std::io::Read;
use std::path;

use fsutil;

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

    pub fn next(&mut self) -> u8 {
        self.rng.gen()
    }

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

    fn into_iter(self) -> Self::IntoIter {
        self.rng.gen_iter::<u8>()
    }
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

pub fn write_file<R: Read>(path: &path::Path,
                           mut contents: R)
                           -> io::Result<u64> {

    try!(fsutil::create_parents(path));
    let mut file = try!(fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&path)
        .map_err(|e| io::Error::new(e.kind(), format!("{}", &path.display()))));

    io::copy(&mut contents, &mut file)
}

pub fn write_str_file(path: &path::Path, contents: &str) -> io::Result<u64> {
    write_file(path, contents.as_bytes())
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
