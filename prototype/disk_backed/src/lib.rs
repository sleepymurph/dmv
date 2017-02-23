#[macro_use]
extern crate log;
extern crate rustc_serialize;

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
use std::path::Path;
use std::path::PathBuf;

#[derive(Debug,Clone,Copy)]
enum Op {
    Read,
    Write,
}

#[derive(Debug)]
pub struct DiskBackError {
    during: Op,
    data_desc: String,
    path: PathBuf,
    cause: Box<Error>,
}

impl DiskBackError {
    fn new<E>(during: Op, data_desc: &str, path: &Path, cause: E) -> Self
        where E: Into<Box<Error>>
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

type Result<T> = ::std::result::Result<T, DiskBackError>;

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

fn hash<T>(data: &T) -> u64
    where T: Hash
{
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

pub struct DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    desc: String,
    path: PathBuf,
    data: T,
    disk_state: u64,
}

impl<T> DiskBacked<T>
    where T: Encodable + Decodable + Hash + Default
{
    pub fn new(desc: String, path: PathBuf) -> Self {
        let data = T::default();
        DiskBacked::construct(desc, path, data)
    }
}

impl<T> DiskBacked<T>
    where T: Encodable + Decodable + Hash
{
    fn construct(desc: String, path: PathBuf, data: T) -> Self {
        DiskBacked {
            desc: desc,
            path: path,
            disk_state: hash(&data),
            data: data,
        }
    }

    pub fn init(desc: String, path: PathBuf, data: T) -> Self {
        DiskBacked::construct(desc, path, data)
    }

    pub fn read(desc: String, path: PathBuf) -> Result<Self> {
        debug!("Reading {}: {}", desc, path.display());
        let data: T = read(&desc, &path)?;
        Ok(DiskBacked::construct(desc, path, data))
    }

    pub fn write(&mut self) -> Result<()> {
        let new_hash = hash(&self.data);
        debug!("Writing {}: {}", self.desc, self.path.display());
        write(&self.desc, &self.path, &self.data)?;
        self.disk_state = new_hash;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        let new_hash = hash(&self.data);
        if new_hash != self.disk_state {
            debug!("Flushing {}: {}", self.desc, self.path.display());
            write(&self.desc, &self.path, &self.data)?;
            self.disk_state = new_hash;
        } else {
            debug!("{} unchanged: {}", self.desc, self.path.display());
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
