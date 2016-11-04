#![allow(dead_code,unused_variables,unused_mut,unused_imports)]
mod disk;

use std::io;
use dag::*;

pub trait IncomingObject<'a>: io::Write + 'a {
    fn set_key(mut self, key: &ObjectKey) -> io::Result<()>;
}

pub trait Repository<'a> {
    type ReadType: io::Read + Sized;
    type IncomingType: IncomingObject<'a> + Sized;

    fn init(&mut self) -> io::Result<()>;

    fn has_object(&self, key: &ObjectKey) -> bool;
    fn stat_object(&mut self, key: &ObjectKey) -> ObjectStat;
    fn read_object(&mut self, key: &ObjectKey) -> io::Result<Self::ReadType>;
    fn add_object(&'a mut self) -> io::Result<Self::IncomingType>;
}
