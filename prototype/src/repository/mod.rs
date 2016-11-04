#![allow(dead_code,unused_variables,unused_mut,unused_imports)]
mod disk;

use std::io;
use dag::*;

pub trait IncomingObject: io::Write {
    fn set_key(self, key: &ObjectKey) -> io::Result<()>;
}

pub trait Repository {
    type IncomingType: IncomingObject + Sized;

    fn init(&mut self) -> io::Result<()>;

    fn has_object(&mut self, key: &ObjectKey) -> bool;
    fn stat_object(&mut self, key: &ObjectKey) -> ObjectStat;
    fn read_object(&mut self, key: &ObjectKey) -> &mut io::Read;
    fn add_object(&mut self) -> Self::IncomingType;
}
