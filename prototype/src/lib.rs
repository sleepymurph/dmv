#![allow(dead_code)]
mod objectstore;

mod dag {
    pub type ObjectKey = String;
    pub type ObjectSize = u64;

    pub enum ObjectType {
        Blob,
        Tree,
        Commit,
    }

    pub struct ObjectStat {
        pub objecttype: ObjectType,
        pub size: ObjectSize,
    }
}
