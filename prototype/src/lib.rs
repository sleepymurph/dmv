#![allow(dead_code)]
mod repo4;

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
