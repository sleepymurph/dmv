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

#[cfg(test)]
mod test {

    use super::*;
    use std::io::{Read, Write};
    use std::io;

    /*
    fn run_repository_trait_tests<'r, F, R>(create_temp_repo: F)
        where F: Fn() -> R,
              R: Repository<'r> + 'r
    {
        let mut repo = create_temp_repo();
        let data = "here be content";
        let key = "9cac8e6ad1da3212c89b73fdbb2302180123b9ca";
        {
            let mut incoming = repo.add_object().expect("open incoming");
            incoming.write(data.as_bytes()).expect("write to incoming");
            incoming.flush().expect("flush incoming");
            incoming.set_key(key).expect("set key");
        }
        assert_eq!(repo.has_object(key), true);

        let mut reader = repo.read_object(key).expect("open saved object");
        let mut read_data = String::new();
        reader.read_to_string(&mut read_data).expect("read saved object");
        assert_eq!(read_data, data);
    }
    */

    pub trait Foo {
        fn new() -> Self;
        fn party(&self) -> String;
    }

    struct Bar {}
    impl Foo for Bar {
        fn new() -> Bar {
            Bar {}
        }
        fn party(&self) -> String {
            "party!".into()
        }
    }

    fn do_foo_tests<F,T>(create: F)
        where F: Fn()->T,
              T: Foo
    {
        let mut foo = create();
        assert_eq!(foo.party(), "party!".to_string());
    }

    #[test]
    fn test_factory() {
        do_foo_tests(Bar::new);
    }
}
