#[allow(dead_code)]
mod repository {

    use std::io;

    pub type ObjectKey = str;
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

    pub trait IncomingObject {
        fn writer(&mut self) -> &mut io::Write;
        fn set_key(self, key: &ObjectKey) -> io::Result<()>;
    }

    pub trait Repository {
        fn has_object(&mut self, key: &ObjectKey) -> bool;
        fn stat_object(&mut self, key: &ObjectKey) -> ObjectStat;
        fn read_object(&mut self, key: &ObjectKey) -> io::Read;
        fn add_object(&mut self) -> &mut IncomingObject;
    }


    mod test {

        use std::io;
        use super::IncomingObject;

        struct DummyIncoming {
            _writer: io::Sink,
        }

        impl DummyIncoming {
            fn new() -> Self {
                return DummyIncoming { _writer: io::sink() };
            }
        }

        impl IncomingObject for DummyIncoming {
            fn writer(&mut self) -> &mut io::Write {
                &mut self._writer
            }
            fn set_key(self, _key: &super::ObjectKey) -> io::Result<()> {
                Ok(())
            }
        }

        #[test]
        fn should_not_be_able_to_write_after_set_key() {
            let mut incoming = DummyIncoming::new();
            let _ = incoming.writer().write_all(b"hello");
            let _ = incoming.set_key("hello");
            // This should not compile if you uncomment it
            // incoming.writer().write_all("hello".as_bytes());
        }
    }
}
