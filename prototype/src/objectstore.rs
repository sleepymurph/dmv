use cache;
use constants;
use dag;
use fsutil;
use rollinghash;
use std::fs;
use std::io;
use std::io::Write;
use std::path;

pub struct ObjectStore {
    path: path::PathBuf,
}

pub struct IncomingObject {
    temp_path: path::PathBuf,
    file: fs::File,
}

impl ObjectStore {
    pub fn init(path: path::PathBuf) -> io::Result<Self> {
        try!(fs::create_dir_all(&path));
        Self::load(path)
    }

    pub fn load(path: path::PathBuf) -> io::Result<Self> {
        Ok(ObjectStore { path: path })
    }

    pub fn path(&self) -> &path::Path {
        &self.path
    }

    fn object_path(&self, key: &dag::ObjectKey) -> path::PathBuf {
        let key = key.to_hex();
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }

    pub fn has_object(&self, key: &dag::ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    pub fn read_object(&self, key: &dag::ObjectKey) -> io::Result<fs::File> {
        fs::File::open(self.object_path(key))
    }

    pub fn new_object(&mut self) -> io::Result<IncomingObject> {
        let temp_path = self.path.join("tmp");
        try!(fsutil::create_parents(&temp_path));
        let file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));
        Ok(IncomingObject {
            temp_path: temp_path,
            file: file,
        })
    }

    pub fn save_object(&mut self,
                       key: dag::ObjectKey,
                       mut object: IncomingObject)
                       -> io::Result<()> {

        try!(object.flush());
        let permpath = self.object_path(&key);
        try!(fsutil::create_parents(&permpath));
        fs::rename(&object.temp_path, &permpath)
    }

    pub fn store_object<O: dag::Object>(&mut self,
                                        obj: &O)
                                        -> io::Result<dag::ObjectKey> {
        let mut incoming = try!(self.new_object());
        let key = try!(obj.write_to(&mut incoming));
        try!(self.save_object(key, incoming));
        Ok(key)
    }

    pub fn store_file(&mut self,
                      path: &path::Path)
                      -> io::Result<dag::ObjectKey> {

        let file = try!(fs::File::open(path));
        let file = io::BufReader::new(file);

        let mut chunker = rollinghash::ChunkReader::wrap(file);
        let chunk1 = chunker.next();
        let chunk2 = chunker.next();

        match (chunk1, chunk2) {
            (None, None) => {
                // Empty file
                let blob = dag::Blob::from_vec(vec![0u8;0]);
                self.store_object(&blob)
            }
            (Some(v1), None) => {
                // File only one-chunk long
                let blob = dag::Blob::from_vec(v1?);
                self.store_object(&blob)
            }
            (Some(v1), Some(v2)) => {
                // Multiple chunks
                let mut chunkedblob = dag::ChunkedBlob::new();

                for chunk in vec![v1, v2].into_iter().chain(chunker) {
                    let blob = dag::Blob::from_vec(chunk?);
                    let key = try!(self.store_object(&blob));
                    chunkedblob.add_chunk(blob.size(), key);
                }

                self.store_object(&chunkedblob)
            }
            (None, Some(_)) => unreachable!(),
        }
    }

    pub fn store_file_with_caching(&mut self,
                                   path: &path::Path)
                                   -> io::Result<dag::ObjectKey> {

        let file_stats = try!(cache::FileStats::read(path));

        let parent_dir = path.parent().unwrap();
        let basename = path.file_name().unwrap();

        let cache_file_name = parent_dir.join(constants::CACHE_FILE_NAME);
        let mut file_cache = cache::HashCacheFile::open(cache_file_name)
            .unwrap();
        if let cache::CacheStatus::Cached { hash } =
               file_cache.check(&basename, &file_stats) {
            return Ok(hash);
        }

        let result = self.store_file(path);

        if let Ok(key) = result {
            file_cache.as_mut()
                .insert(basename.into(), file_stats, key.clone());
        }

        result
    }
}

impl io::Write for IncomingObject {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
pub mod test {
    use dag;
    use dag::Object;
    use rollinghash;
    use std::fs;
    use std::io;
    use std::io::Read;
    use std::io::Write;
    use super::*;
    use testutil;

    fn create_temp_repository
        ()
        -> io::Result<(testutil::TempDir, ObjectStore)>
    {
        let wd_temp = try!(testutil::in_mem_tempdir("test_directory"));
        let wd_path = wd_temp.path().to_path_buf();
        try!(fs::create_dir_all(&wd_path));
        let os_path = wd_path.join("object_store");
        let os = try!(ObjectStore::init(os_path));

        Ok((wd_temp, os))
    }

    #[test]
    fn test_object_store() {
        let (key, data) = (
            dag::ObjectKey::from_hex("69342c5c39e5ae5f0077aecc32c0f81811fb8193")
                .unwrap(),
            "Hello!".to_string()
        );

        let (_tempdir, mut store) = create_temp_repository().unwrap();

        assert_eq!(store.has_object(&key), false);
        {
            let mut writer = store.new_object().expect("new incoming object");
            writer.write(data.as_bytes()).expect("write to incoming");
            store.save_object(key.clone(), writer).expect("store incoming");
        }
        {
            let mut reader = store.read_object(&key).expect("open object");
            let mut read_string = String::new();
            reader.read_to_string(&mut read_string).expect("read object");
            assert_eq!(read_string, data);
        }
    }

    #[test]
    fn test_store_file_empty() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");
        testutil::write_str_file(&filepath, "").unwrap();

        let hash = objectstore.store_file(&filepath).unwrap();

        let obj = objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);
        assert_eq!(header.content_size, 0);

        let blob = dag::Blob::read_from(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "");
    }

    #[test]
    fn test_store_file_small() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");

        testutil::write_str_file(&filepath, "foo").unwrap();

        let hash = objectstore.store_file(&filepath).unwrap();

        let obj = objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);

        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
        assert_eq!(header.object_type, dag::ObjectType::Blob);

        let blob = dag::Blob::read_from(&mut obj).unwrap();
        assert_eq!(String::from_utf8(blob.content).unwrap(), "foo");
    }

    #[test]
    fn test_store_file_chunked() {
        let (temp, mut objectstore) = create_temp_repository().unwrap();
        let filepath = temp.path().join("foo");
        let filesize = 3 * rollinghash::CHUNK_TARGET_SIZE as u64;

        let mut rng = testutil::RandBytes::new();
        rng.write_file(&filepath, filesize).unwrap();

        let hash = objectstore.store_file(&filepath).unwrap();

        let obj = objectstore.read_object(&hash).unwrap();
        let mut obj = io::BufReader::new(obj);
        let header = dag::ObjectHeader::read_from(&mut obj).unwrap();

        assert_eq!(header.object_type, dag::ObjectType::ChunkedBlob);

        let chunked = dag::ChunkedBlob::read_from(&mut obj).unwrap();
        assert_eq!(chunked.total_size, filesize);
        assert_eq!(chunked.chunks.len(), 5);

        for chunkrecord in chunked.chunks {
            let obj = objectstore.read_object(&chunkrecord.hash).unwrap();
            let mut obj = io::BufReader::new(obj);
            let header = dag::ObjectHeader::read_from(&mut obj).unwrap();
            assert_eq!(header.object_type, dag::ObjectType::Blob);

            let blob = dag::Blob::read_from(&mut obj).unwrap();
            assert_eq!(blob.size(), chunkrecord.size);
        }
    }

}
