use dag::KEY_SHORT_LEN;
use dag::KEY_SIZE_HEX_DIGITS;
use dag::OBJECT_KEY_PAT;
use dag::ObjectCommon;
use dag::ObjectHandle;
use dag::ObjectKey;
use error::*;
use fsutil;
use regex::Regex;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

pub type RefName = String;
type RefNameBorrow = str;

pub struct ObjectStore {
    path: PathBuf,
}

impl ObjectStore {
    pub fn init(path: PathBuf) -> Result<Self> {
        try!(fs::create_dir_all(&path));
        Self::open(path)
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        Ok(ObjectStore { path: path })
    }

    pub fn path(&self) -> &Path { &self.path }

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.object_path_sloppy(&key.to_hex())
    }

    fn object_path_sloppy(&self, key: &str) -> PathBuf {
        self.path
            .join("objects")
            .join(&key[0..2])
            .join(&key[2..4])
            .join(&key[4..])
    }

    fn object_from_path(&self, path: &Path) -> Result<ObjectKey> {
        path.strip_prefix(&self.path)
            .and_then(|p| p.strip_prefix("objects"))
            .err_into()
            .map(|p| p.to_str().expect("should be ascii"))
            .map(|s| s.replace("/", ""))
            .and_then(|s| ObjectKey::from_hex(&s))
    }

    fn ref_path(&self, name: &RefNameBorrow) -> PathBuf {
        self.path.join("refs").join(name)
    }

    pub fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    pub fn find_object(&self, rev: &RevSpec) -> Result<ObjectKey> {
        fn get_fn_str(path: &Path) -> &str {
            path.file_name()
                .expect("should have a file_name")
                .to_str()
                .expect("should be ascii")
        }
        match *rev {
            RevSpec::Hash(ref hash) => {
                if self.has_object(hash) {
                    Ok(hash.to_owned())
                } else {
                    bail!(ErrorKind::RevNotFound(rev.to_owned()))
                }
            }
            RevSpec::ShortHash(ref s) => {
                let path = self.object_path_sloppy(s);
                let dir = path.parent_or_err()?;
                let short_name = get_fn_str(&path);

                if !dir.exists() {
                    bail!(ErrorKind::RevNotFound(rev.to_owned()))
                } else {
                    for entry in dir.read_dir()? {
                        let entry = entry?.path();
                        debug!("Looking for '{}', checking: {}",
                               rev,
                               entry.strip_prefix(&self.path)?.display());
                        if get_fn_str(&entry).starts_with(&short_name) {
                            return self.object_from_path(&entry);
                        }
                    }
                    bail!(ErrorKind::RevNotFound(rev.to_owned()))
                }
            }
        }
    }

    pub fn open_object_file(&self,
                            key: &ObjectKey)
                            -> Result<io::BufReader<fs::File>> {

        if !self.has_object(&key) {
            bail!(ErrorKind::ObjectNotFound(key.to_owned()))
        }

        let file = try!(fs::File::open(self.object_path(key)).err_into());
        Ok(io::BufReader::new(file))
    }

    pub fn open_object(&self, key: &ObjectKey) -> Result<ObjectHandle> {
        let file = self.open_object_file(key)?;
        ObjectHandle::read_header(Box::new(file))
    }

    /// Writes a single object into the object store
    ///
    /// Returns the hash key of the object
    pub fn store_object(&mut self, obj: &ObjectCommon) -> Result<ObjectKey> {

        // If object already exists, no need to store
        let key = obj.calculate_hash();
        if self.has_object(&key) {
            return Ok(key);
        }

        // Create temporary file
        let temp_path = self.path.join("tmp");
        try!(fsutil::create_parents(&temp_path));
        let mut file = try!(fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .map_err(|e| {
                io::Error::new(e.kind(), format!("{}", &temp_path.display()))
            }));

        // Write object to temporary file
        let key = try!(obj.write_to(&mut file));

        // Move file to permanent path
        let permpath = self.object_path(&key);
        try!(fsutil::create_parents(&permpath));
        try!(fs::rename(&temp_path, &permpath));

        Ok(key)
    }


    pub fn update_ref(&mut self,
                      name: &RefNameBorrow,
                      hash: &ObjectKey)
                      -> Result<()> {
        use std::io::Write;
        let ref_path = self.ref_path(name);
        fsutil::create_parents(&ref_path)
            .and_then(|_| {
                fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&ref_path)
            })
            .and_then(|mut file| write!(file, "{}", hash))
            .chain_err(|| {
                format!("Could not write ref path: {}", ref_path.display())
            })
    }

    pub fn read_ref(&mut self, name: &RefNameBorrow) -> Result<ObjectKey> {
        use std::io::Read;
        let ref_path = self.ref_path(name);
        if !ref_path.exists() {
            return Err(ErrorKind::RefNotFound(name.to_owned()).into());
        }
        fs::File::open(&ref_path)
            .and_then(|mut file| {
                let mut ref_str = String::new();
                file.read_to_string(&mut ref_str).map(|_| ref_str)
            })
            .err_into()
            .and_then(|s| ObjectKey::from_hex(&s))
            .chain_err(|| {
                format!("Could not read ref path: {}", ref_path.display())
            })
    }
}

lazy_static!{
    pub static ref SHORT_OBJECT_KEY_PAT:Regex = Regex::new(
        &format!("[[:xdigit:]]{{ {},{} }}",
                    KEY_SHORT_LEN, KEY_SIZE_HEX_DIGITS-1)).unwrap();
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RevSpec {
    Hash(ObjectKey),
    ShortHash(String),
}

impl FromStr for RevSpec {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        if OBJECT_KEY_PAT.is_match(s) {
            ObjectKey::from_hex(s).map(|h| RevSpec::Hash(h))
        } else if SHORT_OBJECT_KEY_PAT.is_match(s) {
            Ok(RevSpec::ShortHash(s.to_owned()))
        } else {
            bail!(ErrorKind::BadRevSpec(s.to_owned()))
        }
    }
}

impl From<ObjectKey> for RevSpec {
    fn from(hash: ObjectKey) -> Self { RevSpec::Hash(hash) }
}

impl fmt::Display for RevSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            RevSpec::Hash(ref hash) => write!(f, "{:x}", hash),
            RevSpec::ShortHash(ref short) => write!(f, "{}", short),
        }
    }
}

#[cfg(test)]
pub mod test {
    use dag::Blob;
    use dag::Object;
    use dag::ToHashed;
    use std::str::FromStr;
    use super::*;
    use testutil::tempdir::TempDir;

    pub fn create_temp_repository() -> Result<(TempDir, ObjectStore)> {
        let wd_temp = in_mem_tempdir!();
        let os_path = wd_temp.path().join("object_store");
        let os = try!(ObjectStore::init(os_path));

        Ok((wd_temp, os))
    }

    #[test]
    fn test_store_and_retrieve() {
        let (_tempdir, mut store) = create_temp_repository().unwrap();

        let obj = Blob::from("Hello!").to_hashed();

        assert!(!store.has_object(obj.hash()),
                "Store should not have key at first");

        let stored_key = store.store_object(&obj).unwrap();
        assert_eq!(stored_key,
                   *obj.hash(),
                   "Key when stored should be the same as given by \
                    calculate_hash");
        assert!(store.has_object(&stored_key),
                "Store should report that key is present");

        let mut reader = store.open_object_file(&stored_key).unwrap();
        let retrieved = Object::read_from(&mut reader).unwrap();
        assert_eq!(retrieved,
                   *obj,
                   "Retrieved object should be the same as stored object");
    }


    #[test]
    fn test_update_and_read_ref() {
        let (_tempdir, mut store) = create_temp_repository().unwrap();
        let hash = Blob::from("Hello!").to_hashed().hash().to_owned();

        let result = store.read_ref("master");
        assert_match!(result,
                    Err(Error(ErrorKind::RefNotFound(ref name), _))
                        if name=="master");

        let result = store.update_ref("master", &hash);
        assert_match!(result, Ok(()));

        let result = store.read_ref("master");
        assert_match!(result, Ok(x) if x==hash);
    }


    #[test]
    fn test_rev_spec() {
        let hash =
            ObjectKey::from_hex("da39a3ee5e6b4b0d3255bfef95601890afd80709")
                .unwrap();
        let rev = RevSpec::from_str("da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_match!(rev, Ok(RevSpec::Hash(ref h)) if h==&hash);

        let rev = RevSpec::from_str(&hash.to_short());
        assert_match!(rev, Ok(RevSpec::ShortHash(ref s)) if s=="da39a3ee");
    }


    #[test]
    fn test_find_object() {
        let (_tempdir, mut store) = create_temp_repository().unwrap();
        let blob = Blob::from("Hello!");
        let hash = store.store_object(&blob).unwrap();

        let rev = RevSpec::from(hash);
        let result = store.find_object(&rev);
        assert_match!(result, Ok(found) if found==hash);

        let rev = RevSpec::from_str(&hash.to_short()).unwrap();
        let result = store.find_object(&rev);
        assert_match!(result, Ok(found) if found==hash);
    }
}
