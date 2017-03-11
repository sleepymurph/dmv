use dag::*;
use disk_backed::DiskBacked;
use error::*;
use fsutil;
use regex::Regex;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::iter::Iterator;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use walker::*;

type RefMap = BTreeMap<String, ObjectKey>;

pub struct ObjectStore {
    path: PathBuf,
    refs: DiskBacked<RefMap>,
}

impl ObjectStore {
    pub fn init(path: PathBuf) -> Result<Self> {
        try!(fs::create_dir_all(&path));
        Self::open(path)
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        Ok(ObjectStore {
            refs: DiskBacked::read_or_default("refs", path.join("refs"))?,
            path: path,
        })
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
        let key_path = path.strip_prefix(&self.path)
            .and_then(|p| p.strip_prefix("objects"))?;
        let key_str = key_path.to_str()
            .expect("should be ascii")
            .replace("/", "");
        ObjectKey::parse(&key_str)
    }

    pub fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    pub fn find_object(&self, rev: &RevSpec) -> Result<ObjectKey> {
        match self.try_find_object(rev) {
            Ok(Some(x)) => Ok(x),
            Ok(None) => bail!(ErrorKind::RevNotFound(rev.to_owned())),
            Err(e) => bail!(e),
        }
    }

    pub fn try_find_object(&self, rev: &RevSpec) -> Result<Option<ObjectKey>> {
        match *rev {
            RevSpec::Hash(ref hash) => {
                match self.has_object(hash) {
                    true => Ok(Some(hash.to_owned())),
                    false => Ok(None),
                }
            }
            RevSpec::ShortHash(ref s) => {
                match self.try_find_short_hash(s) {
                    Ok(None) => Ok(self.try_find_ref(s)),
                    other => other,
                }
            }
            RevSpec::Ref(ref s) => Ok(self.try_find_ref(s)),
        }
    }

    fn try_find_short_hash(&self, s: &str) -> Result<Option<ObjectKey>> {
        fn get_fn_str(path: &Path) -> &str {
            path.file_name()
                .expect("should have a file_name")
                .to_str()
                .expect("should be ascii")
        }

        let path = self.object_path_sloppy(s);
        let dir = path.parent_or_err()?;
        let short_name = get_fn_str(&path);

        if !dir.exists() {
            return Ok(None);
        } else {
            for entry in dir.read_dir()? {
                let entry = entry?.path();
                trace!("Looking for '{}', checking: {}",
                       s,
                       entry.strip_prefix(&self.path)?.display());
                if get_fn_str(&entry).starts_with(&short_name) {
                    return Ok(Some(self.object_from_path(&entry)?));
                }
            }
            return Ok(None);
        }
    }

    pub fn open_object_file(&self,
                            key: &ObjectKey)
                            -> Result<io::BufReader<fs::File>> {

        if !self.has_object(&key) {
            bail!(ErrorKind::ObjectNotFound(key.to_owned()))
        }

        let file = fs::File::open(self.object_path(key))?;
        Ok(io::BufReader::new(file))
    }

    pub fn open_object(&self, key: &ObjectKey) -> Result<ObjectHandle> {
        let file = self.open_object_file(key)?;
        ObjectHandle::read_header(Box::new(file))
    }

    pub fn open_commit(&self, key: &ObjectKey) -> Result<Commit> {
        match self.open_object(key) {
            Ok(ObjectHandle::Commit(raw)) => raw.read_content(),
            Ok(other) => {
                bail!("{} is a {:?}. Expected a commit.",
                      key,
                      other.header().object_type)
            }
            Err(e) => Err(e),
        }
    }

    pub fn open_tree(&self, key: &ObjectKey) -> Result<Tree> {
        match self.open_object(key) {
            Ok(ObjectHandle::Tree(raw)) => raw.read_content(),
            Ok(ObjectHandle::Commit(raw)) => {
                let commit = raw.read_content()?;
                self.open_tree(&commit.tree)
            }
            Ok(other) => {
                bail!("{} is a {:?}. Expected a tree.",
                      key,
                      other.header().object_type)
            }
            Err(e) => Err(e),
        }
    }

    pub fn try_find_tree_path(&self,
                              key: &ObjectKey,
                              path: &Path)
                              -> Result<Option<ObjectKey>> {
        use std::path::Component;

        let mut next_key = key.to_owned();

        if path == PathBuf::from("") {
            match self.open_object(&next_key)? {
                ObjectHandle::Commit(raw) => {
                    return raw.read_content().map(|commit| Some(commit.tree));
                }
                _ => return Ok(Some(next_key)),
            }
        }

        let mut path_so_far = PathBuf::new();
        for component in path.components() {
            let tree = self.open_tree(&next_key)
                .chain_err(|| {
                    format!("While trying to open {}/{}",
                            key,
                            path_so_far.display())
                })?;
            match component {
                Component::Normal(child_path) => {
                    path_so_far.push(&child_path);
                    match tree.get(child_path) {
                        Some(child_key) => next_key = child_key.to_owned(),
                        None => return Ok(None),
                    }
                }
                _ => {
                    bail!("Unexpected path component type '{:?}' in path '{}'",
                          component,
                          path.display())
                }
            }
        }
        Ok(Some(next_key))
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


    /// Get all refs
    pub fn refs(&self) -> &RefMap { &self.refs }

    pub fn refs_for(&self, hash: &ObjectKey) -> Vec<&str> {
        self.refs
            .iter()
            .filter(|&(_, v)| v == hash)
            .map(|(k, _)| k.as_str())
            .collect::<Vec<&str>>()
    }

    pub fn update_ref<S, O>(&mut self, name: S, hash: O) -> Result<()>
        where S: Into<String>,
              O: Into<ObjectKey>
    {
        self.refs.insert(name.into(), hash.into());
        self.refs.flush().map_err(|e| e.into())
    }

    pub fn try_find_ref(&self, name: &str) -> Option<ObjectKey> {
        self.refs.get(name).cloned()
    }

    pub fn log(&self, start: &RevSpec) -> Result<Commits> {
        Ok(Commits {
            object_store: &self,
            next: self.try_find_object(start)?,
        })
    }
}

lazy_static!{
    pub static ref SHORT_OBJECT_KEY_PAT:Regex = Regex::new(
        &format!("[[:xdigit:]]{{ {},{} }}",
                    KEY_SHORT_LEN, KEY_SIZE_HEX_DIGITS-1)).unwrap();

    pub static ref REF_NAME_PAT:Regex = Regex::new("[[:word:]/-]+").unwrap();
}


pub type ObjectWalkNode = (ObjectKey, ObjectType);

impl NodeLookup<ObjectKey, ObjectWalkNode> for ObjectStore {
    fn lookup_node(&self, handle: ObjectKey) -> Result<ObjectWalkNode> {
        let object_type = self.open_object(&handle)?.header().object_type;
        Ok((handle, object_type))
    }
}

impl NodeReader<ObjectWalkNode> for ObjectStore {
    fn read_children(&self,
                     node: &ObjectWalkNode)
                     -> Result<ChildMap<ObjectWalkNode>> {
        let mut children = BTreeMap::new();
        for (name, hash) in self.open_tree(&node.0)? {
            let name = name.into_string()
                .map_err(|e| format!("Bad UTF-8 in name: {:?}", e))?;
            let node = self.lookup_node(hash.clone())?;
            children.insert(name, node);
        }
        Ok(children)
    }
}


#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum RevSpec {
    Hash(ObjectKey),
    ShortHash(String),
    Ref(String),
}

impl FromStr for RevSpec {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {

        if OBJECT_KEY_PAT.is_match(s) {
            ObjectKey::parse(s).map(|h| RevSpec::Hash(h))

        } else if SHORT_OBJECT_KEY_PAT.is_match(s) {
            Ok(RevSpec::ShortHash(s.to_owned()))

        } else if REF_NAME_PAT.is_match(s) {
            Ok(RevSpec::Ref(s.to_owned()))

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
            RevSpec::Ref(ref refname) => write!(f, "{}", refname),
        }
    }
}


/// Iterator over commits
pub struct Commits<'a> {
    object_store: &'a ObjectStore,
    next: Option<ObjectKey>,
}

impl<'a> Iterator for Commits<'a> {
    type Item = Result<(ObjectKey, Commit, Vec<&'a str>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next.map(|hash| {
            let result = self.object_store.open_commit(&hash);
            if let &Ok(ref commit) = &result {
                self.next = match commit.parents.len() {
                    0 => None,
                    1 => Some(commit.parents[0]),
                    _ => unimplemented!(),
                }
            }
            result.map(|commit| {
                (hash, commit, self.object_store.refs_for(&hash))
            })
        })
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

        let result = store.try_find_ref("master");
        assert_match!(result, None);

        let result = store.update_ref("master", &hash);
        assert_match!(result, Ok(()));

        let result = store.try_find_ref("master");
        assert_match!(result, Some(x) if x==hash);
    }


    #[test]
    fn test_rev_spec() {
        let hash = ObjectKey::parse("da39a3ee5e6b4b0d3255bfef95601890afd80709")
            .unwrap();

        let rev = RevSpec::from_str("da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assert_match!(rev, Ok(RevSpec::Hash(ref h)) if h==&hash);

        let rev = RevSpec::from_str(&hash.to_short());
        assert_match!(rev, Ok(RevSpec::ShortHash(ref s)) if s=="da39a3ee");

        let rev = RevSpec::from_str("master");
        assert_match!(rev, Ok(RevSpec::Ref(ref s)) if s=="master");
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

        store.update_ref("master", &hash).unwrap();
        let rev = RevSpec::from_str("master").unwrap();
        let result = store.find_object(&rev);
        assert_match!(result, Ok(found) if found==hash);

        // Mistaken identity: ref that could be a short hash
        store.update_ref("abad1dea", &hash).unwrap();
        let rev = RevSpec::from_str("abad1dea").unwrap();
        let result = store.find_object(&rev);
        assert_match!(result, Ok(found) if found==hash);
    }
}
