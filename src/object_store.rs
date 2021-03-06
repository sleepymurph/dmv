use dag::*;
use disk_backed::DiskBacked;
use error::*;
use filebuffer::FileBuffer;
use fsutil;
use human_readable::human_bytes;
use log::LogLevel;
use progress::*;
use regex::Regex;
use revisions::*;
use status::ComparableNode;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt;
use std::fs;
use std::io;
use std::io::Read;
use std::io::Write;
use std::iter;
use std::path::Path;
use std::path::PathBuf;
use std::thread;
use std::time::Instant;
use variance::VarianceCalc;
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
            .join(&key[2..])
    }

    fn object_from_path(&self, path: &Path) -> Result<ObjectKey> {
        let key_path = path.strip_prefix(&self.path)
            .and_then(|p| p.strip_prefix("objects"))?;
        let key_str = key_path.to_str()
            .expect("should be ascii")
            .replace("/", "");
        ObjectKey::parse(&key_str)
    }

    /// Check all stored objects
    ///
    /// Returns a list of bad objects: file name key vs actual hash
    pub fn fsck(&self) -> Result<Vec<(ObjectKey, ObjectKey)>> {
        let mut obj_count = 0;
        let mut total_bytes = 0;
        for dir1 in fs::read_dir(self.path.join("objects"))? {
            let dir1 = dir1?;
            for obj_file in fs::read_dir(dir1.path())? {
                let obj_file = obj_file?;
                obj_count += 1;
                total_bytes += obj_file.metadata()?.len();
            }
        }

        stderrln!("{} objects, {}", obj_count, human_bytes(total_bytes));

        let prog = ProgressCounter::arc("Verifying", total_bytes);
        let prog_clone = prog.clone();
        let prog_thread = thread::spawn(move || std_err_watch(prog_clone));
        let mut bad_hashes = Vec::new();

        let mut size_stats = VarianceCalc::new();
        let mut stats_by_type = BTreeMap::<ObjectType, VarianceCalc>::new();
        for t in &[ObjectType::Blob,
                   ObjectType::ChunkedBlob,
                   ObjectType::Tree,
                   ObjectType::Commit] {
            stats_by_type.insert(*t, VarianceCalc::new());
        }

        for dir1 in fs::read_dir(self.path.join("objects"))? {
            let dir1 = dir1?;
            for obj_file in fs::read_dir(dir1.path())? {
                let obj_file = obj_file?;

                let size = obj_file.metadata()?.len();
                size_stats.item(size as i64);

                let hash = self.object_from_path(&obj_file.path())?;
                let obj_file = FileBuffer::open(&obj_file.path())?;
                let mut obj_file = ProgressReader::new(&*obj_file, &prog);
                let mut hasher = HashWriter::wrap(io::sink());

                let mut header_buf = [0u8; 12];
                obj_file.read_exact(&mut header_buf)?;
                let object_type =
                    ObjectHeader::read_from(&mut header_buf.as_ref())
                        ?
                        .object_type;
                hasher.write_all(header_buf.as_ref())?;
                stats_by_type.get_mut(&object_type)
                    .unwrap()
                    .item(size as i64);


                io::copy(&mut obj_file, &mut hasher)?;
                let actual = hasher.hash();
                if actual != hash {
                    warn!("Corrupt object {0}: expected {0:x}, actual \
                               {1:x}",
                          hash,
                          actual);
                    bad_hashes.push((hash, actual));
                }
            }
        }
        prog.finish();
        prog_thread.join().unwrap();

        println!("{:4}  {:>10} {:^23} {:^23}", "", "count", "mean", "std");
        for (type_str, size_stats) in stats_by_type.iter()
            .map(|(t, s)| (t.code(), s))
            .chain(iter::once(("all", &size_stats))) {
            println!("{:4}: {:10} {:10.1} ({:>10}) {:10.1} ({:>10})",
                     type_str,
                     size_stats.count(),
                     size_stats.mean(),
                     human_bytes(size_stats.mean().round() as u64),
                     size_stats.std(),
                     human_bytes(size_stats.std().round() as u64));
        }
        Ok(bad_hashes)
    }

    pub fn has_object(&self, key: &ObjectKey) -> bool {
        self.object_path(key).is_file()
    }

    /// Look up a RevSpec
    ///
    /// Returns a three-value tuple:
    ///
    /// - Hash of exact object specified, which may be a subtree of a commit
    /// - Hash of the commit specified
    /// - The ref name specified
    ///
    pub fn lookup(&self,
                  rev: &RevSpec)
                  -> Result<(ObjectKey, ObjectKey, Option<RevNameBuf>)> {
        self.lookup_ref_or_hash(&rev.rev_name)
            .and_then(|(hash, ref_name)| match &rev.path {
                &None => Ok((hash, hash, ref_name)),
                &Some(ref path) => {
                    self.lookup_rev_path(&hash, path)
                        .map(|subtree| (subtree, hash, ref_name))
                }
            })
    }

    fn lookup_ref_or_hash(&self,
                          name: &RevNameStr)
                          -> Result<(ObjectKey, Option<RevNameBuf>)> {
        self.lookup_ref(name)
            .map(|hash| (hash, Some(name.to_owned())))
            .or_else(|_| self.lookup_short_hash(name).map(|hash| (hash, None)))
    }

    fn lookup_ref(&self, rev: &RevNameStr) -> Result<ObjectKey> {
        self.refs
            .get(rev)
            .map(|hash| *hash)
            .ok_or_else(|| format!("Not a ref: {}", rev).into())
    }

    fn lookup_short_hash(&self, s: &RevNameStr) -> Result<ObjectKey> {
        fn get_fn_str(path: &Path) -> &str {
            path.file_name()
                .expect("should have a file_name")
                .to_str()
                .expect("should be ascii")
        }

        let path = self.object_path_sloppy(s);
        let dir = path.parent_or_err()?;
        let short_name = get_fn_str(&path);

        if dir.exists() {
            for entry in dir.read_dir()? {
                let entry = entry?.path();
                trace!("Looking for '{}', checking: {}",
                       s,
                       entry.strip_prefix(&self.path)?.display());
                if get_fn_str(&entry).starts_with(&short_name) {
                    return self.object_from_path(&entry);
                }
            }
        }
        Err(format!("Hash not found: {}", s).into())
    }

    pub fn lookup_rev_path(&self,
                           hash: &ObjectKey,
                           path: &Path)
                           -> Result<ObjectKey> {
        use std::path::Component;
        let mut components = path.components();
        match components.next() {

                None => Ok(*hash),

                Some(Component::Normal(ch_path)) => {
                    self.open_tree(hash)
                        .and_then(|tree| {
                            tree.get(ch_path)
                                .map(|ch_hash| *ch_hash)
                                .ok_or_else(|| {
                                    format!("Tree {} has no child {}",
                                            hash,
                                            ch_path.to_string_lossy())
                                        .into()
                                })
                        })
                        .and_then(|ch_hash| {
                            self.lookup_rev_path(&ch_hash, components.as_path())
                        })
                }

                Some(other) => {
                    Err(format!("Unexpected component type {:?} in path: {}",
                                other,
                                path.display())
                        .into())
                }
            }
            .chain_err(|| format!("Could not open {}:{}", hash, path.display()))
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
        self.open_object_file(key)
            .and_then(|file| ObjectHandle::read_header(Box::new(file)))
            .chain_err(|| format!("Could not open object {}", key))
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
            .chain_err(|| format!("Could not open object {}", key))
    }

    pub fn open_tree(&self, key: &ObjectKey) -> Result<Tree> {
        match self.open_object(key) {
                Ok(ObjectHandle::Tree(raw)) => raw.read_content(),
                Ok(ObjectHandle::Commit(raw)) => {
                    raw.read_content()
                        .and_then(|commit| self.open_tree(&commit.tree))
                }
                Ok(other) => {
                    bail!("{} is a {:?}. Expected a tree.",
                          key,
                          other.header().object_type)
                }
                Err(e) => Err(e),
            }
            .chain_err(|| format!("Could not open object {}", key))
    }

    /// Writes a single object into the object store
    ///
    /// Returns the hash key of the object
    pub fn store_object(&self, obj: &ObjectCommon) -> Result<ObjectKey> {

        // If object already exists, no need to store
        let key = obj.calculate_hash();
        if self.has_object(&key) {
            trace!("store {} {} -- already exists",
                   obj.object_type().code(),
                   key);
            return Ok(key);
        }

        let start_time = Instant::now();

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

        let elapsed = Instant::now().duration_since(start_time);
        let secs = elapsed.as_secs() as f32 +
                   elapsed.subsec_nanos() as f32 / 1e9;
        let per_sec = obj.content_size() as f32 / secs;

        let log_level = match secs {
            _ if secs > 1.0 => LogLevel::Warn,
            _ if secs > 0.5 => LogLevel::Info,
            _ if secs > 0.010 => LogLevel::Debug,
            _ => LogLevel::Trace,
        };
        log!(log_level,
             "store {} {} -- {:>10} stored in {:0.3}s ({:>10}/s)",
             obj.object_type().code(),
             key,
             human_bytes(obj.content_size()),
             secs,
             human_bytes(per_sec as u64));
        Ok(key)
    }


    /// Give a Display object that will walk the tree and list its contents
    pub fn ls_files(&self,
                    hash: ObjectKey,
                    verbose: bool)
                    -> Result<TreeDisplay> {
        Ok(TreeDisplay {
            node: self.lookup_node(hash)?,
            object_store: self,
            verbose: verbose,
        })
    }


    /// Extract binary content from a Blob or ChunkedBlob to a Write stream
    pub fn copy_blob_content(&self,
                             hash: &ObjectKey,
                             writer: &mut io::Write)
                             -> Result<()> {
        match self.open_object(hash)? {
            ObjectHandle::Blob(blob) => {
                trace!("Extracting blob {}", hash);
                blob.copy_content(writer)?;
            }
            ObjectHandle::ChunkedBlob(index) => {
                debug!("Reading ChunkedBlob {}", hash);
                let index =
                    index.read_content()
                        .chain_err(|| {
                            format!("While reading ChunkedBlob {}", hash)
                        })?;
                for offset in index.chunks {
                    debug!("{}", offset);
                    self.copy_blob_content(&offset.hash, writer)?;
                }
            }
            other => bail!("Expected a Blob or ChunkedBlob, got: {:?}", other),
        };
        Ok(())
    }

    /// Get all refs
    pub fn refs(&self) -> &RefMap { &self.refs }

    pub fn refs_for(&self, hash: &ObjectKey) -> Vec<String> {
        self.refs
            .iter()
            .filter(|&(_, v)| v == hash)
            .map(|(k, _)| k.to_owned())
            .collect::<Vec<_>>()
    }

    pub fn update_ref(&mut self, name: String, hash: ObjectKey) -> Result<()> {
        self.refs.insert(name, hash);
        self.refs.flush().map_err(|e| e.into())
    }

    pub fn try_find_ref(&self, name: &str) -> Option<ObjectKey> {
        self.refs.get(name).cloned()
    }

    pub fn find_common_ancestor<I, S>(&self,
                                      revs: I)
                                      -> Result<Option<ObjectKey>>
        where I: IntoIterator<Item = S>,
              S: AsRef<str>
    {
        let mut search_queues = Vec::new();
        for rev in revs {
            let (hash, _) = self.lookup_ref_or_hash(rev.as_ref())?;

            let mut queue = VecDeque::new();
            queue.push_back(hash);
            search_queues.push(queue);
        }
        search_queues.dedup();

        let mut seen_sets = vec![HashSet::new(); search_queues.len()];
        let mut depth = 0;
        loop {
            depth += 1;

            let mut seen_a_hash = false;
            for (i, queue) in search_queues.iter_mut().enumerate() {
                match queue.pop_front() {
                    None => (),
                    Some(hash) => {
                        seen_a_hash = true;
                        seen_sets[i].insert(hash);
                        if seen_sets.iter().all(|set| set.contains(&hash)) {
                            debug!("Ancestor: Found it! {}", hash);
                            return Ok(Some(hash));
                        }
                        let commit = self.open_commit(&hash)?;
                        trace!("Ancestor: Round {}, Slot {}: {} {}",
                               depth,
                               i,
                               hash,
                               commit.message);
                        queue.extend(commit.parents);
                    }
                }
            }
            if !seen_a_hash {
                debug!("Ancestor: end of history. No common ancestor found.");
                return Ok(None);
            }
        }
    }
}

lazy_static!{
    pub static ref SHORT_OBJECT_KEY_PAT:Regex = Regex::new(
        &format!("[[:xdigit:]]{{ {},{} }}",
                    KEY_SHORT_LEN, KEY_SIZE_HEX_DIGITS)).unwrap();

    pub static ref REF_NAME_PAT:Regex = Regex::new("[[:word:]/-]+").unwrap();
}


#[derive(Debug,Clone,Copy)]
pub struct ObjectWalkNode {
    pub hash: ObjectKey,
    pub object_type: ObjectType,
    pub file_size: ObjectSize,
}

impl Into<ComparableNode> for ObjectWalkNode {
    fn into(self) -> ComparableNode {
        ComparableNode {
            is_treeish: self.object_type.is_treeish(),
            file_size: self.file_size,
            hash: Some(self.hash),
            fs_path: None,
            is_ignored: false,
        }
    }
}


impl NodeLookup<ObjectKey, ObjectWalkNode> for ObjectStore {
    fn lookup_node(&self, handle: ObjectKey) -> Result<ObjectWalkNode> {
        let opened = self.open_object(&handle)?;
        let object_type = opened.header().object_type;
        let file_size;
        match opened {
            ObjectHandle::Blob(_) => file_size = opened.header().content_size,
            ObjectHandle::ChunkedBlob(raw) => {
                let chunked = raw.read_content()?;
                file_size = chunked.total_size
            }
            _ => file_size = 0,
        }
        Ok(ObjectWalkNode {
            hash: handle,
            object_type: object_type,
            file_size: file_size,
        })
    }
}

impl NodeReader<ObjectWalkNode> for ObjectStore {
    fn read_children(&self,
                     node: &ObjectWalkNode)
                     -> Result<ChildMap<ObjectWalkNode>> {
        let mut children = BTreeMap::new();
        for (name, hash) in self.open_tree(&node.hash)? {
            let name = name.into_string()
                .map_err(|e| format!("Bad UTF-8 in name: {:?}", e))?;
            let node = self.lookup_node(hash.clone())?;
            children.insert(name, node);
        }
        Ok(children)
    }
}


impl NodeLookup<ObjectKey, ComparableNode> for ObjectStore {
    fn lookup_node(&self, handle: ObjectKey) -> Result<ComparableNode> {
        let node = <Self as NodeLookup<ObjectKey,ObjectWalkNode>>
                    ::lookup_node(&self, handle)?;
        Ok(node.into())
    }
}

impl NodeReader<ComparableNode> for ObjectStore {
    fn read_children(&self,
                     node: &ComparableNode)
                     -> Result<ChildMap<ComparableNode>> {
        let mut children = BTreeMap::new();
        for (name, hash) in
            self.open_tree(&node.hash.expect("Object should have hash"))? {
            let name = name.into_string()
                .map_err(|e| format!("Bad UTF-8 in name: {:?}", e))?;
            let node = self.lookup_node(hash.clone())?;
            children.insert(name, node);
        }
        Ok(children)
    }
}


pub struct DepthFirstCommitSort<'a> {
    object_store: &'a ObjectStore,
    unvisited: Vec<ObjectKey>,
    visited: HashSet<ObjectKey>,
    sorted: Vec<(ObjectKey, Commit)>,
}
impl<'a> DepthFirstCommitSort<'a> {
    pub fn new(object_store: &'a ObjectStore,
               unvisited: Vec<ObjectKey>)
               -> Self {
        DepthFirstCommitSort {
            object_store: object_store,
            unvisited: unvisited,
            visited: HashSet::new(),
            sorted: Vec::new(),
        }
    }

    pub fn run(mut self) -> Result<Vec<(ObjectKey, Commit)>> {
        debug!("Sorting commits, starting with: {:?}", self.unvisited);
        for hash in &self.unvisited.clone() {
            self.visit(&hash)?;
        }
        Ok(self.sorted)
    }

    fn visit(&mut self, hash: &ObjectKey) -> Result<()> {
        if self.visited.contains(hash) {
            trace!("Already visited: {}", hash);
            return Ok(());
        }
        self.visited.insert(*hash);
        let commit = self.object_store.open_commit(hash)?;
        trace!("Visiting {} {}, parents: {:?}",
               hash,
               commit.message,
               commit.parents);
        for parent in &commit.parents {
            self.visit(parent)?;
        }
        trace!("Pushing  {} {}", hash, commit.message);
        self.sorted.push((*hash, commit));
        Ok(())
    }
}


/// A wrapper to Display a LsTree, with options
pub struct TreeDisplay<'a> {
    object_store: &'a ObjectStore,
    node: ObjectWalkNode,
    verbose: bool,
}
impl<'a> fmt::Display for TreeDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut op = TreeDisplayOp {
            formatter: f,
            verbose: self.verbose,
        };
        match self.object_store.walk_node(&mut op, self.node) {
            Ok(_) => Ok(()),
            Err(_) => Err(fmt::Error),
        }
    }
}


/// An operation that walks an Object Tree to Display it
struct TreeDisplayOp<'s, 'f: 's> {
    formatter: &'s mut fmt::Formatter<'f>,
    verbose: bool,
}
impl<'a, 'b> WalkOp<ObjectWalkNode> for TreeDisplayOp<'a, 'b> {
    type VisitResult = ();

    fn should_descend(&mut self, _ps: &Path, node: &ObjectWalkNode) -> bool {
        node.object_type.is_treeish()
    }

    fn no_descend(&mut self,
                  ps: &Path,
                  node: ObjectWalkNode)
                  -> Result<Option<Self::VisitResult>> {
        if self.verbose {
            writeln!(self.formatter,
                     "{} {} {}",
                     node.hash,
                     node.object_type.code(),
                     ps.display())?;
        } else {
            writeln!(self.formatter, "{}", ps.display())?;
        }
        Ok(None)
    }
}


#[cfg(test)]
pub mod test {
    use dag::Blob;
    use dag::Object;
    use dag::ToHashed;
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
        let (_tempdir, store) = create_temp_repository().unwrap();

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

        let result = store.update_ref("master".to_string(), hash);
        assert_match!(result, Ok(()));

        let result = store.try_find_ref("master");
        assert_match!(result, Some(x) if x==hash);
    }
}
