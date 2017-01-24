use std::fs;
use std::io;
use std::iter;
use std::path;

pub struct DirsDepthFirst {
    stack: Vec<(fs::DirEntry, fs::ReadDir)>,
}

enum DirsAction {
    Finished,
    Ascend,
    Descend(fs::DirEntry),
    EmitErr(io::Error),
}

macro_rules! bizarro_try {
    ($e:expr) => {
        match $e {
            Ok(t) => t,
            Err(e) => { return Some(Err(e)); }
        }
    }
}

fn find_next_dir(iter: &mut fs::ReadDir) -> Option<io::Result<fs::DirEntry>> {
    while let Some(entry) = iter.next() {
        let entry = bizarro_try!(entry);
        let file_type = bizarro_try!(entry.file_type());
        if file_type.is_dir() {
            return Some(Ok(entry));
        }
    }
    None
}

impl iter::Iterator for DirsDepthFirst {
    type Item = io::Result<fs::DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let action = match self.stack.last_mut() {
                None => DirsAction::Finished,
                Some(&mut (_, ref mut iter)) => {
                    match find_next_dir(iter) {
                        None => DirsAction::Ascend,
                        Some(Err(e)) => DirsAction::EmitErr(e),
                        Some(Ok(dir)) => DirsAction::Descend(dir),
                    }
                }
            };
            match action {
                DirsAction::Finished => {
                    return None;
                }
                DirsAction::EmitErr(e) => {
                    return Some(Err(e));
                }
                DirsAction::Ascend => {
                    let (dir, _) = self.stack.pop().expect("safe to unwrap");
                    return Some(Ok(dir));
                }
                DirsAction::Descend(entry) => {
                    let iter = bizarro_try!(entry.path().read_dir());
                    self.stack.push((entry, iter));
                }
            }
        }
    }
}
