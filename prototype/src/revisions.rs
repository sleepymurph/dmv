//! Ways of specifying revisions

use error::*;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;

/// The name of a revision or object: either a ref name or a hash
///
/// Needs to be checked agaist the object store to see if it actually exists
pub type RevNameBuf = String;
pub type RevNameStr = str;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct RevSpec {
    pub rev_name: RevNameBuf,
    pub path: Option<PathBuf>,
}
impl FromStr for RevSpec {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        let could_not_parse =
            || Error::from(format!("Could not parse revision \"{}\"", s));

        let mut split = s.split(":");
        let rev_name = split.next().ok_or_else(&could_not_parse)?.to_owned();
        let path = split.next().map(|s| PathBuf::from(s));
        if split.next().is_some() {
            return Err(could_not_parse());
        }
        Ok(RevSpec {
            rev_name: rev_name,
            path: path,
        })
    }
}
impl fmt::Display for RevSpec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &RevSpec { ref rev_name, path: None } => write!(f, "{}", rev_name),
            &RevSpec { ref rev_name, path: Some(ref path) } => {
                write!(f, "{}:{}", rev_name, path.display())
            }
        }
    }
}
