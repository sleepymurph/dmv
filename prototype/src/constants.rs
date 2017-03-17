pub const PROJECT_NAME: &'static str = "prototype";
pub const HIDDEN_DIR_NAME: &'static str = ".prototype";
pub const CACHE_FILE_NAME: &'static str = ".prototype_cache";

pub const DEFAULT_BRANCH_NAME: &'static str = "master";

/// A hard-coded branch name
///
/// TODO: Allow multiple branches, read current branch from WorkDir
pub const HARDCODED_BRANCH: &'static str = "master";

pub const PROJECT_GIT_LOG: &'static str =
    include_str!(concat!(env!("OUT_DIR"), "/project_git_log.txt"));
