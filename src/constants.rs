pub const HIDDEN_DIR_NAME: &'static str = ".dmv";
pub const CACHE_FILE_NAME: &'static str = ".dmv_cache";

pub const DEFAULT_BRANCH_NAME: &'static str = "master";

pub const PROJECT_GIT_LOG: &'static str =
    include_str!(concat!(env!("OUT_DIR"), "/project_git_log.txt"));

pub const BUILD_PROFILE: &'static str =
    include_str!(concat!(env!("OUT_DIR"), "/build_profile.txt"));
