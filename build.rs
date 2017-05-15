use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    // Save git version
    let output = Command::new("git").arg("rev-parse").arg("--short").arg("HEAD")
        .output().expect("could not get current Git version");

    let stdout = String::from_utf8(output.stdout).unwrap();

    let dest_path = Path::new(&out_dir).join("project_git_log.txt");
    let mut f = File::create(&dest_path).unwrap();
    f.write_all(stdout.trim().as_bytes()).unwrap();

    // Save build profile
    let dest_path = Path::new(&out_dir).join("build_profile.txt");
    let mut f = File::create(&dest_path).unwrap();
    f.write_all(env::var("PROFILE").unwrap().as_bytes()).unwrap();
}
