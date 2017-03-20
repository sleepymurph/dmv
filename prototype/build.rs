use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();

    let dest_path = Path::new(&out_dir).join("project_git_log.txt");
    let mut f = File::create(&dest_path).unwrap();

    let output = Command::new("git").arg("log").arg("-n1").arg("--oneline")
        .output().expect("could not get current Git version");

    f.write_all(&output.stdout).unwrap();
}