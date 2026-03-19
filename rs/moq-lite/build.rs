use std::process::Command;

fn main() {
    // Embed the short git hash at compile time.
    let hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();
    println!("cargo:rustc-env=MOQ_GIT_HASH={}", hash.trim());
}
