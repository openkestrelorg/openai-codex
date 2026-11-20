#![cfg(target_os = "linux")]

use codex_protocol::models::PermissionProfile;
use codex_utils_cargo_bin::cargo_bin;

#[test]
fn inner_stage_refuses_to_run_without_a_bubblewrap_parent_pidfd() {
    let profile = serde_json::to_string(&PermissionProfile::read_only()).expect("profile");
    let output =
        std::process::Command::new(cargo_bin("codex-linux-sandbox").expect("sandbox helper"))
            .args(["--sandbox-policy-cwd", "/", "--permission-profile"])
            .arg(profile)
            .args([
                "--apply-seccomp-then-exec",
                "--",
                "/bin/sh",
                "-c",
                "printf command-ran",
            ])
            .output()
            .expect("sandbox helper should execute");
    assert!(!output.status.success());
    assert_eq!(output.stdout, Vec::<u8>::new());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("refusing to run the inner sandbox stage without --bwrap-parent-pidfd"),
        "{stderr}"
    );
}
