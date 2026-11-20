//! Real package installation and replacement through the daemon CLI.
//! Keep these I/O-heavy cases separate from ordinary lifecycle coverage.
#![cfg(unix)]

mod support;

use anyhow::Context;
use anyhow::Result;
use anyhow::ensure;
use pretty_assertions::assert_eq;
use serde_json::Value;
use support::TestDaemon;
use support::assert_update_disabled;
use support::wait_for_exit;

#[derive(Clone, Copy, PartialEq, Eq)]
enum InitialDaemon {
    Missing,
    Legacy,
}

fn packaged_daemon_launch(action: &str, initial: InitialDaemon) -> Result<()> {
    let mut daemon = TestDaemon::new()?;
    let standalone = daemon.home.path().join("packages/standalone");
    let package = if action == "start" && initial == InitialDaemon::Missing {
        standalone.join("releases/caller")
    } else {
        daemon.home.path().join("cli-package")
    };
    for directory in ["bin", "codex-path", "codex-resources"] {
        std::fs::create_dir_all(package.join(directory))?;
    }
    // Keep the current fixture's immutable same-filesystem executable sharing.
    std::fs::hard_link(
        standalone.join("current/bin/codex"),
        package.join("bin/codex"),
    )?;
    daemon.codex = package.join("bin/codex");
    for helper in [
        "bin/codex-code-mode-host",
        "codex-path/rg",
        "codex-resources/bwrap",
    ] {
        codex_utils_cargo_bin::write_executable(&package.join(helper), "runtime fixture")?;
    }
    let target = format!(
        "{}-{}",
        std::env::consts::ARCH,
        if cfg!(target_os = "macos") {
            "apple-darwin"
        } else if cfg!(target_env = "gnu") {
            "unknown-linux-gnu"
        } else {
            "unknown-linux-musl"
        }
    );
    std::fs::write(
        package.join("codex-package.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"), "target": target, "entrypoint": "bin/codex"
        }))?,
    )?;
    if action == "start" && initial == InitialDaemon::Missing {
        std::fs::remove_file(standalone.join("current"))?;
        std::os::unix::fs::symlink(&package, standalone.join("current"))?;
    }
    let cli_selection = standalone.join("current").canonicalize()?;
    let state = daemon.home.path().join("app-server-daemon");
    if initial == InitialDaemon::Missing {
        std::fs::remove_file(state.join("app-server.stderr.log"))?;
    }
    std::fs::write(
        state.join("settings.json"),
        br#"{"shutdownGraceSeconds":0}"#,
    )?;
    let settings_before = std::fs::read(state.join("settings.json"))?;
    let marker_before = std::fs::read(standalone.join("auto-update-version"))?;
    let cli_before = daemon.codex.canonicalize()?;
    let result = daemon
        .command()
        .args(["app-server", "daemon", action])
        .output()
        .context("failed to launch packaged daemon fixture")?;
    let dedicated = daemon
        .home
        .path()
        .canonicalize()?
        .join("packages/app-server-daemon");
    assert_eq!(daemon.codex.canonicalize()?, cli_before);
    assert_eq!(standalone.join("current").canonicalize()?, cli_selection);
    daemon.assert_no_updater();
    if initial == InitialDaemon::Missing {
        // A CLI package is not authority to install a daemon implicitly.
        assert!(!result.status.success());
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(stderr.contains("daemon executable not found at"));
        assert!(!stderr.contains("Installing daemon from CLI version"));
        assert!(!dedicated.exists());
        assert!(!state.join("daemon.pid").exists());
        assert!(!state.join("app-server.pid").exists());
        assert_eq!(std::fs::read(state.join("settings.json"))?, settings_before);
        assert_eq!(
            std::fs::read(standalone.join("auto-update-version"))?,
            marker_before
        );
        return Ok(());
    }

    ensure!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let output: Value = serde_json::from_slice(&result.stdout)?;
    assert_eq!(output["status"], "started");
    assert_eq!(
        output["managedCodexPath"],
        standalone
            .canonicalize()?
            .join("current/bin/codex")
            .to_str()
            .context("managed path is not UTF-8")?
    );
    let original_pid = daemon.pid("app-server.pid")?;
    // Neither a confirmation flag nor a complete local package grants update
    // authority. Preserve the running daemon, CLI selection and settings.
    for args in [
        vec!["app-server", "daemon", "update", "--from-cli"],
        vec!["app-server", "daemon", "update", "--from-cli", "--yes"],
    ] {
        assert_update_disabled(&daemon.command().args(args).output()?);
        assert_eq!(daemon.pid("app-server.pid")?, original_pid);
        assert!(!dedicated.exists());
        daemon.assert_no_updater();
        assert_eq!(std::fs::read(state.join("settings.json"))?, settings_before);
        assert_eq!(standalone.join("current").canonicalize()?, cli_selection);
        assert_eq!(
            std::fs::read(standalone.join("auto-update-version"))?,
            marker_before
        );
        assert_eq!(
            std::fs::read(package.join("bin/codex-code-mode-host"))?,
            b"runtime fixture"
        );
    }
    assert_eq!(daemon.lifecycle("start")?["status"], "alreadyRunning");
    assert_eq!(daemon.lifecycle("stop")?["status"], "stopped");
    wait_for_exit(original_pid)?;
    Ok(())
}

#[test]
fn packaged_daemon_start_does_not_install_missing_package() -> Result<()> {
    packaged_daemon_launch("start", InitialDaemon::Missing)
}

#[test]
fn packaged_daemon_restart_does_not_install_missing_package() -> Result<()> {
    packaged_daemon_launch("restart", InitialDaemon::Missing)
}

#[test]
fn packaged_daemon_bootstrap_does_not_install_missing_package() -> Result<()> {
    packaged_daemon_launch("bootstrap", InitialDaemon::Missing)
}

#[test]
fn packaged_daemon_explicit_update_preserves_running_legacy() -> Result<()> {
    packaged_daemon_launch("start", InitialDaemon::Legacy)
}
