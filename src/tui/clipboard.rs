//! Clipboard without a platform clipboard crate (arboard pulled in the objc2
//! stack on macOS and x11rb/wayland on Linux).
//!
//! There is no single mechanism that works everywhere, so try them in order
//! of reliability:
//!
//! 1. the platform helper (`pbcopy` and friends), skipped over ssh where it
//!    would set the clipboard of the wrong machine;
//! 2. `tmux load-buffer -w`, since tmux drops OSC 52 coming from applications
//!    unless `set-clipboard` is `on` (the default is `external`), while its
//!    own clipboard path is allowed under `external` too;
//! 3. OSC 52, the only thing that can reach the local clipboard over ssh.

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use std::io::Write;
use std::process::{Command, Stdio};

/// Terminals that do implement OSC 52 usually cap the payload (xterm's
/// default is 1MB of base64), and a truncated paste is worse than none.
const MAX_LEN: usize = 512 * 1024;

#[cfg(target_os = "macos")]
const HELPERS: &[&[&str]] = &[&["pbcopy"]];
#[cfg(not(target_os = "macos"))]
const HELPERS: &[&[&str]] = &[
    &["wl-copy"],
    &["xclip", "-selection", "clipboard"],
    &["xsel", "--clipboard", "--input"],
];

pub fn set_text(text: &str) -> Result<()> {
    let over_ssh = std::env::var_os("SSH_CONNECTION").is_some()
        || std::env::var_os("SSH_TTY").is_some()
        || std::env::var_os("SSH_CLIENT").is_some();

    if !over_ssh {
        for helper in HELPERS {
            if pipe_to(helper, text) {
                return Ok(());
            }
        }
    }

    if std::env::var_os("TMUX").is_some() && pipe_to(&["tmux", "load-buffer", "-w", "-"], text) {
        return Ok(());
    }

    osc52(text)
}

/// Feeds the text to `argv` over stdin, true if it took it.
fn pipe_to(argv: &[&str], text: &str) -> bool {
    let child = Command::new(argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn();
    let mut child = match child {
        Ok(child) => child,
        // Not installed, or no $DISPLAY/$WAYLAND_DISPLAY to talk to
        Err(e) => {
            log::debug!("clipboard: {} is unusable: {}", argv[0], e);
            return false;
        }
    };

    // The pipe has to be closed before wait(), otherwise the helper keeps
    // reading and both sides block.
    let written = child
        .stdin
        .take()
        .map(|mut stdin| stdin.write_all(text.as_bytes()).is_ok())
        .unwrap_or(false);

    match child.wait() {
        Ok(status) => written && status.success(),
        Err(_) => false,
    }
}

fn osc52(text: &str) -> Result<()> {
    if text.len() > MAX_LEN {
        anyhow::bail!(
            "text is too large for OSC 52 ({} bytes, limit is {})",
            text.len(),
            MAX_LEN
        );
    }

    // "c" is the clipboard selection, ST (ESC \) terminates
    let seq = format!("\x1b]52;c;{}\x1b\\", BASE64.encode(text));

    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(seq.as_bytes())
        .and_then(|_| stdout.flush())
        .context("failed to write OSC 52 sequence")
}
