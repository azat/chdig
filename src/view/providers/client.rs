use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    utils::TerminalRawModeGuard,
    view::ViewProvider,
};
use cursive::{Cursive, views::Dialog};
use percent_encoding::percent_decode;
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::process::Command;

/// Parse a clickhouse-rs duration string (e.g. "600s", "500ms") into microseconds.
fn parse_duration_us(s: &str) -> Option<u64> {
    if let Some(ms) = s.strip_suffix("ms") {
        ms.parse::<u64>().ok().map(|v| v * 1_000)
    } else if let Some(secs) = s.strip_suffix('s') {
        secs.parse::<u64>().ok().map(|v| v * 1_000_000)
    } else {
        s.parse::<u64>().ok().map(|v| v * 1_000_000)
    }
}

pub struct ClientViewProvider;

impl ClientViewProvider {
    #[cfg(unix)]
    fn spawn_and_wait(cmd: &mut Command) -> std::io::Result<std::process::ExitStatus> {
        // Ignore SIGINT/SIGTTOU: SIGINT because we're no longer the foreground
        // group (child is), SIGTTOU because tcsetpgrp from a background group
        // would otherwise stop us.
        let prev_sigint = unsafe { libc::signal(libc::SIGINT, libc::SIG_IGN) };
        let prev_sigttou = unsafe { libc::signal(libc::SIGTTOU, libc::SIG_IGN) };

        let result = cmd.spawn().and_then(|mut child| {
            let child_pid = child.id() as libc::pid_t;
            unsafe { libc::tcsetpgrp(libc::STDIN_FILENO, child_pid) };
            let status = child.wait();
            unsafe { libc::tcsetpgrp(libc::STDIN_FILENO, libc::getpgrp()) };
            status
        });

        unsafe { libc::signal(libc::SIGTTOU, prev_sigttou) };
        unsafe { libc::signal(libc::SIGINT, prev_sigint) };

        result
    }

    #[cfg(not(unix))]
    fn spawn_and_wait(cmd: &mut Command) -> std::io::Result<std::process::ExitStatus> {
        cmd.spawn().and_then(|mut child| child.wait())
    }
}

impl ViewProvider for ClientViewProvider {
    fn name(&self) -> &'static str {
        "Client"
    }

    fn view_type(&self) -> ChDigViews {
        ChDigViews::Client
    }

    fn show(&self, siv: &mut Cursive, context: ContextArc) {
        let options = context.lock().unwrap().options.clickhouse.clone();

        let mut cmd = Command::new("clickhouse");
        cmd.arg("client");

        if let Some(config) = &options.config {
            cmd.arg("--config").arg(config);
        }

        if let Some(history_file) = &options.history_file {
            // Some version does not expand HOME in the --history_file passed from command line argument
            let expanded = if let Some(stripped) = history_file.strip_prefix("~/") {
                if let Ok(home) = std::env::var("HOME") {
                    format!("{}/{}", home, stripped)
                } else {
                    history_file.clone()
                }
            } else {
                history_file.clone()
            };
            cmd.arg("--history_file").arg(expanded);
        }

        if let Some(url) = &options.url
            && let Ok(url) = url::Url::parse(url)
        {
            if let Some(host) = &url.host() {
                cmd.arg("--host").arg(host.to_string());
            }
            if let Some(port) = &url.port() {
                cmd.arg("--port").arg(port.to_string());
            }
            if !url.username().is_empty() {
                cmd.arg("--user").arg(url.username());
            }
            if let Some(password) = &url.password() {
                cmd.arg("--password").arg(
                    percent_decode(password.as_bytes())
                        .decode_utf8_lossy()
                        .to_string(),
                );
            }

            let database = url.path().strip_prefix('/').unwrap_or_default();
            if !database.is_empty() {
                cmd.arg("--database").arg(database);
            }

            let pairs: HashMap<_, _> = url.query_pairs().into_owned().collect();
            for (key, value) in &pairs {
                match key.as_str() {
                    // clickhouse-rs internal settings, not relevant for client
                    "compression" | "pool_min" | "pool_max" | "nodelay" | "keepalive"
                    | "ping_before_query" | "send_retries" | "retry_timeout" | "ping_timeout"
                    | "insert_timeout" | "execute_timeout" | "alt_hosts" | "client_name" => {}
                    // only via client config
                    "ca_certificate" => {}
                    "client_certificate" => {}
                    "client_private_key" => {}
                    // mapped to different client flag names
                    "skip_verify" => {
                        if value == "true" {
                            cmd.arg("--accept-invalid-certificate");
                        }
                    }
                    "secure" => {
                        if value == "true" {
                            cmd.arg("--secure");
                        } else {
                            cmd.arg("--no-secure");
                        }
                    }
                    "connection_timeout" => {
                        if let Some(us) = parse_duration_us(value) {
                            if !pairs.contains_key("connect_timeout") {
                                cmd.arg(format!("--connect_timeout={}", us / 1_000_000));
                            }
                            if !pairs.contains_key("connect_timeout_with_failover_ms") {
                                cmd.arg(format!(
                                    "--connect_timeout_with_failover_ms={}",
                                    us / 1_000
                                ));
                            }
                            if !pairs.contains_key("connect_timeout_with_failover_secure_ms") {
                                cmd.arg(format!(
                                    "--connect_timeout_with_failover_secure_ms={}",
                                    us / 1_000
                                ));
                            }
                        }
                    }
                    "query_timeout" => {
                        if let Some(us) = parse_duration_us(value)
                            && !pairs.contains_key("max_execution_time")
                        {
                            cmd.arg(format!("--max_execution_time={}", us / 1_000_000));
                        }
                    }
                    // pass through as-is (query settings like skip_unavailable_shards, etc.)
                    _ => {
                        cmd.arg(format!("--{}={}", key, value));
                    }
                }
            }
        }

        let cb_sink = siv.cb_sink().clone();
        let cmd_line = format!("{:?}", cmd);
        log::info!("Spawning client: {}", cmd_line);

        // Spawn the child in its own process group and give it the terminal
        // foreground, like a shell does for foreground jobs. This way Ctrl-C is
        // delivered only to the child's group and chdig's terminal state stays clean.
        #[cfg(unix)]
        cmd.process_group(0);

        let mut guard = TerminalRawModeGuard::leave();
        eprintln!("\n--- chdig: launching clickhouse client ---\n");

        let result = Self::spawn_and_wait(&mut cmd);

        if let Err(e) = guard.restore() {
            log::error!("Failed to restore terminal: {}", e);
            siv.quit();
            return;
        }

        match result {
            Ok(status) => {
                cb_sink
                    .send(Box::new(move |siv| {
                        siv.complete_clear();
                        if !status.success() {
                            siv.add_layer(Dialog::info(format!(
                                "clickhouse client exited with status: {}\n\nCommand: {}",
                                status, cmd_line
                            )));
                        }
                    }))
                    .ok();
            }
            Err(err) => {
                cb_sink.send(Box::new(move |siv| {
                    siv.complete_clear();
                    siv.add_layer(Dialog::info(format!(
                        "Failed to spawn clickhouse client: {}\n\nCommand: {}\n\nMake sure clickhouse is installed and in PATH",
                        err, cmd_line
                    )));
                })).ok();
            }
        }

        siv.complete_clear();
        log::info!("Client terminated.");
    }
}
