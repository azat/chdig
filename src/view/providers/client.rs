use crate::{
    interpreter::{ContextArc, options::ChDigViews},
    utils::TerminalRawModeGuard,
    view::ViewProvider,
};
use cursive::{Cursive, views::Dialog};
use percent_encoding::percent_decode;
use std::process::Command;
use std::str::FromStr;

pub struct ClientViewProvider;

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

            let pairs: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
            if let Some(skip_verify) = pairs
                .get("skip_verify")
                .and_then(|v| bool::from_str(v).ok())
                && skip_verify
            {
                cmd.arg("--accept-invalid-certificate");
            }
            if pairs
                .get("secure")
                .and_then(|v| bool::from_str(v).ok())
                .unwrap_or_default()
            {
                cmd.arg("--secure");
            }
        }

        let cb_sink = siv.cb_sink().clone();
        let cmd_line = format!("{:?}", cmd);
        log::info!("Spawning client: {}", cmd_line);

        cmd.stderr(std::process::Stdio::piped());

        let result = {
            let _guard = TerminalRawModeGuard::leave();
            eprintln!("\n--- chdig: launching clickhouse client ---\n");

            // Ignore SIGINT in chdig while the child runs, so Ctrl-C only reaches
            // the clickhouse client (same semantics as a shell foreground job).
            let prev_handler = unsafe { libc::signal(libc::SIGINT, libc::SIG_IGN) };
            let output = cmd.spawn().and_then(|child| child.wait_with_output());
            unsafe { libc::signal(libc::SIGINT, prev_handler) };
            output
        };

        match result {
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                cb_sink
                    .send(Box::new(move |siv| {
                        siv.complete_clear();
                        if !output.status.success() {
                            let mut msg = format!(
                                "clickhouse client exited with status: {}\n\nCommand: {}",
                                output.status, cmd_line
                            );
                            if !stderr.is_empty() {
                                msg.push_str(&format!("\n\nStderr:\n{}", stderr));
                            }
                            siv.add_layer(Dialog::info(msg));
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
