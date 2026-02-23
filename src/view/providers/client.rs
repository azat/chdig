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

        let result = {
            let _guard = TerminalRawModeGuard::leave();
            cmd.status()
        };

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
