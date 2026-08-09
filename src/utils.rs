use crate::pastila;
use anyhow::{Error, Result};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::Builder;

/// RAII guard that leaves the TUI terminal state (raw mode, alternate screen,
/// mouse capture, hidden cursor) and restores it on drop.
pub struct TerminalRawModeGuard {
    restored: bool,
}

use crossterm as ct;

impl TerminalRawModeGuard {
    pub fn leave() -> Self {
        ct::terminal::disable_raw_mode().unwrap();
        ct::execute!(
            std::io::stdout(),
            ct::event::DisableMouseCapture,
            ct::style::ResetColor,
            ct::style::SetAttribute(ct::style::Attribute::Reset),
            ct::cursor::Show,
            ct::terminal::LeaveAlternateScreen,
        )
        .unwrap();
        Self { restored: false }
    }

    fn do_restore() -> std::io::Result<()> {
        ct::terminal::enable_raw_mode()?;
        ct::execute!(
            std::io::stdout(),
            ct::terminal::EnterAlternateScreen,
            ct::event::EnableMouseCapture,
            ct::cursor::Hide,
        )
    }

    pub fn restore(&mut self) -> std::io::Result<()> {
        self.restored = true;
        Self::do_restore()
    }
}

impl Drop for TerminalRawModeGuard {
    fn drop(&mut self) {
        if !self.restored {
            let _ = Self::do_restore();
        }
    }
}

pub fn get_query(query: &str, settings: &HashMap<String, String>) -> String {
    // NOTE: terminal wrapping cannot handle "\t" (rendered as a replacement
    // glyph at the start of each wrapped/continuation line).
    let mut ret = query.replace('\t', "    ");
    let settings_str = settings
        .iter()
        .enumerate()
        .map(|(i, kv)| {
            let is_last = i + 1 == settings.len();
            // NOTE: "\t" does not survive wrapping (see above), hence 4 spaces
            let prefix = "    ";
            format!(
                "{}{}='{}'{}\n",
                prefix,
                kv.0,
                kv.1.replace('\'', "\\\'"),
                if !is_last { "," } else { "" }
            )
        })
        .collect::<Vec<String>>()
        .join("");
    // ClickHouse accepts multiple SETTINGS clauses (last value wins per setting),
    // so always append our own instead of detecting and merging into one the
    // query may already carry.
    ret.push_str("\nSETTINGS\n");
    ret.push_str(&settings_str);
    return ret;
}

pub fn edit_query(query: &str, settings: &HashMap<String, String>) -> Result<String> {
    let mut tmp_file = Builder::new()
        .prefix("chdig-query-")
        .suffix(".sql")
        .rand_bytes(5)
        .tempfile()?;

    let query = get_query(query, settings);
    tmp_file.write_all(query.as_bytes())?;

    let editor = env::var_os("EDITOR").unwrap_or_else(|| "vim".into());
    let tmp_file_path = tmp_file.path().to_str().unwrap();

    let _guard = TerminalRawModeGuard::leave();

    let result = Command::new(&editor)
        .arg(tmp_file_path)
        .spawn()
        .map_err(|e| Error::msg(format!("Cannot execute editor {:?} ({})", editor, e)))?
        .wait()?;

    if !result.success() {
        return Err(Error::msg(format!(
            "Editor exited unsuccessfully {:?} ({})",
            editor, result
        )));
    }

    let query = fs::read_to_string(tmp_file_path)?;
    return Ok(query);
}

pub fn open_url_command(url: &str) -> Command {
    let mut cmd = if cfg!(target_os = "windows") {
        let mut c = Command::new("cmd");
        c.args(["/C", "start", "", url]); // "" to avoid stealing the first quoted argument as window title
        c
    } else if cfg!(target_os = "macos") {
        let mut c = Command::new("open");
        c.arg(url);
        c
    } else {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };

    cmd.stderr(Stdio::null()).stdout(Stdio::null());
    cmd
}

pub async fn share_graph(
    graph: String,
    pastila: &crate::pastila::PastilaConfig,
    progress: impl Fn(&str),
) -> Result<()> {
    if graph.is_empty() {
        return Err(Error::msg("Graph is empty"));
    }

    // Create a self-contained HTML file that renders the Graphviz graph
    // Using viz.js from CDN for client-side rendering
    let html = format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Graphviz Graph</title>
    <style>
        body {{ margin: 0; padding: 20px; font-family: sans-serif; }}
        #graph {{ text-align: center; }}
    </style>
</head>
<body>
    <div id="graph">Loading graph...</div>
    <script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.2.4/lib/viz-standalone.js"></script>
    <script>
        const dot = {};
        Viz.instance().then(viz => {{
            const svg = viz.renderSVGElement(dot);
            const container = document.getElementById('graph');
            container.innerHTML = '';
            container.appendChild(svg);
        }}).catch(err => {{
            document.getElementById('graph').textContent = 'Error rendering graph: ' + err;
        }});
    </script>
</body>
</html>"#,
        serde_json::to_string(&graph)?
    );

    // Upload HTML to pastila with end-to-end encryption
    let url = pastila::upload_encrypted(&html, pastila, ".html", progress).await?;

    // Open the URL in the browser
    open_url_command(&url).status()?;

    Ok(())
}

pub fn find_common_hostname_prefix_and_suffix<'a, I>(hostnames: I) -> (String, String)
where
    I: Iterator<Item = &'a str>,
{
    let hostnames_vec: Vec<&str> = hostnames.collect();
    let Some(&first) = hostnames_vec.first() else {
        return (String::new(), String::new());
    };

    // Single distinct host (e.g. one-node k8s cluster with a long FQDN): there
    // is no inter-host difference to preserve, keep the first label and strip
    // the domain.
    if hostnames_vec[1..].iter().all(|h| *h == first) {
        let suffix = first
            .find('.')
            .map(|pos| first[pos..].to_string())
            .unwrap_or_default();
        return (String::new(), suffix);
    }

    let mut prefix_len = first.len();
    let mut suffix_len = first.len();
    for h in &hostnames_vec[1..] {
        prefix_len = prefix_len.min(
            first
                .bytes()
                .zip(h.bytes())
                .take_while(|(a, b)| a == b)
                .count(),
        );
        suffix_len = suffix_len.min(
            first
                .bytes()
                .rev()
                .zip(h.bytes().rev())
                .take_while(|(a, b)| a == b)
                .count(),
        );
    }

    // Cut at delimiters so the distinguishing parts stay intact.
    let common_prefix = &first[..prefix_len];
    let prefix = common_prefix
        .rfind(['.', '-'])
        .map(|pos| common_prefix[..=pos].to_string())
        .unwrap_or_default();

    let common_suffix = &first[first.len() - suffix_len..];
    let suffix = common_suffix
        .find(['.', '-'])
        .map(|pos| common_suffix[pos..].to_string())
        .unwrap_or_default();

    (prefix, suffix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_find_common_hostname_prefix_and_suffix() {
        let find = |hosts: &[&str]| find_common_hostname_prefix_and_suffix(hosts.iter().copied());

        assert_eq!(find(&[]), (String::new(), String::new()));

        // Single distinct host: strip the domain, keep the first label
        let k8s = "chi-foo-0-0-0.chi-foo-headless.ns.svc.cluster.local";
        assert_eq!(
            find(&[k8s, k8s]),
            (
                String::new(),
                ".chi-foo-headless.ns.svc.cluster.local".into()
            )
        );
        assert_eq!(find(&["localhost"]), (String::new(), String::new()));

        assert_eq!(
            find(&["node-1.cluster.local", "node-2.cluster.local"]),
            ("node-".into(), ".cluster.local".into())
        );

        // Nothing in common: nothing to strip
        assert_eq!(find(&["alpha", "beta"]), (String::new(), String::new()));
    }
}
