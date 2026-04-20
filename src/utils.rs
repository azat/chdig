use crate::actions::ActionDescription;
use crate::pastila;
use crate::view::Navigation;
use anyhow::{Context, Error, Result};
use cursive::Cursive;
use cursive::align::HAlign;
use cursive::event;
use cursive::utils::markup::StyledString;
use cursive::view::Nameable;
use cursive::views::{EditView, LinearLayout, OnEventView, Panel, SelectView};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Write;
use std::process::{Command, Stdio};
use syntect::{highlighting::ThemeSet, parsing::SyntaxSet};
use tempfile::Builder;

/// RAII guard that leaves cursive's terminal state (raw mode, alternate screen,
/// mouse capture, hidden cursor) and restores it on drop.
///
/// Uses cursive's re-exported crossterm to ensure we operate on the same global
/// raw mode state that the cursive backend uses.
pub struct TerminalRawModeGuard {
    restored: bool,
}

use cursive::backends::crossterm::crossterm as ct;

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

pub fn fuzzy_actions<F>(siv: &mut Cursive, actions: Vec<ActionDescription>, on_select: F)
where
    F: Fn(&mut Cursive, String) + 'static + Send + Sync,
{
    let items: Vec<(String, String)> = actions
        .iter()
        .map(|a| {
            let text = a.text.to_string();
            (text.clone(), text)
        })
        .collect();
    fuzzy_select_strings(siv, "Fuzzy search", items, on_select);
}

pub fn fuzzy_select_strings<F>(
    siv: &mut Cursive,
    title: &str,
    items: Vec<(String, String)>,
    on_select: F,
) where
    F: Fn(&mut Cursive, String) + 'static + Send + Sync,
{
    if siv.has_view("fuzzy_search") {
        return;
    }

    let mut select = SelectView::<String>::new().h_align(HAlign::Left).autojump();
    for (label, value) in &items {
        select.add_item(label.clone(), value.clone());
    }

    select.set_on_submit(move |siv, item: &String| {
        let selected = item.clone();
        siv.pop_layer();
        on_select(siv, selected);
    });

    let search = EditView::new()
        .on_edit(move |siv, query, _| {
            siv.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.clear();

                let matcher = SkimMatcherV2::default();
                let query_words: Vec<&str> = query.split_whitespace().collect();

                let mut matches: Vec<(i64, String, String)> = items
                    .iter()
                    .filter_map(|(label, value)| {
                        if query_words.is_empty() {
                            return Some((0, label.clone(), value.clone()));
                        }

                        let mut total_score = 0i64;
                        for word in &query_words {
                            match matcher.fuzzy_match(label, word) {
                                Some(score) => total_score += score,
                                None => return None,
                            }
                        }

                        Some((total_score, label.clone(), value.clone()))
                    })
                    .collect();

                matches.sort_by(|a, b| b.0.cmp(&a.0));

                for (_, label, value) in matches {
                    view.add_item(label, value);
                }
            });
        })
        .on_submit(|siv, _| {
            siv.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.set_selection(0);
            });
            siv.focus_name("fuzzy_select").ok();
            siv.on_event(event::Event::Key(cursive::event::Key::Enter));
        })
        .with_name("fuzzy_search");

    let layout = LinearLayout::vertical()
        .child(search)
        .child(select.with_name("fuzzy_select"));

    let dialog = OnEventView::new(Panel::new(layout).title(title.to_string()))
        .on_pre_event(event::Event::CtrlChar('k'), |s| {
            s.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.select_up(1);
            });
        })
        .on_pre_event(event::Event::CtrlChar('j'), |s| {
            s.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.select_down(1);
            });
        })
        .on_pre_event(event::Event::CtrlChar('w'), |s| {
            let callback = s.call_on_name("fuzzy_search", |view: &mut EditView| {
                let content = view.get_content();
                let cursor = view.get_cursor();

                let before_cursor = &content[..cursor];
                let trimmed = before_cursor.trim_end();
                if trimmed.is_empty() {
                    let cb = view.set_content("");
                    view.set_cursor(0);
                    return Some(cb);
                }

                let new_pos = trimmed
                    .rfind(|c: char| c.is_whitespace())
                    .map(|pos| pos + 1)
                    .unwrap_or(0);

                let new_content = format!("{}{}", &content[..new_pos], &content[cursor..]);
                let cb = view.set_content(new_content);
                view.set_cursor(new_pos);
                Some(cb)
            });

            if let Some(Some(cb)) = callback {
                cb(s);
            }
        })
        .on_event(event::Key::Backspace, |_| {})
        .on_event(event::Event::CtrlChar('p'), |s| {
            s.pop_layer();
        })
        .on_event(event::Key::Esc, |s| {
            s.pop_layer();
        });

    siv.add_layer(dialog);
    siv.focus_name("fuzzy_search").ok();
}

pub fn highlight_sql(text: &str) -> Result<StyledString> {
    let syntax_set = SyntaxSet::load_defaults_newlines();
    let ts = ThemeSet::load_defaults();
    let mut highlighter = syntect::easy::HighlightLines::new(
        syntax_set
            .find_syntax_by_token("sql")
            .context("Cannot load SQL syntax")?,
        &ts.themes["base16-ocean.dark"],
    );
    // NOTE: parse() does not interpret syntect::highlighting::Color::a (alpha/transparency)
    return cursive_syntect::parse(text, &mut highlighter, &syntax_set)
        .context("Cannot highlight query");
}

pub fn get_query(query: &str, settings: &HashMap<String, String>) -> String {
    // NOTE: LinesIterator (that is used by TextView for wrapping) cannot handle "\t",
    // it renders as a replacement glyph at the start of each wrapped/continuation line.
    let mut ret = query.replace('\t', "    ");
    let settings_str = settings
        .iter()
        .enumerate()
        .map(|(i, kv)| {
            let is_last = i + 1 == settings.len();
            // NOTE: LinesIterator (that is used by TextView for wrapping) cannot handle "\t", hence 4 spaces
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
    if !query.contains("SETTINGS") {
        ret.push_str("\nSETTINGS\n");
    } else {
        ret.push_str(",\n");
    }
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
    pastila_clickhouse_host: &str,
    pastila_url: &str,
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
    let mut url = pastila::upload_encrypted(&html, pastila_clickhouse_host, pastila_url).await?;

    if let Some(anchor_pos) = url.find('#') {
        url.insert_str(anchor_pos, ".html");
    }

    // Open the URL in the browser
    open_url_command(&url).status()?;

    Ok(())
}

pub fn find_common_hostname_prefix_and_suffix<'a, I>(hostnames: I) -> (String, String)
where
    I: Iterator<Item = &'a str>,
{
    let hostnames_vec: Vec<&str> = hostnames.collect();
    if hostnames_vec.is_empty() {
        return (String::new(), String::new());
    }

    let first = hostnames_vec[0];

    // Find common prefix
    let mut prefix_end = first.len();
    for pos in (0..first.len()).rev() {
        let candidate = &first[..=pos];
        if hostnames_vec[1..].iter().all(|h| h.starts_with(candidate)) {
            prefix_end = pos + 1;
            break;
        }
    }

    let common_prefix = &first[..prefix_end];
    let prefix_delim_pos = common_prefix
        .rfind('.')
        .into_iter()
        .chain(common_prefix.rfind('-'))
        .max();

    let prefix = if let Some(pos) = prefix_delim_pos {
        common_prefix[..=pos].to_string()
    } else {
        String::new()
    };

    // Find common suffix
    let mut suffix_start = 0;
    for pos in 0..first.len() {
        let candidate = &first[pos..];
        if hostnames_vec[1..].iter().all(|h| h.ends_with(candidate)) {
            suffix_start = pos;
            break;
        }
    }

    let common_suffix = &first[suffix_start..];
    let suffix_delim_pos = common_suffix
        .find('.')
        .into_iter()
        .chain(common_suffix.find('-'))
        .min();

    let suffix = if let Some(pos) = suffix_delim_pos {
        common_suffix[pos..].to_string()
    } else {
        String::new()
    };

    (prefix, suffix)
}
