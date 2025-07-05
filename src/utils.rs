use anyhow::{Context, Error, Result};
use cursive::utils::markup::StyledString;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{stdout, Write};
use std::process::{Command, Stdio};
use syntect::{highlighting::ThemeSet, parsing::SyntaxSet};
use tempfile::Builder;
use urlencoding::encode;

#[cfg(not(target_family = "windows"))]
use {crate::actions::ActionDescription, skim::prelude::*};

#[cfg(not(target_family = "windows"))]
impl SkimItem for ActionDescription {
    fn text(&self) -> Cow<'_, str> {
        return Cow::Borrowed(self.text);
    }
}

// TODO: render from the bottom
#[cfg(not(target_family = "windows"))]
pub fn fuzzy_actions(actions: Vec<ActionDescription>) -> Option<String> {
    let options = SkimOptionsBuilder::default()
        .height("30%".to_string())
        .build()
        .unwrap();

    let (tx, rx): (SkimItemSender, SkimItemReceiver) = unbounded();
    actions
        .iter()
        .for_each(|i| tx.send(Arc::new(i.clone())).unwrap());
    drop(tx);

    // Put cursor to the end of the screen to make layout works properly for skim
    let (cols, rows) = crossterm::terminal::size().ok()?;
    crossterm::execute!(
        stdout(),
        crossterm::cursor::MoveTo(cols.saturating_sub(1), rows.saturating_sub(1),)
    )
    .ok()?;

    let out = Skim::run_with(&options, Some(rx))?;
    // FIXME: skim breaks resizing (but only for the time skim is running)

    if out.is_abort {
        return None;
    }

    let selected_items = out.selected_items;
    if selected_items.is_empty() {
        return None;
    }

    // TODO: cast SkimItem to ActionDescription
    return Some(selected_items[0].text().into());
}

pub fn highlight_sql(text: &String) -> Result<StyledString> {
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

pub fn get_query(query: &String, settings: &HashMap<String, String>) -> String {
    let mut ret = query.to_owned();
    let settings_str = settings
        .iter()
        .map(|kv| format!("\t{}='{}'\n", kv.0, kv.1.replace('\'', "\\\'")))
        .collect::<Vec<String>>()
        .join(",");
    if !query.contains("SETTINGS") {
        ret.push_str("\nSETTINGS\n");
    } else {
        ret.push_str(",\n");
    }
    ret.push_str(&settings_str);
    return ret;
}

pub fn edit_query(query: &String, settings: &HashMap<String, String>) -> Result<String> {
    let mut tmp_file = Builder::new()
        .prefix("chdig-query-")
        .suffix(".sql")
        .rand_bytes(5)
        .tempfile()?;

    let query = get_query(query, settings);
    tmp_file.write_all(query.as_bytes())?;

    let editor = env::var_os("EDITOR").unwrap_or_else(|| "vim".into());
    let tmp_file_path = tmp_file.path().to_str().unwrap();
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

pub fn open_graph_in_browser(graph: String) -> Result<()> {
    if graph.is_empty() {
        return Err(Error::msg("Graph is empty"));
    }
    let url = format!(
        "https://dreampuf.github.io/GraphvizOnline/#{}",
        encode(&graph)
    );
    open_url_command(&url).status()?;
    return Ok(());
}
