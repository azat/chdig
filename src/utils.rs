use crate::actions::ActionDescription;
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
use urlencoding::encode;

pub fn fuzzy_actions<F>(siv: &mut Cursive, actions: Vec<ActionDescription>, on_select: F)
where
    F: Fn(&mut Cursive, String) + 'static + Send + Sync,
{
    if siv.has_view("fuzzy_search") {
        return;
    }

    let mut select = SelectView::<String>::new().h_align(HAlign::Left).autojump();
    actions.iter().for_each(|action| {
        let action_name = action.text.to_string();
        select.add_item(action_name.clone(), action_name);
    });

    select.set_on_submit(move |siv, item: &String| {
        let selected = item.clone();
        siv.pop_layer();
        on_select(siv, selected);
    });

    let search = EditView::new()
        .on_edit(move |siv, query, _| {
            siv.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                // Clear and repopulate based on search
                view.clear();

                let matcher = SkimMatcherV2::default();
                let mut matches: Vec<(i64, String)> = actions
                    .iter()
                    .filter_map(|action| {
                        let action_name = action.text.to_string();
                        matcher
                            .fuzzy_match(&action_name, query)
                            .map(|score| (score, action_name))
                    })
                    .collect();

                // Sort by match score (highest first)
                matches.sort_by(|a, b| b.0.cmp(&a.0));

                for (_, text) in matches {
                    view.add_item(text.clone(), text);
                }
            });
        })
        .on_submit(|siv, _| {
            // When Enter is pressed in search box, submit the first item in the select view
            siv.call_on_name("fuzzy_select", |view: &mut SelectView<String>| {
                view.set_selection(0);
            });

            // Trigger the submit event on the select view
            siv.focus_name("fuzzy_select").ok();
            siv.on_event(event::Event::Key(cursive::event::Key::Enter));
        })
        .with_name("fuzzy_search");

    let layout = LinearLayout::vertical()
        .child(search)
        .child(select.with_name("fuzzy_select"));

    let dialog = OnEventView::new(Panel::new(layout).title("Fuzzy search"))
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

                // Find the start of the word to delete
                let before_cursor = &content[..cursor];

                // Skip trailing whitespace first
                let trimmed = before_cursor.trim_end();
                if trimmed.is_empty() {
                    // If only whitespace, clear everything before cursor
                    let cb = view.set_content("");
                    view.set_cursor(0);
                    return Some(cb);
                }

                // Find the last word boundary (space or start of string)
                let new_pos = trimmed
                    .rfind(|c: char| c.is_whitespace())
                    .map(|pos| pos + 1) // Keep the space
                    .unwrap_or(0); // Or delete to start

                // Reconstruct content without the deleted word
                let new_content = format!("{}{}", &content[..new_pos], &content[cursor..]);
                let cb = view.set_content(new_content);
                view.set_cursor(new_pos);
                Some(cb)
            });

            // Finally update the EditView
            if let Some(Some(cb)) = callback {
                cb(s);
            }
        })
        // Override global pop_layer()
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
    let mut ret = query.to_string();
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
