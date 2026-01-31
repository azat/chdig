use crate::interpreter::clickhouse::Columns;
use crate::pastila;
use crate::utils::open_url_command;
use anyhow::{Error, Result};
use crossterm::event::{self, Event as CrosstermEvent, KeyEventKind};
use flamelens::app::{App, AppResult};
use flamelens::flame::FlameGraph;
use flamelens::handler::handle_key_events;
use flamelens::ui;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use std::io;
use urlencoding::encode;

pub fn show(block: Columns) -> AppResult<()> {
    let data = block
        .rows()
        .map(|x| {
            [
                x.get::<String, _>(0).unwrap(),
                x.get::<u64, _>(1).unwrap().to_string(),
            ]
            .join(" ")
        })
        .collect::<Vec<String>>()
        .join("\n");

    if data.trim().is_empty() {
        return Err(Error::msg("Flamegraph is empty").into());
    }

    let flamegraph = FlameGraph::from_string(data, true);
    let mut app = App::with_flamegraph("Query", flamegraph);

    let backend = CrosstermBackend::new(io::stderr());
    let mut terminal = Terminal::new(backend)?;
    let timeout = std::time::Duration::from_secs(1);

    terminal.clear()?;

    // Start the main loop.
    while app.running {
        terminal.draw(|frame| {
            ui::render(&mut app, frame);
            if let Some(input_buffer) = &app.input_buffer
                && let Some(cursor) = input_buffer.cursor
            {
                frame.set_cursor_position((cursor.0, cursor.1));
            }
        })?;

        // FIXME: note, right now I cannot use EventHandle with Tui, since EventHandle is not
        // terminated gracefully
        if event::poll(timeout).expect("failed to poll new events") {
            match event::read().expect("unable to read event") {
                CrosstermEvent::Key(e) => {
                    if e.kind == KeyEventKind::Press {
                        handle_key_events(e, &mut app)?
                    }
                }
                CrosstermEvent::Mouse(_e) => {}
                CrosstermEvent::Resize(_w, _h) => {}
                CrosstermEvent::FocusGained => {}
                CrosstermEvent::FocusLost => {}
                CrosstermEvent::Paste(_) => {}
            }
        }
    }

    terminal.clear()?;

    Ok(())
}

pub async fn open_in_speedscope(
    block: Columns,
    pastila_clickhouse_host: &str,
    pastila_url: &str,
) -> Result<()> {
    let data = block
        .rows()
        .map(|x| {
            [
                x.get::<String, _>(0).unwrap(),
                x.get::<u64, _>(1).unwrap().to_string(),
            ]
            .join(" ")
        })
        .collect::<Vec<String>>()
        .join("\n");

    if data.trim().is_empty() {
        return Err(Error::msg("Flamegraph is empty"));
    }

    let pastila_url = pastila::upload(&data, pastila_clickhouse_host, pastila_url).await?;

    let url = format!(
        "https://www.speedscope.app/#profileURL={}",
        encode(&pastila_url)
    );

    let mut child = open_url_command(&url)
        .spawn()
        .map_err(|e| Error::msg(format!("Cannot open URL: {}", e)))?;

    let result = child.wait()?;
    if !result.success() {
        return Err(Error::msg(format!(
            "Error while opening flamegraph in browser: {:?} (Do you have some browser installed?)",
            result
        )));
    }

    return Ok(());
}
