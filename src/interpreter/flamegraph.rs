use crate::interpreter::clickhouse::Columns;
use crate::utils::open_url_command;
use anyhow::{Error, Result};
use crossterm::event::{self, Event as CrosstermEvent, KeyEventKind};
use flamelens::app::{App, AppResult};
use flamelens::flame::FlameGraph;
use flamelens::handler::handle_key_events;
use flamelens::ui;
use futures::channel::mpsc;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io;
use tokio::time::{sleep, Duration};
use urlencoding::encode;
use warp::http::header::{HeaderMap, HeaderValue};
use warp::Filter;

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

    // TODO: rewrite to termion on linux (windows uses crossterm as well)
    let backend = CrosstermBackend::new(io::stderr());
    let mut terminal = Terminal::new(backend)?;
    let timeout = std::time::Duration::from_secs(1);

    terminal.clear()?;

    // Start the main loop.
    while app.running {
        terminal.draw(|frame| {
            ui::render(&mut app, frame);
            if let Some(input_buffer) = &app.input_buffer {
                if let Some(cursor) = input_buffer.cursor {
                    frame.set_cursor(cursor.0, cursor.1);
                }
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

pub async fn open_in_speedscope(block: Columns) -> Result<()> {
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
    } else {
        let (tx, mut rx) = mpsc::channel(1);

        let mut headers = HeaderMap::new();
        headers.insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));

        let route = warp::any()
            .map(move || {
                // stop the server
                tx.clone().try_send(()).ok();
                return data.clone();
            })
            .with(warp::reply::with::headers(headers));
        let (bind_address, server) =
            warp::serve(route).bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
                while rx.try_next().is_err() {
                    sleep(Duration::from_millis(100)).await;
                }
                // FIXME: this is a dirty hack that assumes that 1 second is enough to server the
                // request
                sleep(Duration::from_secs(1)).await;
            });

        // NOTE: here we need a webserver, since we cannot use localProfilePath due to browser
        // policies
        let url = format!(
            "https://www.speedscope.app/#profileURL={}",
            encode(&format!("http://{}/", bind_address))
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

        // TODO: correctly wait the server stopped serving
        server.await;
    }

    return Ok(());
}
