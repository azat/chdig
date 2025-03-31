use crate::interpreter::clickhouse::Columns;
use anyhow::{Error, Result};
use flamelens::app::{App, AppResult};
use flamelens::event::{Event, EventHandler};
use flamelens::flame::FlameGraph;
use flamelens::handler::handle_key_events;
use flamelens::tui::Tui;
use futures::channel::mpsc;
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::io;
use std::process::{Command, Stdio};
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
    let terminal = Terminal::new(backend)?;
    let events = EventHandler::new(250);
    let mut tui = Tui::new(terminal, events);
    // NOTE: No need to tui.init(), since we are already in TUI mode

    // Start the main loop.
    while app.running {
        // Render the user interface.
        tui.draw(&mut app)?;
        // Handle events.
        match tui.events.next()? {
            Event::Tick => app.tick(),
            Event::Key(key_event) => handle_key_events(key_event, &mut app)?,
            Event::Mouse(_) => {}
            Event::Resize(_, _) => {}
        }
    }

    // NOTE: No need to tui.exit(), since we are still in TUI mode
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
        let mut child = Command::new("xdg-open")
            .arg(format!(
                "https://www.speedscope.app/#profileURL={}",
                encode(&format!("http://{}/", bind_address))
            ))
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .map_err(|e| Error::msg(format!("Cannot find/execute xdg-open ({})", e)))?;

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
