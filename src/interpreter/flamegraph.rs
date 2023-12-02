use crate::interpreter::clickhouse::Columns;
use anyhow::{Error, Result};
use flameshow::flameshow;
use futures::channel::mpsc;
use std::process::{Command, Stdio};
use tokio::time::{sleep, Duration};
use urlencoding::encode;
use warp::http::header::{HeaderMap, HeaderValue};
use warp::Filter;

pub fn show(block: Columns) -> Result<()> {
    let data = block
        .rows()
        .map(|x| {
            vec![
                x.get::<String, _>(0).unwrap(),
                x.get::<u64, _>(1).unwrap().to_string(),
            ]
            .join(" ")
        })
        .collect::<Vec<String>>()
        .join("\n");

    return flameshow(&data, "ClickHouse");
}

pub async fn open_in_speedscope(block: Columns) -> Result<()> {
    let data = block
        .rows()
        .map(|x| {
            vec![
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
                while !rx.try_next().is_ok() {
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
                encode(&format!("http://{}/", bind_address.to_string()))
            ))
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .or_else(|e| Err(Error::msg(format!("Cannot find/execute xdg-open ({})", e))))?;

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
