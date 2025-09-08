use anyhow::{Context, Error, Result};
use cursive::utils::markup::StyledString;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::{Write, stdout};
use std::process::{Command, Stdio};
use syntect::{highlighting::ThemeSet, parsing::SyntaxSet};
use tempfile::Builder;
use urlencoding::encode;
use tokio::net::TcpListener;

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

    cmd.stdout(Stdio::null());
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

pub fn open_perfetto_trace_in_browser(trace_data: Vec<u8>) -> Result<()> {
    use std::thread;
    use warp::Filter;
    
    // Clone trace data for the server
    let trace_data_clone = trace_data.clone();
    
    // Generate HTML that fetches trace data from server
    let html_content = r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>ClickHouse Query Trace - Perfetto</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            margin: 0;
            padding: 20px;
            font-family: Arial, sans-serif;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            padding: 30px;
        }
        h1 {
            color: #333;
            margin-bottom: 20px;
            text-align: center;
        }
        .buttons {
            display: flex;
            gap: 15px;
            margin: 20px 0;
            justify-content: center;
        }
        .btn {
            background: #4CAF50;
            color: white;
            padding: 12px 24px;
            text-decoration: none;
            border-radius: 6px;
            font-weight: bold;
            transition: background-color 0.3s;
            display: inline-block;
            cursor: pointer;
            border: none;
        }
        .btn:hover {
            background: #45a049;
        }
        .btn.secondary {
            background: #2196F3;
        }
        .btn.secondary:hover {
            background: #1976D2;
        }
        .btn:disabled {
            background: #ccc;
            cursor: not-allowed;
        }
        .info {
            background: #f0f8ff;
            border: 1px solid #b6d7ff;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }
        .trace-info {
            background: #fff3cd;
            border: 1px solid #ffeaa7;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
        }
        .logs {
            background: #f8f9fa;
            border: 1px solid #ddd;
            border-radius: 4px;
            padding: 15px;
            margin: 20px 0;
            font-family: monospace;
            white-space: pre-wrap;
            max-height: 200px;
            overflow-y: auto;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 ClickHouse Query Performance Trace</h1>
        
        <div class="info">
            <h3>📊 Trace Analysis</h3>
            <p>This trace contains comprehensive performance data from your ClickHouse query execution including:</p>
            <ul>
                <li><strong>System Metrics:</strong> CPU usage, memory consumption, disk I/O statistics</li>
                <li><strong>Query Execution:</strong> Thread activity, processor pipeline performance</li>
                <li><strong>Profile Events:</strong> Detailed performance counters and timing data</li>
                <li><strong>Call Stacks:</strong> CPU sampling data for performance hotspot identification</li>
                <li><strong>Log Messages:</strong> Execution logs and debug information</li>
            </ul>
        </div>

        <div class="buttons">
            <button onclick="fetchAndOpenInPerfetto()" class="btn" id="openBtn">🚀 Open in Perfetto UI</button>
            <a href="/trace.pb" download="trace.pb" class="btn secondary">💾 Download .pb File</a>
        </div>

        <div class="trace-info">
            <h3>🛠️ How to Use</h3>
            <p><strong>Recommended:</strong> Click "Open in Perfetto UI" to fetch the trace and open it directly in Perfetto.</p>
            <p><strong>Alternative:</strong> Download the .pb file to analyze offline or manually upload to Perfetto.</p>
        </div>

        <div id="logs" class="logs"></div>
    </div>

    <script>
        const logs = document.getElementById('logs');
        const openBtn = document.getElementById('openBtn');
        const PERFETTO_ORIGIN = 'https://ui.perfetto.dev';

        async function fetchAndOpenInPerfetto() {
            try {
                openBtn.disabled = true;
                openBtn.textContent = '⏳ Fetching trace...';
                
                logs.textContent += 'Fetching trace from server...\n';
                const resp = await fetch('/trace.pb');
                
                if (!resp.ok) {
                    throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
                }
                
                const blob = await resp.blob();
                const arrayBuffer = await blob.arrayBuffer();
                
                logs.textContent += `Fetch complete (${arrayBuffer.byteLength} bytes), opening Perfetto UI...\n`;
                
                openTrace(arrayBuffer);
                
            } catch (error) {
                logs.textContent += `Error: ${error.message}\n`;
                logs.textContent += 'Please try downloading the .pb file and manually uploading to Perfetto.\n';
                openBtn.textContent = '❌ Error - Try Download';
                openBtn.disabled = false;
            }
        }

        function openTrace(arrayBuffer) {
            const win = window.open(PERFETTO_ORIGIN);
            
            if (!win) {
                openBtn.style.background = '#f3ca63';
                openBtn.onclick = () => openTrace(arrayBuffer);
                openBtn.textContent = 'Popups blocked, click here to open trace';
                openBtn.disabled = false;
                logs.textContent += 'Popups blocked, you need to manually click the button\n';
                return;
            }

            logs.textContent += 'Waiting for Perfetto UI to load...\n';

            // Start PING/PONG handshake
            const timer = setInterval(() => win.postMessage('PING', PERFETTO_ORIGIN), 50);

            const onMessageHandler = (evt) => {
                if (evt.data !== 'PONG') return;

                // We got a PONG, the UI is ready
                logs.textContent += 'Perfetto UI is ready, sending trace data...\n';
                window.clearInterval(timer);
                window.removeEventListener('message', onMessageHandler);

                // Create reopen URL for this trace
                const reopenUrl = new URL(location.href);
                reopenUrl.hash = `#reopen=${encodeURIComponent(location.href + 'trace.pb')}`;

                // Send the trace data
                win.postMessage({
                    perfetto: {
                        buffer: arrayBuffer,
                        title: 'ClickHouse Query Trace',
                        url: reopenUrl.toString(),
                    }
                }, PERFETTO_ORIGIN);

                logs.textContent += 'Trace sent to Perfetto UI successfully!\n';
                openBtn.textContent = '✅ Opened in Perfetto';
                openBtn.style.background = '#28a745';
                openBtn.disabled = false;
            };

            window.addEventListener('message', onMessageHandler);

            // Timeout after 10 seconds if no PONG received
            setTimeout(() => {
                window.clearInterval(timer);
                window.removeEventListener('message', onMessageHandler);
                if (openBtn.textContent.includes('Fetching') || openBtn.textContent.includes('⏳')) {
                    logs.textContent += 'Timeout waiting for Perfetto UI. Please try again or use manual download.\n';
                    openBtn.textContent = '⚠️ Timeout - Try Again';
                    openBtn.style.background = '#f3ca63';
                    openBtn.disabled = false;
                    openBtn.onclick = () => fetchAndOpenInPerfetto();
                }
            }, 10000);
        }

        // Auto-focus the main button
        window.onload = function() {
            openBtn.focus();
        };
    </script>
</body>
</html>"#.to_string();

    // Start HTTP server in a separate thread
    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            // Route for serving HTML page
            let html_route = warp::path::end()
                .map(move || warp::reply::html(html_content.clone()));
            
            // Route for serving the protobuf file
            let trace_route = warp::path("trace.pb")
                .map(move || {
                    warp::reply::with_header(
                        trace_data_clone.clone(),
                        "content-type",
                        "application/octet-stream"
                    )
                });
            
            let routes = html_route.or(trace_route);


            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap(); // <-- actual assigned port here

            // Use dynamic port allocation
            let server = warp::serve(routes).incoming(listener);
            
            // Print the URL for manual access
            let url = format!("http://{}", addr);
            println!("Perfetto server started at: {}", url);
            let server_task = tokio::spawn(server.run());

            // Give the server a moment to fully start
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            
            match open_url_command(&url).status() {
                Ok(status) if status.success() => {
                    // Browser opened successfully
                }
                _ => {
                    println!("Could not automatically open browser. Please open the URL manually.");
                }
            }
            
            tokio::select! {
                _ = server_task => {
                    log::info!("Perfetto HTTP server completed");
                },
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(300)) => {
                    log::info!("Perfetto HTTP server shutting down after 5 minutes");
                }
            }
        });
    });

    Ok(())
}
