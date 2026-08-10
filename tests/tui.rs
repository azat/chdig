// TUI tests: the real chdig app driven headlessly - events are injected
// programmatically into the App and frames rendered into a ratatui
// TestBackend are asserted on as plain text (no PTY).

#[allow(dead_code)]
mod common;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use chdig::tui::{Event, Key, MouseButton, MouseEvent};
use crossbeam_channel::{Receiver, Sender};
use ratatui::layout::Position;

use common::ClickHouseServer;

// Only one TUI at a time: scenarios act on the single running query visible in the queries view.
// The lock must be taken before spawning the marker query - a query spawned outside of it shows
// up in the other scenario's queries view and steals the row selection there.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial_lock() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A rendered frame as lines of text.
#[derive(Clone)]
struct Screen {
    lines: Vec<String>,
}

impl Screen {
    fn contains(&self, pattern: &str) -> bool {
        self.lines.iter().any(|l| l.contains(pattern))
    }

    fn cell(&self, x: usize, y: usize) -> Option<char> {
        self.lines.get(y).and_then(|l| l.chars().nth(x))
    }
}

impl std::fmt::Display for Screen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for line in &self.lines {
            writeln!(f, "{}", line)?;
        }
        Ok(())
    }
}

struct Tui {
    input: Sender<Event>,
    frames: Receiver<Screen>,
    thread: Option<std::thread::JoinHandle<()>>,
    _serial: MutexGuard<'static, ()>,
}

impl Tui {
    fn start(server: &'static ClickHouseServer, serial: MutexGuard<'static, ()>) -> Self {
        let options = chdig::interpreter::options::parse_from([
            "chdig",
            "--url",
            &format!("tcp://default@127.0.0.1:{}/system", server.tcp_port),
            // The empty config files keep it hermetic (no user configs from default paths)
            "--chdig-config",
            "tests/configs/chdig_empty.yaml",
            "--config",
            "tests/configs/empty.xml",
            // Log to a file: the in-TUI logger can only be initialized once per process
            "--log",
            &server.dir.join("chdig-tui.log").to_string_lossy(),
        ])
        .unwrap();

        let (input_tx, input_rx) = crossbeam_channel::unbounded::<Event>();
        let (frames_tx, frames_rx) = crossbeam_channel::unbounded::<Screen>();

        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let clickhouse = Arc::new(
                    chdig::interpreter::ClickHouse::new(options.clickhouse.clone())
                        .await
                        .expect("chdig cannot connect"),
                );
                let mut logger_handle = None;
                let (mut app, _context) =
                    chdig::chdig_build_app(options, clickhouse, Vec::new(), &mut logger_handle)
                        .await
                        .expect("chdig TUI failed");

                let backend = ratatui::backend::TestBackend::new(180, 50);
                let mut terminal = ratatui::Terminal::new(backend).unwrap();
                while app.is_running() {
                    app.process_callbacks();
                    if !app.is_running() {
                        break;
                    }
                    terminal.draw(|frame| app.draw(frame)).unwrap();
                    let buffer = terminal.backend().buffer();
                    let mut lines = Vec::with_capacity(buffer.area.height as usize);
                    for y in 0..buffer.area.height {
                        let mut line = String::new();
                        for x in 0..buffer.area.width {
                            line.push_str(buffer[(x, y)].symbol());
                        }
                        lines.push(line);
                    }
                    frames_tx.send(Screen { lines }).ok();

                    if let Ok(event) = input_rx.recv_timeout(Duration::from_millis(30)) {
                        app.on_event(event);
                    }
                }
            });
        });

        Tui {
            input: input_tx,
            frames: frames_rx,
            thread: Some(thread),
            _serial: serial,
        }
    }

    fn send(&self, event: Event) {
        self.input.send(event).unwrap();
    }

    /// Wait until the predicate holds for a frame and return that frame.
    /// The TUI loop emits a frame at least every 30ms on its own, so no
    /// redraw has to be forced (a Refresh per frame would keep the loop
    /// hot, spinning it at full speed for the whole wait).
    fn wait_for<F: Fn(&Screen) -> bool>(&self, what: &str, pred: F) -> Screen {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut last_screen = None;
        loop {
            if let Ok(screen) = self.frames.recv_timeout(Duration::from_millis(200)) {
                if pred(&screen) {
                    return screen;
                }
                last_screen = Some(screen);
            }
            if Instant::now() >= deadline {
                if let Some(screen) = &last_screen {
                    eprintln!("last screen:\n{}", screen);
                }
                panic!("{what} did not appear on the screen");
            }
        }
    }

    /// Wait until the pattern shows up on the screen and return that frame.
    fn wait_for_text(&self, pattern: &str) -> Screen {
        self.wait_for(&format!("'{pattern}'"), |screen| screen.contains(pattern))
    }

    fn quit(mut self) {
        self.send(Event::Char('Q'));
        self.thread
            .take()
            .unwrap()
            .join()
            .expect("chdig TUI panicked");
    }
}

fn wait_query_is_running(server: &ClickHouseServer, query_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let count = server.query(&format!(
            "SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ));
        if count == "1" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{query_id} did not show up in system.processes"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn kill_query(server: &ClickHouseServer, query_id: &str, client: &mut std::process::Child) {
    server.query(&format!("KILL QUERY WHERE query_id = '{query_id}' SYNC"));
    let _ = client.wait();
}

fn click(x: u16, y: u16) -> Event {
    Event::Mouse {
        position: Position::new(x, y),
        event: MouseEvent::Press(MouseButton::Left),
    }
}

// The default view: a running query must show up, along with the summary header.
async fn test_queries_view() {
    let Some(server) = common::server() else {
        return;
    };
    let serial = serial_lock();
    let mut child = server.spawn_query(
        "it-tui-queries",
        "SELECT sum(sleep(0.5)) AS tui_marker_queries FROM numbers(600) SETTINGS max_block_size=1",
    );
    wait_query_is_running(server, "it-tui-queries");

    let tui = Tui::start(server, serial);
    // The marker survives normalizeQuery() (identifiers are kept, literals are not)
    let screen = tui.wait_for_text("tui_marker_queries");
    assert!(screen.contains("Uptime:"));
    assert!(screen.contains("default"));

    kill_query(server, "it-tui-queries", &mut child);
    tui.quit();
}

// 'l' on the selected query opens its logs (system.text_log for this query_id).
async fn test_query_logs_view() {
    let Some(server) = common::server() else {
        return;
    };
    let serial = serial_lock();
    let mut child = server.spawn_query(
        "it-tui-logs",
        "SELECT sum(sleep(0.5)) AS tui_marker_logs FROM numbers(600) SETTINGS max_block_size=1",
    );
    // The log rows must be within the query execution time window, so insert them only after the
    // query is known to be running (and with event_time of now)
    wait_query_is_running(server, "it-tui-logs");
    server.query(
        r#"
        INSERT INTO system.text_log
            (hostname, event_date, event_time, event_time_microseconds,
             thread_id, level, logger_name, query_id, message)
        VALUES
            (hostName(), today(), now(), now64(6),
             1, 'Information', 'TUITestLogger', 'it-tui-logs', 'tui marker log line')
        "#,
    );

    let tui = Tui::start(server, serial);
    tui.wait_for_text("tui_marker_logs");
    // The table has no selection until the first interaction
    tui.send(Event::Key(Key::Down));
    tui.send(Event::Char('l'));
    // Only the prefix: the long log line is cropped at the logs pane width
    tui.wait_for_text("tui marker");

    kill_query(server, "it-tui-logs", &mut child);
    tui.quit();
}

// Split panes (#164): Alt+= adds a pane and opens the views menu, the chosen view
// shows up next to the old one; Ctrl+x zooms the focused pane; q closes it.
async fn test_panes() {
    let Some(server) = common::server() else {
        return;
    };
    let serial = serial_lock();
    let mut child = server.spawn_query(
        "it-tui-panes",
        "SELECT sum(sleep(0.5)) AS tui_marker_panes FROM numbers(600) SETTINGS max_block_size=1",
    );
    wait_query_is_running(server, "it-tui-panes");

    let tui = Tui::start(server, serial);
    tui.wait_for_text("tui_marker_panes");

    tui.send(Event::AltChar('='));
    tui.wait_for_text("Press F2 to choose a view");
    // Select the Tables view in the menu (autojump + submit); it replaces the stub
    tui.send(Event::Char('T'));
    tui.send(Event::Key(Key::Enter));
    // Both panes at once: the queries view and the tables view ("engine" column).
    // The query text is cropped in the halved pane, the query_id column is not.
    tui.wait_for("queries and tables panes", |screen| {
        screen.contains("it-tui-panes") && screen.contains("engine")
    });

    // Zoom the focused (tables) pane: the queries pane is hidden, then back
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("zoomed tables pane", |screen| {
        !screen.contains("it-tui-panes") && screen.contains("engine")
    });
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("unzoomed panes", |screen| {
        screen.contains("it-tui-panes") && screen.contains("engine")
    });

    // A mouse click into the left pane focuses it: zoom must now show the
    // queries view (the click also selects the row under the cursor)
    tui.send(click(20, 10));
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("zoomed queries pane", |screen| {
        screen.contains("it-tui-panes") && !screen.contains("engine")
    });
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("unzoomed panes", |screen| {
        screen.contains("it-tui-panes") && screen.contains("engine")
    });

    // 'l' on the selected query opens its logs in a pane (the default),
    // Ctrl+x zooms it even though the log view is the focused one
    tui.send(Event::Key(Key::Down));
    tui.send(Event::Char('l'));
    tui.wait_for("logs pane", |screen| {
        screen.contains("Logs:") && screen.contains("it-tui-panes")
    });
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("zoomed logs pane", |screen| {
        screen.contains("Logs:") && !screen.contains("it-tui-panes")
    });
    tui.send(Event::CtrlChar('x'));

    // q closes the focused (logs) pane, then the tables pane
    tui.send(Event::Char('q'));
    tui.wait_for("queries and tables panes again", |screen| {
        !screen.contains("Logs:") && screen.contains("it-tui-panes") && screen.contains("engine")
    });
    tui.send(Event::Char('q'));
    tui.wait_for("single pane", |screen| {
        !screen.contains("engine") || !screen.contains("it-tui-panes")
    });

    kill_query(server, "it-tui-panes", &mut child);
    tui.quit();
}

// Logs pane after a view replacement: switching a view collapses the Mux tree
// to a root leaf, adding the logs pane next to it must stay inside the tree
// (used to end up detached: invisible, but querying).
async fn test_table_logs_pane() {
    let Some(server) = common::server() else {
        return;
    };
    let serial = serial_lock();
    let tui = Tui::start(server, serial);
    tui.wait_for_text("Queries (");

    // Switch the pane to the Tables view
    tui.send(Event::Key(Key::F2));
    tui.send(Event::Char('T'));
    tui.send(Event::Key(Key::Enter));
    tui.wait_for_text("MergeTree");

    // Row submit opens the fuzzy actions dialog; Enter picks the first
    // action ("Show table logs")
    tui.send(Event::Key(Key::Down));
    tui.send(Event::Key(Key::Enter));
    tui.wait_for_text("Fuzzy search");
    tui.send(Event::Key(Key::Enter));
    tui.wait_for_text("Logs:");

    tui.quit();
}

// Click-to-focus must work for a pane that lost focus (the logs pane content
// used to refuse take_focus, making it impossible to focus it back).
async fn test_pane_click_focus() {
    let Some(server) = common::server() else {
        return;
    };
    let serial = serial_lock();
    let mut child = server.spawn_query(
        "it-tui-click",
        "SELECT sum(sleep(0.5)) AS tui_marker_click FROM numbers(600) SETTINGS max_block_size=1",
    );
    wait_query_is_running(server, "it-tui-click");

    let tui = Tui::start(server, serial);
    tui.wait_for_text("tui_marker_click");
    tui.send(Event::Key(Key::Down));
    tui.send(Event::Char('l'));
    tui.wait_for_text("Logs:");

    // Focus the left (queries) pane by click, verify via zoom
    tui.send(click(20, 10));
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("zoomed queries", |screen| {
        !screen.contains("Logs:") && screen.contains("it-tui-click")
    });
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("unzoomed", |screen| screen.contains("Logs:"));

    // Focus the right (logs) pane by click, verify via zoom
    tui.send(click(150, 10));
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("zoomed logs", |screen| {
        screen.contains("Logs:") && !screen.contains("it-tui-click")
    });
    tui.send(Event::CtrlChar('x'));
    tui.wait_for("unzoomed again", |screen| screen.contains("it-tui-click"));

    // Drag the separator (screen column 90) to column 130
    let separator_at = |screen: &Screen, x: usize| screen.cell(x, 20) == Some('\u{2502}');
    tui.send(click(90, 20));
    tui.send(Event::Mouse {
        position: Position::new(130, 20),
        event: MouseEvent::Hold(MouseButton::Left),
    });
    tui.send(Event::Mouse {
        position: Position::new(130, 20),
        event: MouseEvent::Release(MouseButton::Left),
    });
    tui.wait_for("separator dragged to column 130", |screen| {
        separator_at(screen, 130) && !separator_at(screen, 90)
    });

    kill_query(server, "it-tui-click", &mut child);
    tui.quit();
}

// Applying a settings change while a log view is the current view must not
// wipe the already fetched entries (the provider's has_view guard keeps the
// existing view; only new views pick the option up).
async fn test_settings_align_keeps_logs() {
    let Some(server) = common::server() else {
        return;
    };
    let serial = serial_lock();
    server.query(
        r#"
        INSERT INTO system.text_log
            (hostname, event_date, event_time, event_time_microseconds,
             thread_id, level, logger_name, query_id, message)
        VALUES
            (hostName(), today(), now(), now64(6),
             1, 'Information', 'TUITestLogger', '', 'tui marker srvlog line')
        "#,
    );

    let tui = Tui::start(server, serial);
    tui.wait_for_text("Queries (");

    // Switch to the server logs view via the fuzzy actions dialog
    tui.send(Event::CtrlChar('p'));
    tui.wait_for_text("Fuzzy search");
    for c in "Server logs".chars() {
        tui.send(Event::Char(c));
    }
    tui.send(Event::Key(Key::Enter));
    tui.wait_for_text("tui marker srvlog");

    // Toggle "align_log_columns" in the settings (F3, search focuses it) and apply
    tui.send(Event::Key(Key::F3));
    tui.wait_for_text("Settings");
    tui.send(Event::Char('/'));
    for c in "align_log_columns".chars() {
        tui.send(Event::Char(c));
    }
    tui.send(Event::Key(Key::Enter));
    tui.send(Event::Char(' '));
    tui.send(Event::Key(Key::Enter));

    // The logs view must keep the previously fetched entries
    tui.wait_for("settings applied", |screen| !screen.contains("<Apply>"));
    tui.wait_for_text("tui marker srvlog");

    tui.quit();
}

common::integration_tests!(
    test_settings_align_keeps_logs,
    test_pane_click_focus,
    test_queries_view,
    test_query_logs_view,
    test_panes,
    test_table_logs_pane
);
