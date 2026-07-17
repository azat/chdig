// TUI tests: the real chdig app running on cursive's puppet backend - events are injected
// programmatically and rendered frames are asserted on as plain text (no PTY).

#[allow(dead_code)]
mod common;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use cursive::Vec2;
use cursive::backends::puppet::observed::ObservedScreen;
use cursive::event::Event;

use common::ClickHouseServer;

// Only one TUI at a time: scenarios act on the single running query visible in the queries view.
// The lock must be taken before spawning the marker query - a query spawned outside of it shows
// up in the other scenario's queries view and steals the row selection there.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial_lock() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Tui {
    input: Sender<Option<Event>>,
    frames: Receiver<ObservedScreen>,
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

        let backend = cursive::backends::puppet::Backend::init(Some(Vec2::new(180, 50)));
        let input = backend.input();
        let frames = backend.stream();

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
                chdig::chdig_tui_async(options, clickhouse, Vec::new(), backend, None)
                    .await
                    .expect("chdig TUI failed");
            });
        });

        Tui {
            input,
            frames,
            thread: Some(thread),
            _serial: serial,
        }
    }

    fn send(&self, event: Event) {
        self.input.send(Some(event)).unwrap();
    }

    /// Wait until the pattern shows up on the screen and return that frame.
    fn wait_for_text(&self, pattern: &str) -> ObservedScreen {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut last_screen = None;
        loop {
            // Force a redraw, so that a new frame is always emitted
            self.send(Event::Refresh);
            if let Ok(screen) = self.frames.recv_timeout(Duration::from_millis(200)) {
                if !screen.find_occurences(pattern).is_empty() {
                    return screen;
                }
                last_screen = Some(screen);
            }
            if Instant::now() >= deadline {
                if let Some(screen) = &last_screen {
                    eprintln!("last screen:\n{}", screen_to_string(screen));
                }
                panic!("'{pattern}' did not appear on the screen");
            }
        }
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

fn screen_to_string(screen: &ObservedScreen) -> String {
    let size = screen.size();
    let mut out = String::new();
    for y in 0..size.y {
        for x in 0..size.x {
            match &screen[Vec2::new(x, y)] {
                Some(cell) => {
                    if let Some(letter) = cell.letter.as_option() {
                        out.push_str(letter);
                    }
                }
                None => out.push(' '),
            }
        }
        out.push('\n');
    }
    out
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
    assert!(!screen.find_occurences("Uptime:").is_empty());
    assert!(!screen.find_occurences("default").is_empty());

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
    tui.send(Event::Key(cursive::event::Key::Down));
    tui.send(Event::Char('l'));
    tui.wait_for_text("tui marker log line");

    kill_query(server, "it-tui-logs", &mut child);
    tui.quit();
}

common::integration_tests!(test_queries_view, test_query_logs_view);
