// Integration test harness: throwaway ClickHouse server with deterministic system log tables.
//
// The server is started twice:
// 1. With all system log tables enabled, so that the server itself creates them with the real
//    schema for this server version (via SYSTEM FLUSH LOGS).
// 2. With all of them disabled (config.d override with "@remove"), so that the server never
//    writes to them again. The tables are then truncated and tests insert deterministic rows.

use std::env;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use chdig::interpreter::{ClickHouse, options::ClickHouseOptions};

// System log tables chdig reads from. Each gets a section in the server config (phase 1) and a
// "@remove" override (phase 2).
const SYSTEM_LOG_TABLES: &[&str] = &[
    "query_log",
    "query_thread_log",
    "part_log",
    "trace_log",
    "text_log",
    "metric_log",
    "asynchronous_metric_log",
    "error_log",
    "processors_profile_log",
    "opentelemetry_span_log",
    "asynchronous_insert_log",
    "blob_storage_log",
    "query_metric_log",
    "session_log",
    "background_schedule_pool_log",
    "s3queue_log",
    "azure_queue_log",
    "aggregated_zookeeper_log",
];

const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Name of the test cluster from the server config (two replicas pointing to the same server).
pub const CLUSTER: &str = "it_cluster";

pub struct ClickHouseServer {
    pub tcp_port: u16,
    #[allow(dead_code)]
    pub dir: PathBuf,
    binary: PathBuf,
    #[allow(dead_code)]
    server: Option<ServerProcess>,
}

// The server process and the thread that spawned it. PR_SET_PDEATHSIG delivers the signal when
// the spawning *thread* dies (not the process), so the spawning thread must outlive the server:
// it stays alive blocked in Child::wait().
struct ServerProcess {
    pid: libc::pid_t,
    keeper: std::thread::JoinHandle<()>,
}

fn find_clickhouse_binary() -> Option<PathBuf> {
    if let Some(binary) = env::var_os("CLICKHOUSE_BINARY") {
        return Some(PathBuf::from(binary));
    }
    env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|dir| dir.join("clickhouse"))
            .find(|path| path.is_file())
    })
}

fn alloc_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn server_config(dir: &Path, tcp_port: u16) -> String {
    let mut config = format!(
        r#"path: {dir}/data/
tmp_path: {dir}/tmp/
user_files_path: {dir}/user_files/
format_schema_path: {dir}/format_schemas/
logger:
  level: information
  log: {dir}/clickhouse-server.log
  errorlog: {dir}/clickhouse-server.err.log
listen_host: ["127.0.0.1", "127.0.0.2"]
tcp_port: {tcp_port}
mlock_executable: false
user_directories:
  users_xml:
    path: users.yaml
# Two "replicas" that are both this very server, for --cluster tests
remote_servers:
  {CLUSTER}:
    shard:
      replica:
        - host: 127.0.0.1
          port: {tcp_port}
        - host: 127.0.0.2
          port: {tcp_port}
"#,
        CLUSTER = CLUSTER,
        dir = dir.display(),
    );
    for table in SYSTEM_LOG_TABLES {
        config.push_str(&format!(
            r#"{table}:
  database: system
  table: {table}
  flush_interval_milliseconds: 500
"#
        ));
    }
    config
}

const USERS_CONFIG: &str = r#"profiles:
  default:
    readonly: 0
users:
  default:
    password: ""
    networks:
      # Distributed queries to the 127.0.0.2 replica come from a 127.0.0.2 source address
      ip: "127.0.0.0/8"
    profile: default
    quota: default
    access_management: 1
quotas:
  default:
    interval:
      duration: 3600
"#;

fn disable_logs_config() -> String {
    SYSTEM_LOG_TABLES
        .iter()
        .map(|table| format!("{table}:\n  \"@remove\": remove\n"))
        .collect()
}

impl ClickHouseServer {
    fn start(binary: PathBuf) -> Self {
        let target_tmpdir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
        cleanup_stale_dirs(&target_tmpdir);

        let dir = target_tmpdir.join(format!("chdig-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let tcp_port = alloc_port();
        std::fs::write(dir.join("config.yaml"), server_config(&dir, tcp_port)).unwrap();
        std::fs::write(dir.join("users.yaml"), USERS_CONFIG).unwrap();

        // Phase 1: logs enabled, let the server create the tables with the real schema.
        let process = spawn_server(&binary, &dir);
        wait_ready(&binary, tcp_port, &dir, process.pid);
        let mut server = ClickHouseServer {
            tcp_port,
            dir: dir.clone(),
            binary,
            server: Some(process),
        };
        server.materialize_log_tables();

        // Phase 2: logs disabled, the tables are now plain MergeTree tables owned by the tests.
        let config_d = dir.join("config.d");
        std::fs::create_dir_all(&config_d).unwrap();
        std::fs::write(
            config_d.join("disable_system_logs.yaml"),
            disable_logs_config(),
        )
        .unwrap();

        stop_server(server.server.take().unwrap());
        let process = spawn_server(&server.binary, &dir);
        wait_ready(&server.binary, tcp_port, &dir, process.pid);
        server.server = Some(process);

        server.truncate_log_tables();
        server.assert_logging_disabled();
        server
    }

    pub fn query(&self, sql: &str) -> String {
        let output = Command::new(&self.binary)
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.tcp_port.to_string(),
                "--query",
                sql,
            ])
            .output()
            .expect("failed to run clickhouse client");
        assert!(
            output.status.success(),
            "query failed: {}\n{}",
            sql,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .trim_end()
            .to_string()
    }

    /// Run a query in the background (e.g. to populate system.processes).
    pub fn spawn_query(&self, query_id: &str, sql: &str) -> std::process::Child {
        Command::new(&self.binary)
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.tcp_port.to_string(),
                "--query_id",
                query_id,
                "--query",
                sql,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn clickhouse client")
    }

    /// Base options for connecting chdig to this server; tests override fields as needed
    /// (e.g. history/cluster).
    pub fn chdig_options(&self) -> ClickHouseOptions {
        // Bypass options::parse_from() since it loads user configs from default paths,
        // which would make tests non-hermetic.
        let url = format!(
            "tcp://default@127.0.0.1:{}/system?connection_timeout=5s&query_timeout=600s",
            self.tcp_port
        );
        ClickHouseOptions {
            url: Some(url.clone()),
            url_safe: url,
            // Default::default() gives 0 (the clap default_value_t does not apply), which would
            // turn queries using it into LIMIT 0.
            limit: 100000,
            ..Default::default()
        }
    }

    /// chdig's ClickHouse interpreter connected to this server (the code under test).
    pub async fn chdig(&self) -> ClickHouse {
        ClickHouse::new(self.chdig_options())
            .await
            .expect("chdig cannot connect")
    }

    fn materialize_log_tables(&self) {
        self.query("SELECT 1");
        // SYSTEM FLUSH LOGS creates the tables synchronously, so a few rounds suffice. Tables
        // that are still missing are simply not supported by this server version - tests for
        // them are skipped via server_with_table().
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.query("SYSTEM FLUSH LOGS");
            let missing: Vec<&&str> = SYSTEM_LOG_TABLES
                .iter()
                .filter(|table| self.query(&format!("EXISTS TABLE system.{table}")) != "1")
                .collect();
            if missing.is_empty() {
                return;
            }
            if Instant::now() >= deadline {
                eprintln!(
                    "system log tables not supported by this server (tests are skipped): {missing:?}"
                );
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn has_table(&self, table: &str) -> bool {
        self.query(&format!("EXISTS TABLE system.{table}")) == "1"
    }

    fn truncate_log_tables(&self) {
        let tables = self.query(
            "SELECT name FROM system.tables WHERE database = 'system' AND name LIKE '%\\_log'",
        );
        for table in tables.lines() {
            self.query(&format!("TRUNCATE TABLE system.{table}"));
        }
    }

    // After phase 2 the server must not write to the log tables on its own, otherwise tests are
    // not deterministic.
    fn assert_logging_disabled(&self) {
        self.query("SELECT 1");
        self.query("SYSTEM FLUSH LOGS");
        let count = self.query("SELECT count() FROM system.query_log");
        assert_eq!(
            count, "0",
            "system.query_log is still written by the server (logging is not disabled)"
        );
    }

    /// One QueryFinish row in system.query_log, in the last minute.
    pub fn insert_query_log(&self, query_id: &str, user: &str, duration_ms: u64, query: &str) {
        self.insert_query_log_into("system.query_log", query_id, user, duration_ms, query);
    }

    /// Same, but into an arbitrary query_log-structured table (e.g. a rotated query_log_0 for
    /// --history tests).
    pub fn insert_query_log_into(
        &self,
        table: &str,
        query_id: &str,
        user: &str,
        duration_ms: u64,
        query: &str,
    ) {
        self.query(&format!(
            r#"
            INSERT INTO {table}
                (hostname, type, event_date, event_time, event_time_microseconds,
                 query_start_time, query_start_time_microseconds, query_duration_ms,
                 memory_usage, current_database, query, normalized_query_hash,
                 query_id, initial_query_id, is_initial_query, user, initial_user,
                 peak_threads_usage, exception_code, client_name)
            VALUES
                (hostName(), 'QueryFinish',
                 toDate(now() - INTERVAL 1 MINUTE),
                 now() - INTERVAL 1 MINUTE,
                 now64(6) - INTERVAL 1 MINUTE,
                 now() - INTERVAL 1 MINUTE, now64(6) - INTERVAL 1 MINUTE, {duration_ms},
                 1048576, 'default', '{query}', normalizedQueryHash('{query}'),
                 '{query_id}', '{query_id}', 1, '{user}', '{user}',
                 2, 0, '')
            "#
        ));
    }
}

fn spawn_server(binary: &Path, dir: &Path) -> ServerProcess {
    use std::os::unix::process::CommandExt;
    let binary = binary.to_owned();
    let dir = dir.to_owned();
    let (tx, rx) = std::sync::mpsc::channel();
    let keeper = std::thread::spawn(move || {
        let mut command = Command::new(binary);
        command
            .args(["server", "--config-file"])
            .arg(dir.join("config.yaml"))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // Kill the server if the test process dies, so that it cannot leak.
        unsafe {
            command.pre_exec(|| {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
                Ok(())
            });
        }
        let mut child = command.spawn().expect("failed to spawn clickhouse server");
        tx.send(child.id() as libc::pid_t).unwrap();
        let _ = child.wait();
    });
    ServerProcess {
        pid: rx.recv().unwrap(),
        keeper,
    }
}

fn stop_server(server: ServerProcess) {
    unsafe {
        libc::kill(server.pid, libc::SIGTERM);
    }
    server.keeper.join().expect("server keeper thread panicked");
}

fn panic_with_server_log(dir: &Path, reason: &str) -> ! {
    let log = std::fs::read_to_string(dir.join("clickhouse-server.err.log")).unwrap_or_default();
    let tail: Vec<&str> = log.lines().rev().take(30).collect();
    panic!(
        "{reason}, last error log lines:\n{}",
        tail.into_iter().rev().collect::<Vec<_>>().join("\n")
    );
}

fn wait_ready(binary: &Path, tcp_port: u16, dir: &Path, pid: libc::pid_t) {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let ok = Command::new(binary)
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &tcp_port.to_string(),
                "--query",
                "SELECT 1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if ok {
            return;
        }
        // Fail fast instead of burning the whole timeout if the server died (e.g. the port was
        // grabbed by someone else between alloc_port() and bind).
        if unsafe { libc::kill(pid, 0) } != 0 {
            panic_with_server_log(dir, "clickhouse server died during startup");
        }
        if Instant::now() >= deadline {
            panic_with_server_log(
                dir,
                &format!("clickhouse server did not become ready in {READY_TIMEOUT:?}"),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn cleanup_stale_dirs(target_tmpdir: &Path) {
    let Ok(entries) = std::fs::read_dir(target_tmpdir) else {
        return;
    };
    let current = format!("chdig-{}", std::process::id());
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("chdig-") && name != current {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// Shared server for the whole test binary. None (with a message) if there is no clickhouse
/// binary around, so that plain `cargo test` still passes in environments without it.
pub fn server() -> Option<&'static ClickHouseServer> {
    static SERVER: OnceLock<Option<ClickHouseServer>> = OnceLock::new();
    SERVER
        .get_or_init(|| match find_clickhouse_binary() {
            Some(binary) => Some(ClickHouseServer::start(binary)),
            None => {
                eprintln!(
                    "integration tests are skipped: no 'clickhouse' binary in PATH \
                     (set CLICKHOUSE_BINARY to override)"
                );
                None
            }
        })
        .as_ref()
}

/// Like server(), but additionally skips the test if this server version does not have the table.
pub fn server_with_table(table: &str) -> Option<&'static ClickHouseServer> {
    let server = server()?;
    if !server.has_table(table) {
        eprintln!("test is skipped: no system.{table} on this server");
        return None;
    }
    Some(server)
}
