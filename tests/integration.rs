mod common;

use chdig::common::RelativeDateTime;
use chdig::interpreter::clickhouse::TraceType;
use chdig::interpreter::options::ClickHouseOptions;
use chdig::interpreter::{ClickHouse, TextLogArguments};
use chrono::{DateTime, Local, TimeDelta};
use std::collections::HashMap;

// All scenarios run sequentially against one shared server (see the runner at the bottom): with
// process-per-test runners (cargo-nextest) separate #[tokio::test]s would each bootstrap their
// own server. Each scenario still inserts rows with its own unique prefix and filters by it,
// since the data stays on the shared server.

fn window() -> (RelativeDateTime, RelativeDateTime) {
    (
        RelativeDateTime::new(Some(TimeDelta::minutes(10))),
        RelativeDateTime::new(None),
    )
}

fn perfetto_window() -> (DateTime<Local>, DateTime<Local>) {
    (Local::now() - TimeDelta::minutes(10), Local::now())
}

async fn test_connect_and_version() {
    let Some(server) = common::server() else {
        return;
    };
    let chdig = server.chdig().await;
    let server_version = server.query("SELECT version()");
    assert!(
        chdig.version().contains(&server_version),
        "chdig version '{}' does not match server version '{}'",
        chdig.version(),
        server_version
    );
}

async fn test_summary() {
    let Some(server) = common::server() else {
        return;
    };
    let chdig = server.chdig().await;
    let summary = chdig.get_summary(None).await.unwrap();
    assert!(summary.uptime.server > 0);
    assert!(summary.memory.os_total > 0);
    assert_eq!(summary.servers, 1);
}

async fn test_last_query_log() {
    let Some(server) = common::server() else {
        return;
    };
    server.insert_query_log("it-last-1", "it_user_last", 5000, "SELECT 1 FROM it_last");
    server.insert_query_log("it-last-2", "it_user_last", 100, "SELECT 2 FROM it_last");

    let chdig = server.chdig().await;
    let (start, end) = window();
    let block = chdig
        .get_last_query_log(&"it-last-%".to_string(), start, end, 100, None)
        .await
        .unwrap();

    assert_eq!(block.row_count(), 2);
    let mut rows: Vec<(String, String, f64, i64)> = block
        .rows()
        .map(|row| {
            (
                row.get::<String, _>("query_id").unwrap(),
                row.get::<String, _>("user").unwrap(),
                row.get::<f64, _>("elapsed").unwrap(),
                row.get::<i64, _>("peak_memory_usage").unwrap(),
            )
        })
        .collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        rows,
        vec![
            (
                "it-last-1".to_string(),
                "it_user_last".to_string(),
                5.0,
                1048576
            ),
            (
                "it-last-2".to_string(),
                "it_user_last".to_string(),
                0.1,
                1048576
            ),
        ]
    );
}

async fn test_last_query_log_normalized_query() {
    let Some(server) = common::server() else {
        return;
    };
    server.insert_query_log(
        "it-norm-1",
        "it_user_norm",
        100,
        "SELECT 42, ''quoted'' FROM it_norm",
    );

    let chdig = server.chdig().await;
    let (start, end) = window();
    let block = chdig
        .get_last_query_log(&"it-norm-%".to_string(), start, end, 100, None)
        .await
        .unwrap();

    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.get::<String, _>(0, "original_query").unwrap(),
        "SELECT 42, 'quoted' FROM it_norm"
    );
    let normalized = block.get::<String, _>(0, "normalized_query").unwrap();
    assert!(
        normalized.contains('?') && !normalized.contains("42"),
        "literals are not normalized: {normalized}"
    );
}

async fn test_slow_query_log() {
    let Some(server) = common::server() else {
        return;
    };
    // Only queries slower than 1s are considered slow.
    server.insert_query_log("it-slow-1", "it_user_slow", 5000, "SELECT 1 FROM it_slow");
    server.insert_query_log("it-slow-2", "it_user_slow", 100, "SELECT 2 FROM it_slow");

    let chdig = server.chdig().await;
    let (start, end) = window();
    let block = chdig
        .get_slow_query_log(&"it-slow-%".to_string(), start, end, 100, None)
        .await
        .unwrap();

    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<String, _>(0, "query_id").unwrap(), "it-slow-1");
    assert_eq!(block.get::<f64, _>(0, "elapsed").unwrap(), 5.0);
}

async fn test_query_log_out_of_window() {
    let Some(server) = common::server() else {
        return;
    };
    server.insert_query_log("it-window-1", "it_user_window", 100, "SELECT 1");

    let chdig = server.chdig().await;
    // The fixture row is ~1 minute old, a window that ends 5 minutes ago must not see it.
    let start = RelativeDateTime::new(Some(TimeDelta::minutes(10)));
    let end = RelativeDateTime::new(Some(TimeDelta::minutes(5)));
    let block = chdig
        .get_last_query_log(&"it-window-%".to_string(), start, end, 100, None)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 0);
}

fn find_rows(
    block: &chdig::interpreter::clickhouse::Columns,
    column: &str,
    value: &str,
) -> Vec<usize> {
    (0..block.row_count())
        .filter(|&i| {
            block
                .get::<String, _>(i, column)
                .map(|v| v == value)
                .unwrap_or(false)
        })
        .collect()
}

async fn test_processlist_and_kill_query() {
    let Some(server) = common::server() else {
        return;
    };
    let mut child = server.spawn_query(
        "it-proc-1",
        "SELECT sum(sleep(0.5)) FROM numbers(240) SETTINGS max_block_size=1",
    );

    let chdig = server.chdig().await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let block = chdig
            .get_processlist("it-proc-%".to_string(), 100, None)
            .await
            .unwrap();
        if block.row_count() == 1 {
            assert_eq!(block.get::<String, _>(0, "query_id").unwrap(), "it-proc-1");
            assert_eq!(block.get::<String, _>(0, "user").unwrap(), "default");
            assert!(block.get::<f64, _>(0, "elapsed").unwrap() >= 0.0);
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "query did not show up in system.processes"
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    chdig.kill_query("it-proc-1").await.unwrap();
    // KILL QUERY ... SYNC waits for the query to die, so the client must exit with an error.
    let status = child.wait().unwrap();
    assert!(!status.success());
    let block = chdig
        .get_processlist("it-proc-%".to_string(), 100, None)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 0);
}

async fn test_execute_query() {
    let Some(server) = common::server() else {
        return;
    };
    let chdig = server.chdig().await;
    chdig.execute_query("default", "SELECT 1").await.unwrap();
    assert!(
        chdig
            .execute_query("default", "SELECT throwIf(1)")
            .await
            .is_err()
    );
}

async fn test_explains_and_show_create_table() {
    let Some(server) = common::server() else {
        return;
    };
    server.query("CREATE TABLE default.it_explain (key UInt64) ENGINE=MergeTree ORDER BY key");
    // An empty table is optimized to ReadNothing in the plan.
    server.query("INSERT INTO default.it_explain VALUES (1)");

    let chdig = server.chdig().await;

    let syntax = chdig
        .explain_syntax(
            "default",
            "SELECT key FROM it_explain WHERE 1=1",
            &HashMap::new(),
        )
        .await
        .unwrap();
    assert!(syntax.join("\n").contains("SELECT"), "{syntax:?}");

    let settings = HashMap::from([("max_threads".to_string(), "1".to_string())]);
    let syntax = chdig
        .explain_syntax("default", "SELECT key FROM it_explain", &settings)
        .await
        .unwrap();
    assert!(!syntax.is_empty());

    let plan = chdig
        .explain_plan("default", "SELECT key FROM it_explain")
        .await
        .unwrap();
    assert!(plan.join("\n").contains("ReadFromMergeTree"), "{plan:?}");

    let pipeline = chdig
        .explain_pipeline("default", "SELECT key FROM it_explain")
        .await
        .unwrap();
    assert!(!pipeline.is_empty());

    let graph = chdig
        .explain_pipeline_graph("default", "SELECT key FROM it_explain")
        .await
        .unwrap();
    assert!(graph.join("\n").contains("digraph"), "{graph:?}");

    let indexes = chdig
        .explain_plan_indexes("default", "SELECT key FROM it_explain WHERE key = 1")
        .await
        .unwrap();
    assert!(indexes.join("\n").contains("it_explain"), "{indexes:?}");

    let create = chdig
        .show_create_table("default", "it_explain")
        .await
        .unwrap();
    assert!(create.contains("ENGINE = MergeTree"), "{create}");
}

async fn test_text_log() {
    let Some(server) = common::server_with_table("text_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.text_log
            (hostname, event_date, event_time, event_time_microseconds,
             thread_id, level, logger_name, query_id, message)
        VALUES
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 1, 'Information', 'ITLogger', 'it-text-1',
             'it info message'),
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 1, 'Trace', 'ITLogger', 'it-text-1',
             'it trace message')
        "#,
    );

    let chdig = server.chdig().await;
    let args = TextLogArguments {
        query_ids: Some(vec!["it-text-1".to_string()]),
        logger_names: None,
        hostname: None,
        message_filter: None,
        max_level: None,
        start: Local::now() - TimeDelta::minutes(10),
        end: RelativeDateTime::new(None),
    };

    let block = chdig.get_query_logs(&args).await.unwrap();
    assert_eq!(block.row_count(), 2);
    assert_eq!(
        block.get::<String, _>(0, "logger_name").unwrap(),
        "ITLogger"
    );

    let block = chdig
        .get_query_logs(&TextLogArguments {
            max_level: Some("Information".to_string()),
            ..args.clone()
        })
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<String, _>(0, "level").unwrap(), "Information");

    let block = chdig
        .get_query_logs(&TextLogArguments {
            message_filter: Some("trace message".to_string()),
            ..args.clone()
        })
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.get::<String, _>(0, "message").unwrap(),
        "it trace message"
    );
}

async fn test_flamegraph() {
    let Some(server) = common::server_with_table("trace_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.trace_log
            (hostname, event_date, event_time, event_time_microseconds,
             trace_type, thread_id, query_id, trace, symbols, size)
        VALUES
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'CPU', 1, 'it-flame-1', [101, 102],
             ['it_leaf', 'it_main'], 0),
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'CPU', 1, 'it-flame-1', [101, 102],
             ['it_leaf', 'it_main'], 0),
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'Memory', 1, 'it-flame-1', [101, 102],
             ['it_leaf', 'it_main'], -4096)
        "#,
    );

    let chdig = server.chdig().await;
    let query_ids = vec!["it-flame-1".to_string()];

    let block = chdig
        .get_flamegraph(TraceType::CPU, Some(&query_ids), None, None, None)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.get::<String, _>(0, "human_trace").unwrap(),
        "it_main;it_leaf"
    );
    assert_eq!(block.get::<u64, _>(0, "weight").unwrap(), 2);

    let block = chdig
        .get_flamegraph(TraceType::Memory, Some(&query_ids), None, None, None)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<u64, _>(0, "weight").unwrap(), 4096);
}

async fn test_stack_traces_for_perfetto() {
    let Some(server) = common::server_with_table("trace_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.trace_log
            (hostname, event_date, event_time, event_time_microseconds,
             trace_type, thread_id, query_id, trace, symbols, size)
        VALUES
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'CPU', 7, 'it-stack-1', [101, 102],
             ['it_leaf', 'it_main'], 0)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_stack_traces_for_perfetto(Some(&["it-stack-1".to_string()]), start, end)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.get::<Vec<String>, _>(0, "stack").unwrap(),
        vec!["it_main".to_string(), "it_leaf".to_string()]
    );
    assert_eq!(block.get::<String, _>(0, "trace_type").unwrap(), "CPU");
    assert_eq!(block.get::<u64, _>(0, "thread_id").unwrap(), 7);
}

async fn test_trace_log_counters_for_perfetto() {
    let Some(server) = common::server_with_table("trace_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.trace_log
            (hostname, event_date, event_time, event_time_microseconds,
             trace_type, thread_id, query_id, event, increment)
        VALUES
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'ProfileEvent', 1, 'it-cnt-1', 'SelectedRows', 42)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_trace_log_counters_for_perfetto(Some(&["it-cnt-1".to_string()]), start, end)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<String, _>(0, "event").unwrap(), "SelectedRows");
    assert_eq!(block.get::<i64, _>(0, "increment").unwrap(), 42);
}

async fn test_queries_for_perfetto() {
    let Some(server) = common::server() else {
        return;
    };
    server.insert_query_log("it-pq-1", "it_user_pq", 200, "SELECT 1 FROM it_pq");

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_queries_for_perfetto(start, end, &Some(vec!["it-pq-1".to_string()]))
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<String, _>(0, "query_id").unwrap(), "it-pq-1");
    assert_eq!(block.get::<f64, _>(0, "elapsed").unwrap(), 0.2);
}

async fn test_perfetto_query_scope() {
    let Some(server) = common::server() else {
        return;
    };
    server.insert_query_log("it-scope-1", "it_user_scope", 100, "SELECT 1 FROM it_scope");
    // A child query of it-scope-1 (e.g. a remote query on another shard).
    server.query(
        r#"
        INSERT INTO system.query_log
            (hostname, type, event_date, event_time, event_time_microseconds,
             query_start_time, query_start_time_microseconds, query_duration_ms,
             current_database, query, query_id, initial_query_id, is_initial_query,
             user, initial_user)
        VALUES
            (hostName(), 'QueryFinish', toDate(now() - INTERVAL 1 MINUTE),
             now() - INTERVAL 1 MINUTE, now64(6) - INTERVAL 1 MINUTE,
             now() - INTERVAL 1 MINUTE, now64(6) - INTERVAL 1 MINUTE, 50,
             'default', 'SELECT 2 FROM it_scope', 'it-scope-2', 'it-scope-1', 0,
             'it_user_scope', 'it_user_scope')
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let scope = chdig
        .get_perfetto_query_scope("it-scope-1", start, end)
        .await
        .unwrap();
    let mut query_ids = scope.query_ids.unwrap();
    query_ids.sort();
    assert_eq!(query_ids, vec!["it-scope-1", "it-scope-2"]);
    assert!(scope.start <= scope.end);
}

async fn test_metric_log_for_perfetto() {
    let Some(server) = common::server_with_table("metric_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.metric_log
            (event_date, event_time, event_time_microseconds,
             ProfileEvent_Query, CurrentMetric_Query)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 5, 2)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let rows = chdig.get_metric_log_for_perfetto(start, end).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].profile_events["Query"], 5);
    assert_eq!(rows[0].current_metrics["Query"], 2);
    assert!(rows[0].timestamp_ns > 0);
}

async fn test_asynchronous_metric_log_for_perfetto() {
    let Some(server) = common::server_with_table("asynchronous_metric_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.asynchronous_metric_log
            (event_date, event_time, metric, value)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE, 'ITTestMetric', 1.5)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_asynchronous_metric_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "metric", "ITTestMetric");
    assert_eq!(rows.len(), 1);
    assert_eq!(block.get::<f64, _>(rows[0], "value").unwrap(), 1.5);
}

async fn test_part_log_for_perfetto() {
    let Some(server) = common::server_with_table("part_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.part_log
            (hostname, event_type, event_date, event_time, event_time_microseconds,
             duration_ms, database, table, part_name, rows, size_in_bytes, query_id)
        VALUES
            (hostName(), 'NewPart', toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 10, 'default', 'it_part', 'all_1_1_0', 100, 1024,
             'it-part-1')
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_part_log_for_perfetto(Some(&["it-part-1".to_string()]), start, end)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<String, _>(0, "event_type").unwrap(), "NewPart");
    assert_eq!(block.get::<String, _>(0, "part_name").unwrap(), "all_1_1_0");
    assert_eq!(block.get::<u64, _>(0, "rows").unwrap(), 100);
}

async fn test_otel_spans_for_perfetto() {
    let Some(server) = common::server_with_table("opentelemetry_span_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.opentelemetry_span_log
            (trace_id, span_id, parent_span_id, operation_name,
             start_time_us, finish_time_us, finish_date, attribute)
        VALUES
            (generateUUIDv4(), 1, 0, 'ITSpan',
             toUnixTimestamp64Micro(now64(6) - INTERVAL 1 MINUTE),
             toUnixTimestamp64Micro(now64(6) - INTERVAL 1 MINUTE) + 1000,
             today(), map('clickhouse.query_id', 'it-otel-1'))
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_otel_spans_for_perfetto(Some(&["it-otel-1".to_string()]), start, end)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.get::<String, _>(0, "operation_name").unwrap(),
        "ITSpan"
    );
    assert_eq!(block.get::<String, _>(0, "query_id").unwrap(), "it-otel-1");
}

async fn test_asynchronous_insert_log_for_perfetto() {
    let Some(server) = common::server_with_table("asynchronous_insert_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.asynchronous_insert_log
            (event_date, event_time, event_time_microseconds, database, table, format,
             query_id, bytes, status, flush_time, flush_time_microseconds)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'default', 'it_ai', 'Values',
             'it-ai-1', 10, 'Ok', now() - INTERVAL 1 MINUTE, now64(6) - INTERVAL 1 MINUTE)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_asynchronous_insert_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "query_id", "it-ai-1");
    assert_eq!(rows.len(), 1);
    assert_eq!(block.get::<String, _>(rows[0], "status").unwrap(), "Ok");
    assert_eq!(block.get::<String, _>(rows[0], "table").unwrap(), "it_ai");
}

async fn test_error_log_for_perfetto() {
    let Some(server) = common::server_with_table("error_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.error_log
            (event_date, event_time, code, error, value, remote)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             60, 'UNKNOWN_TABLE', 3, 0)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig.get_error_log_for_perfetto(start, end).await.unwrap();
    let rows = find_rows(&block, "error", "UNKNOWN_TABLE");
    assert_eq!(rows.len(), 1);
    assert_eq!(block.get::<u64, _>(rows[0], "value").unwrap(), 3);
}

async fn test_blob_storage_log_for_perfetto() {
    let Some(server) = common::server_with_table("blob_storage_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.blob_storage_log
            (event_date, event_time, event_time_microseconds, event_type,
             query_id, disk_name, bucket, remote_path, data_size)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'Upload',
             'it-blob-1', 's3', 'it-bucket', 'it/path', 100)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_blob_storage_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "query_id", "it-blob-1");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        block.get::<String, _>(rows[0], "bucket").unwrap(),
        "it-bucket"
    );
    assert_eq!(block.get::<u64, _>(rows[0], "data_size").unwrap(), 100);
}

async fn test_session_log_for_perfetto() {
    let Some(server) = common::server_with_table("session_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.session_log
            (type, event_date, event_time, event_time_microseconds, user,
             auth_type, interface, client_address, client_name)
        VALUES
            ('LoginSuccess', toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'it_sess',
             'NO_PASSWORD', 'TCP', toIPv6('::1'), 'it client')
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_session_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "user", "it_sess");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        block.get::<String, _>(rows[0], "type").unwrap(),
        "LoginSuccess"
    );
    assert_eq!(block.get::<String, _>(rows[0], "interface").unwrap(), "TCP");
}

async fn test_background_schedule_pool_log() {
    let Some(server) = common::server_with_table("background_schedule_pool_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.background_schedule_pool_log
            (event_date, event_time, event_time_microseconds, log_name,
             database, table, query_id, duration_ms)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'ITPool', 'default', 'it_bg', 'it-bg-1', 5)
        "#,
    );

    let chdig = server.chdig().await;

    let (start, end) = window();
    let query_ids = chdig
        .get_background_schedule_pool_query_ids(
            Some("ITPool".to_string()),
            "default".to_string(),
            "it_bg".to_string(),
            start,
            end,
            None,
        )
        .await
        .unwrap();
    assert_eq!(query_ids, vec!["it-bg-1"]);

    let (start, end) = window();
    let query_ids = chdig
        .get_background_schedule_pool_query_ids(
            None,
            "default".to_string(),
            "it_bg".to_string(),
            start,
            end,
            None,
        )
        .await
        .unwrap();
    assert_eq!(query_ids, vec!["it-bg-1"]);

    let (start, end) = perfetto_window();
    let block = chdig
        .get_background_schedule_pool_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "query_id", "it-bg-1");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        block.get::<String, _>(rows[0], "log_name").unwrap(),
        "ITPool"
    );
}

async fn test_query_metrics_for_perfetto() {
    let Some(server) = common::server_with_table("query_metric_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.query_metric_log
            (event_date, event_time, event_time_microseconds, query_id,
             memory_usage, peak_memory_usage, ProfileEvent_SelectedRows)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'it-qm-1', 123, 456, 7)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let rows = chdig
        .get_query_metrics_for_perfetto(Some(&["it-qm-1".to_string()]), start, end)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].memory_usage, 123);
    assert_eq!(rows[0].peak_memory_usage, 456);
    assert_eq!(rows[0].profile_events["SelectedRows"], 7);
}

async fn test_query_thread_log_for_perfetto() {
    let Some(server) = common::server_with_table("query_thread_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.query_thread_log
            (hostname, event_date, event_time, event_time_microseconds, query_id,
             thread_name, thread_id, query_duration_ms, peak_memory_usage)
        VALUES
            (hostName(), toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             now64(6) - INTERVAL 1 MINUTE, 'it-qt-1', 'ITThread', 1, 10, 99)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_query_thread_log_for_perfetto(Some(&["it-qt-1".to_string()]), start, end)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(
        block.get::<String, _>(0, "thread_name").unwrap(),
        "ITThread"
    );
    assert_eq!(block.get::<i64, _>(0, "peak_memory_usage").unwrap(), 99);
}

async fn test_s3_queue_log_for_perfetto() {
    let Some(server) = common::server_with_table("s3queue_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.s3queue_log
            (event_date, event_time, database, table, file_name, rows_processed,
             status, processing_start_time, processing_end_time)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             'default', 'it_s3q', 'it.csv', 5,
             'Processed', now() - INTERVAL 1 MINUTE, now() - INTERVAL 1 MINUTE)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_s3_queue_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "file_name", "it.csv");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        block.get::<String, _>(rows[0], "status").unwrap(),
        "Processed"
    );
    assert_eq!(block.get::<u64, _>(rows[0], "rows_processed").unwrap(), 5);
}

async fn test_azure_queue_log_for_perfetto() {
    let Some(server) = common::server_with_table("azure_queue_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.azure_queue_log
            (event_date, event_time, database, table, file_name, rows_processed,
             status, processing_start_time, processing_end_time)
        VALUES
            (toDate(now() - INTERVAL 1 MINUTE), now() - INTERVAL 1 MINUTE,
             'default', 'it_azq', 'it.csv', 5,
             'Processed', now() - INTERVAL 1 MINUTE, now() - INTERVAL 1 MINUTE)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_azure_queue_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "file_name", "it.csv");
    assert_eq!(rows.len(), 1);
    assert_eq!(block.get::<String, _>(rows[0], "table").unwrap(), "it_azq");
}

async fn test_aggregated_zookeeper_log_for_perfetto() {
    let Some(server) = common::server_with_table("aggregated_zookeeper_log") else {
        return;
    };
    server.query(
        r#"
        INSERT INTO system.aggregated_zookeeper_log
            (event_time, session_id, parent_path, operation, count)
        VALUES
            (now() - INTERVAL 1 MINUTE, 7, '/it', 'Get', 3)
        "#,
    );

    let chdig = server.chdig().await;
    let (start, end) = perfetto_window();
    let block = chdig
        .get_aggregated_zookeeper_log_for_perfetto(start, end)
        .await
        .unwrap();
    let rows = find_rows(&block, "parent_path", "/it");
    assert_eq!(rows.len(), 1);
    assert_eq!(block.get::<String, _>(rows[0], "operation").unwrap(), "Get");
    assert_eq!(block.get::<u64, _>(rows[0], "count").unwrap(), 3);
}

async fn test_warnings_and_cluster_hosts() {
    let Some(server) = common::server() else {
        return;
    };
    let chdig = server.chdig().await;
    // No assertions on the content - just that the queries work.
    chdig.get_warnings().await.unwrap();
    // No --cluster option means no hosts.
    assert!(chdig.get_cluster_hosts().await.unwrap().is_empty());
}

async fn test_history() {
    let Some(server) = common::server() else {
        return;
    };
    // A rotated log table, only visible through merge() with --history
    server.query("CREATE TABLE IF NOT EXISTS system.query_log_0 AS system.query_log");
    server.insert_query_log_into(
        "system.query_log_0",
        "it-hist-0",
        "it_user_hist",
        100,
        "SELECT 0 FROM it_hist",
    );
    server.insert_query_log("it-hist-1", "it_user_hist", 100, "SELECT 1 FROM it_hist");

    // Without --history only the live table is visible
    let chdig = server.chdig().await;
    let (start, end) = window();
    let block = chdig
        .get_last_query_log(&"it-hist-%".to_string(), start, end, 100, None)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 1);
    assert_eq!(block.get::<String, _>(0, "query_id").unwrap(), "it-hist-1");

    let chdig = ClickHouse::new(ClickHouseOptions {
        history: true,
        ..server.chdig_options()
    })
    .await
    .unwrap();
    let (start, end) = window();
    let block = chdig
        .get_last_query_log(&"it-hist-%".to_string(), start, end, 100, None)
        .await
        .unwrap();
    let mut query_ids: Vec<String> = (0..block.row_count())
        .map(|i| block.get::<String, _>(i, "query_id").unwrap())
        .collect();
    query_ids.sort();
    assert_eq!(query_ids, vec!["it-hist-0", "it-hist-1"]);
}

async fn test_cluster() {
    let Some(server) = common::server() else {
        return;
    };
    server.insert_query_log("it-clu-1", "it_user_clu", 100, "SELECT 1 FROM it_clu");

    let chdig = ClickHouse::new(ClickHouseOptions {
        cluster: Some(common::CLUSTER.to_string()),
        ..server.chdig_options()
    })
    .await
    .unwrap();

    // Both "replicas" are the same server, so clusterAllReplicas() must return the row twice
    let (start, end) = window();
    let block = chdig
        .get_last_query_log(&"it-clu-%".to_string(), start, end, 100, None)
        .await
        .unwrap();
    assert_eq!(block.row_count(), 2);
    for i in 0..block.row_count() {
        assert_eq!(block.get::<String, _>(i, "query_id").unwrap(), "it-clu-1");
    }

    // DISTINCT hostName() collapses the two replicas into one host
    let hosts = chdig.get_cluster_hosts().await.unwrap();
    assert_eq!(hosts.len(), 1);
}

async fn test_history_with_cluster() {
    let Some(server) = common::server() else {
        return;
    };
    server.query("CREATE TABLE IF NOT EXISTS system.query_log_0 AS system.query_log");
    server.insert_query_log_into(
        "system.query_log_0",
        "it-histclu-0",
        "it_user_histclu",
        100,
        "SELECT 0 FROM it_histclu",
    );
    server.insert_query_log(
        "it-histclu-1",
        "it_user_histclu",
        100,
        "SELECT 1 FROM it_histclu",
    );

    let chdig = ClickHouse::new(ClickHouseOptions {
        history: true,
        cluster: Some(common::CLUSTER.to_string()),
        ..server.chdig_options()
    })
    .await
    .unwrap();

    // clusterAllReplicas() over merge(): both rotated tables, each row from both "replicas"
    let (start, end) = window();
    let block = chdig
        .get_last_query_log(&"it-histclu-%".to_string(), start, end, 100, None)
        .await
        .unwrap();
    let mut query_ids: Vec<String> = (0..block.row_count())
        .map(|i| block.get::<String, _>(i, "query_id").unwrap())
        .collect();
    query_ids.sort();
    assert_eq!(
        query_ids,
        vec![
            "it-histclu-0",
            "it-histclu-0",
            "it-histclu-1",
            "it-histclu-1"
        ]
    );
}

// Under cargo test every scenario is a separate #[test] (parallel threads against the shared
// server), for per-test reporting. cargo-nextest runs every test in a separate process, which
// would bootstrap one server per test - there (the NEXTEST env variable is set) the per-scenario
// tests no-op and the single `integration` test runs all scenarios sequentially instead,
// reporting the failed ones at the end. Panic messages are printed at the failure site by the
// default panic hook, attributed by the "=== running" markers.
macro_rules! integration_tests {
    ($($name:ident),* $(,)?) => {
        mod tests {
            fn rt() -> tokio::runtime::Runtime {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
            }

            fn under_nextest() -> bool {
                std::env::var_os("NEXTEST").is_some()
            }

            $(
                #[test]
                fn $name() {
                    if under_nextest() {
                        return;
                    }
                    rt().block_on(super::$name());
                }
            )*

            #[test]
            fn integration() {
                if !under_nextest() {
                    return;
                }
                let rt = rt();
                let mut failed = Vec::new();
                $(
                    eprintln!("=== running {}", stringify!($name));
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        rt.block_on(super::$name())
                    }))
                    .is_err()
                    {
                        failed.push(stringify!($name));
                    }
                )*
                assert!(failed.is_empty(), "failed scenarios: {failed:?}");
            }
        }
    };
}

integration_tests!(
    test_connect_and_version,
    test_summary,
    test_last_query_log,
    test_last_query_log_normalized_query,
    test_slow_query_log,
    test_query_log_out_of_window,
    test_processlist_and_kill_query,
    test_execute_query,
    test_explains_and_show_create_table,
    test_text_log,
    test_flamegraph,
    test_stack_traces_for_perfetto,
    test_trace_log_counters_for_perfetto,
    test_queries_for_perfetto,
    test_perfetto_query_scope,
    test_metric_log_for_perfetto,
    test_asynchronous_metric_log_for_perfetto,
    test_part_log_for_perfetto,
    test_otel_spans_for_perfetto,
    test_asynchronous_insert_log_for_perfetto,
    test_error_log_for_perfetto,
    test_blob_storage_log_for_perfetto,
    test_session_log_for_perfetto,
    test_background_schedule_pool_log,
    test_query_metrics_for_perfetto,
    test_query_thread_log_for_perfetto,
    test_s3_queue_log_for_perfetto,
    test_azure_queue_log_for_perfetto,
    test_aggregated_zookeeper_log_for_perfetto,
    test_warnings_and_cluster_hosts,
    test_history,
    test_cluster,
    test_history_with_cluster,
);
