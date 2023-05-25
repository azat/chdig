use crate::interpreter::{options::ClickHouseOptions, ClickHouseAvailableQuirks, ClickHouseQuirks};
use anyhow::{Error, Result};
use chrono::DateTime;
use chrono_tz::Tz;
use clickhouse_rs::{
    types::{Complex, FromSql},
    Block, Pool,
};
use futures_util::StreamExt;

// TODO:
// - implement parsing using serde
// - replace clickhouse_rs::client_info::write() (with extend crate) to change the client name
// - escape parameters

pub type Columns = Block<Complex>;

pub struct ClickHouse {
    options: ClickHouseOptions,
    quirks: ClickHouseQuirks,

    pool: Pool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TraceType {
    CPU,
    Real,
    Memory,
}

#[derive(Default)]
pub struct ClickHouseServerCPU {
    pub count: u64,
    pub user: u64,
    pub system: u64,
}
/// NOTE: Likely misses threads for IO
#[derive(Default)]
pub struct ClickHouseServerThreadPools {
    pub merges_mutations: u64,
    pub fetches: u64,
    pub common: u64,
    pub moves: u64,
    pub schedule: u64,
    pub buffer_flush: u64,
    pub distributed: u64,
    pub message_broker: u64,
}
#[derive(Default)]
pub struct ClickHouseServerThreads {
    pub os_total: u64,
    pub os_runnable: u64,
    pub tcp: u64,
    pub http: u64,
    pub interserver: u64,
    pub pools: ClickHouseServerThreadPools,
}
#[derive(Default)]
pub struct ClickHouseServerMemory {
    pub os_total: u64,
    pub resident: u64,

    pub tracked: u64,
    pub tables: u64,
    pub caches: u64,
    pub processes: u64,
    pub merges: u64,
    pub dictionaries: u64,
    pub primary_keys: u64,
}
/// May have duplicated accounting (due to bridges and stuff)
#[derive(Default)]
pub struct ClickHouseServerNetwork {
    pub send_bytes: u64,
    pub receive_bytes: u64,
}
#[derive(Default)]
pub struct ClickHouseServerUptime {
    pub os: u64,
    pub server: u64,
}
#[derive(Default)]
pub struct ClickHouseServerBlockDevices {
    pub read_bytes: u64,
    pub write_bytes: u64,
}
#[derive(Default)]
pub struct ClickHouseServerSummary {
    pub processes: u64,
    pub merges: u64,
    pub servers: u64,
    pub uptime: ClickHouseServerUptime,
    pub memory: ClickHouseServerMemory,
    pub cpu: ClickHouseServerCPU,
    pub threads: ClickHouseServerThreads,
    pub network: ClickHouseServerNetwork,
    pub blkdev: ClickHouseServerBlockDevices,
}

fn collect_values<'b, T: FromSql<'b>>(block: &'b Columns, column: &str) -> Vec<T> {
    return (0..block.row_count())
        .map(|i| block.get(i, column).unwrap())
        .collect();
}

impl ClickHouse {
    pub async fn new(options: ClickHouseOptions) -> Result<Self> {
        let pool = Pool::new(options.url.as_str());

        let version = pool
            .get_handle()
            .await?
            .query("SELECT version()")
            .fetch_all()
            .await?
            .get::<String, _>(0, 0)?;
        let quirks = ClickHouseQuirks::new(version);
        return Ok(ClickHouse {
            options,
            quirks,
            pool,
        });
    }

    pub fn version(&self) -> String {
        return self.quirks.get_version();
    }

    pub async fn get_slow_query_log(&self, subqueries: bool) -> Result<Columns> {
        let dbtable = self.get_table_name("system.query_log");
        return self
            .execute(
                format!(
                    r#"
                    WITH slow_queries_ids AS (
                        SELECT DISTINCT initial_query_id
                        FROM {db_table}
                        WHERE
                            event_date >= yesterday() AND
                            is_initial_query AND
                            /* To make query faster */
                            query_duration_ms > 1e3 AND
                            query_kind = 'Select'
                        ORDER BY query_duration_ms DESC
                        LIMIT 100
                    )
                    SELECT
                        {pe},
                        thread_ids,
                        // Compatility with system.processlist
                        memory_usage::Int64 AS peak_memory_usage,
                        query_duration_ms/1e3 AS elapsed,
                        user,
                        (count() OVER (PARTITION BY initial_query_id)) AS subqueries,
                        is_initial_query,
                        initial_query_id,
                        query_id,
                        hostName() as host_name,
                        toValidUTF8(query) AS original_query,
                        normalizeQuery(query) AS normalized_query
                    FROM {db_table}
                    WHERE
                        event_date >= yesterday() AND
                        type != 'QueryStart' AND
                        initial_query_id GLOBAL IN slow_queries_ids
                "#,
                    db_table = dbtable,
                    pe = if subqueries {
                        // ProfileEvents are not summarized (unlike progress fields, i.e.
                        // read_rows/read_bytes/...)
                        r#"
                        if(is_initial_query,
                            (sumMap(ProfileEvents) OVER (PARTITION BY initial_query_id)),
                            ProfileEvents
                        ) AS ProfileEvents
                        "#
                    } else {
                        "ProfileEvents"
                    },
                )
                .as_str(),
            )
            .await;
    }

    pub async fn get_processlist(&self, subqueries: bool) -> Result<Columns> {
        let dbtable = self.get_table_name("system.processes");
        return self
            .execute(
                format!(
                    r#"
                    SELECT
                        {pe},
                        thread_ids,
                        peak_memory_usage,
                        elapsed / {q} AS elapsed,
                        user,
                        (count() OVER (PARTITION BY initial_query_id)) AS subqueries,
                        is_initial_query,
                        initial_query_id,
                        query_id,
                        hostName() as host_name,
                        toValidUTF8(query) AS original_query,
                        normalizeQuery(query) AS normalized_query
                    FROM {}
                "#,
                    dbtable,
                    q = if self.quirks.has(ClickHouseAvailableQuirks::ProcessesElapsed) {
                        10
                    } else {
                        1
                    },
                    pe = if subqueries {
                        // ProfileEvents are not summarized (unlike progress fields, i.e.
                        // read_rows/read_bytes/...)
                        r#"
                        if(is_initial_query,
                            (sumMap(ProfileEvents) OVER (PARTITION BY initial_query_id)),
                            ProfileEvents
                        ) AS ProfileEvents
                        "#
                    } else {
                        "ProfileEvents"
                    },
                )
                .as_str(),
            )
            .await;
    }

    pub async fn get_summary(&self) -> Result<ClickHouseServerSummary> {
        // NOTE: metrics are deltas, so chdig do not need to reimplement this logic by itself.
        let block = self
            .execute(
                &format!(
                    r#"
                    WITH
                        -- memory detalization
                        (SELECT sum(value::UInt64) FROM {metrics} WHERE metric = 'MemoryTracking')               AS memory_tracked_,
                        (SELECT sum(total_bytes) FROM {tables} WHERE engine IN ('Join','Memory','Buffer','Set')) AS memory_tables_,
                        (SELECT sum(value::UInt64) FROM {asynchronous_metrics} WHERE metric LIKE '%CacheBytes')  AS memory_caches_,
                        (SELECT sum(memory_usage::UInt64) FROM {processes})                                      AS memory_processes_,
                        (SELECT count() FROM {processes})                                                        AS processes_,
                        (SELECT sum(memory_usage::UInt64) FROM {merges})                                         AS memory_merges_,
                        (SELECT sum(bytes_allocated) FROM {dictionaries})                                        AS memory_dictionaries_,
                        (SELECT sum(primary_key_bytes_in_memory_allocated) FROM {parts})                         AS memory_primary_keys_,
                        (SELECT count() FROM {one})                                                              AS servers_,
                        (SELECT count() FROM {merges})                                                           AS merges_
                    SELECT
                        assumeNotNull(servers_)                                  AS servers,
                        assumeNotNull(memory_tracked_)                           AS memory_tracked,
                        assumeNotNull(memory_tables_)                            AS memory_tables,
                        assumeNotNull(memory_caches_)                            AS memory_caches,
                        assumeNotNull(memory_processes_)                         AS memory_processes,
                        assumeNotNull(processes_)                                AS processes,
                        assumeNotNull(memory_merges_)                            AS memory_merges,
                        assumeNotNull(merges_)                                   AS merges,
                        assumeNotNull(memory_dictionaries_)                      AS memory_dictionaries,
                        assumeNotNull(memory_primary_keys_)                      AS memory_primary_keys,

                        -- NOTE: take into account period for which is was gathered, will be possible after [1].
                        --
                        --   [1]: https://github.com/ClickHouse/ClickHouse/pull/46886
                        asynchronous_metrics.*,
                        metrics.*
                    FROM
                    (
                        -- NOTE: cast should be after aggregation function since the type is Float64
                        SELECT
                            maxIf(value, metric == 'OSUptime')::UInt64               AS os_uptime,
                            maxIf(value, metric == 'Uptime')::UInt64                 AS uptime,
                            -- memory
                            sumIf(value, metric == 'OSMemoryTotal')::UInt64          AS os_memory_total,
                            sumIf(value, metric == 'MemoryResident')::UInt64         AS memory_resident,
                            -- cpu
                            countIf(metric LIKE 'OSUserTimeCPU%')::UInt64            AS cpu_count,
                            sumIf(value, metric LIKE 'OSUserTimeCPU%')::UInt64       AS cpu_user,
                            sumIf(value, metric LIKE 'OSSystemTimeCPU%')::UInt64     AS cpu_system,
                            -- threads detalization
                            sumIf(value, metric = 'HTTPThreads')::UInt64             AS threads_http,
                            sumIf(value, metric = 'TCPThreads')::UInt64              AS threads_tcp,
                            sumIf(value, metric = 'OSThreadsTotal')::UInt64          AS threads_os_total,
                            sumIf(value, metric = 'OSThreadsRunnable')::UInt64       AS threads_os_runnable,
                            sumIf(value, metric = 'InterserverThreads')::UInt64      AS threads_interserver,
                            -- network
                            sumIf(value, metric LIKE 'NetworkSendBytes%')::UInt64    AS net_send_bytes,
                            sumIf(value, metric LIKE 'NetworkReceiveBytes%')::UInt64 AS net_receive_bytes,
                            -- block devices
                            sumIf(value, metric LIKE 'BlockReadBytes%')::UInt64      AS block_read_bytes,
                            sumIf(value, metric LIKE 'BlockWriteBytes%')::UInt64     AS block_write_bytes
                        FROM {asynchronous_metrics}
                    ) as asynchronous_metrics,
                    (
                        SELECT
                            sumIf(value::UInt64, metric == 'BackgroundMergesAndMutationsPoolTask')    AS threads_merges_mutations,
                            sumIf(value::UInt64, metric == 'BackgroundFetchesPoolTask')               AS threads_fetches,
                            sumIf(value::UInt64, metric == 'BackgroundCommonPoolTask')                AS threads_common,
                            sumIf(value::UInt64, metric == 'BackgroundMovePoolTask')                  AS threads_moves,
                            sumIf(value::UInt64, metric == 'BackgroundSchedulePoolTask')              AS threads_schedule,
                            sumIf(value::UInt64, metric == 'BackgroundBufferFlushSchedulePoolTask')   AS threads_buffer_flush,
                            sumIf(value::UInt64, metric == 'BackgroundDistributedSchedulePoolTask')   AS threads_distributed,
                            sumIf(value::UInt64, metric == 'BackgroundMessageBrokerSchedulePoolTask') AS threads_message_broker
                        FROM {metrics}
                    ) as metrics
                    SETTINGS enable_global_with_statement=0
                "#,
                    metrics=self.get_table_name("system.metrics"),
                    tables=self.get_table_name("system.tables"),
                    processes=self.get_table_name("system.processes"),
                    merges=self.get_table_name("system.merges"),
                    dictionaries=self.get_table_name("system.dictionaries"),
                    parts=self.get_table_name("system.parts"),
                    asynchronous_metrics=self.get_table_name("system.asynchronous_metrics"),
                    one=self.get_table_name("system.one"),
                )
            )
            .await?;

        let get = |key: &str| block.get::<u64, _>(0, key).expect(key);

        return Ok(ClickHouseServerSummary {
            processes: get("processes"),
            merges: get("merges"),
            servers: get("servers"),

            uptime: ClickHouseServerUptime {
                os: get("os_uptime"),
                server: get("uptime"),
            },

            memory: ClickHouseServerMemory {
                os_total: get("os_memory_total"),
                resident: get("memory_resident"),

                tracked: get("memory_tracked"),
                tables: get("memory_tables"),
                caches: get("memory_caches"),
                processes: get("memory_processes"),
                merges: get("memory_merges"),
                dictionaries: get("memory_dictionaries"),
                primary_keys: get("memory_primary_keys"),
            },

            cpu: ClickHouseServerCPU {
                count: get("cpu_count"),
                user: get("cpu_user"),
                system: get("cpu_system"),
            },

            threads: ClickHouseServerThreads {
                os_total: get("threads_os_total"),
                os_runnable: get("threads_os_runnable"),
                http: get("threads_http"),
                tcp: get("threads_tcp"),
                interserver: get("threads_interserver"),
                pools: ClickHouseServerThreadPools {
                    merges_mutations: get("threads_merges_mutations"),
                    fetches: get("threads_fetches"),
                    common: get("threads_common"),
                    moves: get("threads_moves"),
                    schedule: get("threads_schedule"),
                    buffer_flush: get("threads_buffer_flush"),
                    distributed: get("threads_distributed"),
                    message_broker: get("threads_merges_mutations"),
                },
            },

            network: ClickHouseServerNetwork {
                send_bytes: get("net_send_bytes"),
                receive_bytes: get("net_receive_bytes"),
            },

            blkdev: ClickHouseServerBlockDevices {
                read_bytes: get("block_read_bytes"),
                write_bytes: get("block_write_bytes"),
            },
        });
    }

    pub async fn kill_query(&self, query_id: &str) -> Result<()> {
        let &query;
        if let Some(cluster) = self.options.cluster.as_ref() {
            query = format!(
                "KILL QUERY ON CLUSTER {} WHERE query_id = '{}' SYNC",
                cluster, query_id
            );
        } else {
            query = format!("KILL QUERY WHERE query_id = '{}' SYNC", query_id);
        }
        return self.execute_simple(&query).await;
    }

    // TODO: copy all settings from the query
    pub async fn explain_syntax(&self, query: &str) -> Result<Vec<String>> {
        return Ok(collect_values(
            &self.execute(&format!("EXPLAIN SYNTAX {}", query)).await?,
            "explain",
        ));
    }

    // TODO: copy all settings from the query
    pub async fn explain_plan(&self, query: &str) -> Result<Vec<String>> {
        return Ok(collect_values(
            &self
                .execute(&format!("EXPLAIN PLAN actions=1 {}", query))
                .await?,
            "explain",
        ));
    }

    // TODO: copy all settings from the query
    pub async fn explain_pipeline(&self, query: &str) -> Result<Vec<String>> {
        return Ok(collect_values(
            &self.execute(&format!("EXPLAIN PIPELINE {}", query)).await?,
            "explain",
        ));
    }

    pub async fn get_query_logs(
        &self,
        query_ids: &Vec<String>,
        event_time_microseconds: Option<DateTime<Tz>>,
    ) -> Result<Columns> {
        // TODO:
        // - optional flush, but right now it gives "blocks should not be empty." error
        //   self.execute("SYSTEM FLUSH LOGS").await;
        // - configure time interval
        //
        // NOTE:
        // - we cannot use LIVE VIEW, since
        //   a) they are pretty complex
        //   b) it does not work in case we monitor the whole cluster

        let dbtable = self.get_table_name("system.text_log");
        return self
            .execute(
                format!(
                    r#"
                    SELECT
                        event_time,
                        event_time_microseconds,
                        level::String AS level,
                        // LowCardinality is not supported by the driver
                        // logger_name::String AS logger_name,
                        message
                    FROM {}
                    WHERE event_date >= yesterday() AND query_id IN ('{}') {}
                    "#,
                    dbtable,
                    query_ids.join("','"),
                    event_time_microseconds
                        .and_then(|x| Some(format!(
                            " AND event_time_microseconds > parseDateTime64BestEffort('{}', 6)",
                            x
                        )))
                        .unwrap_or_default()
                )
                .as_str(),
            )
            .await;
    }

    /// Return query flamegraph in pyspy format for tfg.
    /// It is the same format as TSV, but with ' ' delimiter between symbols and weight.
    pub async fn get_flamegraph(
        &self,
        trace_type: TraceType,
        query_ids: Option<&Vec<String>>,
    ) -> Result<Columns> {
        let dbtable = self.get_table_name("system.trace_log");
        return self
            .execute(&format!(
                r#"
            SELECT
              arrayStringConcat(arrayMap(
                addr -> demangle(addressToSymbol(addr)),
                arrayReverse(trace)
              ), ';') AS human_trace,
              {} weight
            FROM {}
            WHERE
                event_date >= yesterday()
                -- TODO: configure interval
                AND event_time > now() - INTERVAL 1 DAY
                AND trace_type = '{:?}'
                {}
            GROUP BY human_trace
            SETTINGS allow_introspection_functions=1
            "#,
                if trace_type == TraceType::Memory {
                    "abs(sum(size))"
                } else {
                    "count()"
                },
                dbtable,
                trace_type,
                if query_ids.is_some() {
                    format!("AND query_id IN ('{}')", query_ids.unwrap().join("','"))
                } else {
                    "".to_string()
                },
            ))
            .await;
    }

    pub async fn get_live_query_flamegraph(&self, query_ids: &Vec<String>) -> Result<Columns> {
        let dbtable = self.get_table_name("system.stack_trace");
        return self
            .execute(&format!(
                r#"
            SELECT
              arrayStringConcat(arrayMap(
                addr -> demangle(addressToSymbol(addr)),
                arrayReverse(trace)
              ), ';') AS human_trace,
              count() weight
            FROM {}
            WHERE query_id IN ('{}')
            GROUP BY human_trace
            SETTINGS
                allow_introspection_functions=1,
                /* TODO: add settings support for clickhouse-rs, and use them with
                 * is_important=false (for compatiblity with previous versions at least) */
                storage_system_stack_trace_pipe_read_timeout_ms=1000
            "#,
                dbtable,
                query_ids.join("','"),
            ))
            .await;
    }

    pub async fn execute(&self, query: &str) -> Result<Columns> {
        return Ok(self
            .pool
            .get_handle()
            .await?
            .query(query)
            .fetch_all()
            .await?);
    }

    async fn execute_simple(&self, query: &str) -> Result<()> {
        let mut client = self.pool.get_handle().await?;
        let mut stream = client.query(query).stream_blocks();
        let ret = stream.next().await;
        if let Some(Err(err)) = ret {
            return Err(Error::new(err));
        } else {
            return Ok(());
        }
    }

    pub fn get_table_name(&self, dbtable: &str) -> String {
        let cluster = self
            .options
            .cluster
            .as_ref()
            .unwrap_or(&"".to_string())
            .clone();
        if cluster.is_empty() {
            return dbtable.to_string();
        }
        return format!("clusterAllReplicas({}, {})", cluster, dbtable);
    }
}
