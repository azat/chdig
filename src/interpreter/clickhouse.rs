use crate::interpreter::options::ClickHouseOptions;
use anyhow::Result;
use clickhouse_rs::{types::Complex, Block, Pool};

// TODO:
// - implement parsing using serde
// - replace clickhouse_rs::client_info::write() (with extend crate) to change the client name
// - escape parameters

pub type Columns = Block<Complex>;

pub struct ClickHouse {
    options: ClickHouseOptions,
    pool: Pool,
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
    pub uptime: ClickHouseServerUptime,
    pub memory: ClickHouseServerMemory,
    pub cpu: ClickHouseServerCPU,
    pub threads: ClickHouseServerThreads,
    pub network: ClickHouseServerNetwork,
    pub blkdev: ClickHouseServerBlockDevices,
}

impl ClickHouse {
    pub fn new(options: ClickHouseOptions) -> Self {
        let pool = Pool::new(options.url.as_str());

        return ClickHouse { options, pool };
    }

    pub async fn version(&mut self) -> String {
        return self
            .execute("SELECT version()")
            .await
            .get::<String, _>(0, 0)
            .expect("Cannot get server version");
    }

    pub async fn get_processlist(&mut self) -> Columns {
        let dbtable = self.get_table_name("system.processes");
        return self
            .execute(
                format!(
                    r#"
                    SELECT
                        ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu,
                        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] AS disk_io,
                        (
                            ProfileEvents['NetworkReceiveBytes'] +
                            ProfileEvents['NetworkSendBytes']
                        ) AS net_io,

                        thread_ids,
                        peak_memory_usage,
                        elapsed,
                        user,
                        query_id,
                        hostName() as host_name,
                        -- TODO: support multi-line queries
                        normalizeQuery(query) AS query
                    FROM {}
                "#,
                    dbtable
                )
                .as_str(),
            )
            .await;
    }

    pub async fn get_summary(&mut self) -> Result<ClickHouseServerSummary> {
        // NOTE: metrics are deltas, so chdig do not need to reimplement this logic by itself.
        let block = self
            .execute(
                &format!(
                    r#"
                    -- TODO: query is suboptimal
                    WITH
                        -- memory detalization
                        (SELECT sum(value::UInt64) FROM {metrics} WHERE metric = 'MemoryTracking')               AS memory_tracked_,
                        (SELECT sum(total_bytes) FROM {tables} WHERE engine IN ('Join','Memory','Buffer','Set')) AS memory_tables_,
                        (SELECT sum(value::UInt64) FROM {asynchronous_metrics} WHERE metric LIKE '%CacheBytes')  AS memory_caches_,
                        (SELECT sum(memory_usage::UInt64) FROM {processes})                                      AS memory_processes_,
                        (SELECT sum(memory_usage::UInt64) FROM {merges})                                         AS memory_merges_,
                        (SELECT sum(bytes_allocated) FROM {dictionaries})                                        AS memory_dictionaries_,
                        (SELECT sum(primary_key_bytes_in_memory_allocated) FROM {parts})                         AS memory_primary_keys_
                    SELECT
                        assumeNotNull(memory_tracked_)                           AS memory_tracked,
                        assumeNotNull(memory_tables_)                            AS memory_tables,
                        assumeNotNull(memory_caches_)                            AS memory_caches,
                        assumeNotNull(memory_processes_)                         AS memory_processes,
                        assumeNotNull(memory_merges_)                            AS memory_merges,
                        assumeNotNull(memory_dictionaries_)                      AS memory_dictionaries,
                        assumeNotNull(memory_primary_keys_)                      AS memory_primary_keys,

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
                "#,
                    metrics=self.get_table_name("system.metrics"),
                    tables=self.get_table_name("system.tables"),
                    processes=self.get_table_name("system.processes"),
                    merges=self.get_table_name("system.merges"),
                    dictionaries=self.get_table_name("system.dictionaries"),
                    parts=self.get_table_name("system.parts"),
                    asynchronous_metrics=self.get_table_name("system.asynchronous_metrics"),
                )
            )
            .await;

        let get = |key: &str| block.get::<u64, _>(0, key).expect(key);

        return Ok(ClickHouseServerSummary {
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

    // TODO: stream logs with LIVE VIEW
    pub async fn get_query_logs(&mut self, query_id: &str) -> Columns {
        // TODO:
        // - optional flush, but right now it gives "blocks should not be empty." error
        //   self.execute("SYSTEM FLUSH LOGS").await;
        // - configure time interval

        let dbtable = self.get_table_name("system.text_log");
        return self
            .execute(
                format!(
                    r#"
                    SELECT
                        // TODO: read native types
                        event_time::String AS event_time,
                        level::String AS level,
                        // LowCardinality is not supported by the driver
                        // logger_name::String AS logger_name,
                        message
                    FROM {}
                    WHERE event_date >= today() AND query_id = '{}'
                    "#,
                    dbtable, query_id
                )
                .as_str(),
            )
            .await;
    }

    /// Return query flamegraph in pyspy format for tfg.
    /// It is the same format as TSV, but with ' ' delimiter between symbols and weight.
    pub async fn get_query_flamegraph(&mut self, query_id: &str) -> Columns {
        let dbtable = self.get_table_name("system.trace_log");
        return self
            .execute(
                format!(
                    r#"
            SELECT
              arrayStringConcat(arrayMap(
                addr -> demangle(addressToSymbol(addr)),
                arrayReverse(trace)
              ), ';') AS human_trace,
              count() weight
            FROM {}
            WHERE
                query_id = '{}' AND
                event_date >= yesterday() AND
                -- TODO: configure interval
                event_time > now() - INTERVAL 1 DAY
                -- TODO: for now show everything (mostly for demo screencast), but it should be CPU and/or configurable
                -- trace_type = 'CPU'
            GROUP BY human_trace
            SETTINGS allow_introspection_functions=1
            "#,
                    dbtable,
                    query_id,
                )
                .as_str(),
            )
            .await;
    }

    /// Return server flamegraph in pyspy format for tfg.
    /// It is the same format as TSV, but with ' ' delimiter between symbols and weight.
    ///
    /// NOTE: in case of cluster we may want to extract all query_ids (by initial_query_id) and
    /// gather everything
    pub async fn get_server_flamegraph(&mut self) -> Columns {
        let dbtable = self.get_table_name("system.trace_log");
        return self
            .execute(
                format!(
                    r#"
                SELECT
                  arrayStringConcat(arrayMap(
                    addr -> demangle(addressToSymbol(addr)),
                    arrayReverse(trace)
                  ), ';') AS human_trace,
                  count() weight
                FROM {}
                WHERE
                    event_date >= yesterday() AND
                    -- TODO: configure internal
                    event_time > now() - INTERVAL 1 MINUTE
                    -- TODO: for now show everything (mostly for demo screencast), but it should be CPU and/or configurable
                    -- trace_type = 'CPU'
                GROUP BY human_trace
                SETTINGS allow_introspection_functions=1
                "#,
                    dbtable,
                ).as_str(),
            )
            .await;
    }

    async fn execute(&mut self, query: &str) -> Columns {
        // TODO:
        // - handle timeouts/errors gracefully
        // - log queries (log crate but capture logs and show it in a separate view)
        let mut client = self.pool.get_handle().await.unwrap();
        // unwrap_or_default() to avoid panic on tokio shutdown
        return client.query(query).fetch_all().await.unwrap_or_default();
    }

    fn get_table_name(&self, dbtable: &str) -> String {
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
