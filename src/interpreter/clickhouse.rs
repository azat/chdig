use crate::interpreter::{options::ClickHouseOptions, ClickHouseAvailableQuirks, ClickHouseQuirks};
use anyhow::{Error, Result};
use chrono::{DateTime, Local};
use clickhouse_rs::{
    types::{Complex, FromSql},
    Block, Options, Pool,
};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::str::FromStr;

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
#[allow(clippy::upper_case_acronyms)]
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
    pub backups: u64,
    pub io: u64,
    pub remote_io: u64,
    pub queries: u64,
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
    pub _os: u64,
    pub server: u64,
}
/// May does not take into account some block devices (due to filter by sd*/nvme*/vd*)
#[derive(Default)]
pub struct ClickHouseServerBlockDevices {
    pub read_bytes: u64,
    pub write_bytes: u64,
}
#[derive(Default)]
pub struct ClickHouseServerStorages {
    pub buffer_bytes: u64,
    // Replace with bytes once [1] will be merged.
    //
    //   [1]: https://github.com/ClickHouse/ClickHouse/pull/50238
    pub distributed_insert_files: u64,
}
#[derive(Default)]
pub struct ClickHouseServerRows {
    pub selected: u64,
    pub inserted: u64,
}
#[derive(Default)]
pub struct ClickHouseServerSummary {
    pub processes: u64,
    pub merges: u64,
    pub mutations: u64,
    pub replication_queue: u64,
    pub replication_queue_tries: u64,
    pub fetches: u64,
    pub servers: u64,
    pub rows: ClickHouseServerRows,
    pub storages: ClickHouseServerStorages,
    pub uptime: ClickHouseServerUptime,
    pub memory: ClickHouseServerMemory,
    pub cpu: ClickHouseServerCPU,
    pub threads: ClickHouseServerThreads,
    pub network: ClickHouseServerNetwork,
    pub blkdev: ClickHouseServerBlockDevices,
    pub update_interval: u64,
}

fn collect_values<'b, T: FromSql<'b>>(block: &'b Columns, column: &str) -> Vec<T> {
    return (0..block.row_count())
        .map(|i| block.get(i, column).unwrap())
        .collect();
}

impl ClickHouse {
    pub async fn new(options: ClickHouseOptions) -> Result<Self> {
        let url = options.url.clone().unwrap();
        let connect_options: Options = Options::from_str(&url)?
            .with_setting(
                "storage_system_stack_trace_pipe_read_timeout_ms",
                1000,
                /* is_important= */ false,
            )
            // FIXME: ClickHouse's analyzer does not handle ProfileEvents.Names (and similar), it throws:
            //
            //   Invalid column type for ColumnUnique::insertRangeFrom. Expected String, got LowCardinality(String)
            //
            .with_setting("allow_experimental_analyzer", false, true)
            // TODO: add support of Map type for LowCardinality in the driver
            .with_setting("low_cardinality_allow_in_native_format", false, true);
        let pool = Pool::new(connect_options);

        let version = pool
            .get_handle()
            .await
            .map_err(|e| {
                Error::msg(format!(
                    "Cannot connect to ClickHouse at {} ({})",
                    options.url_safe, e
                ))
            })?
            .query("SELECT version()")
            .fetch_all()
            .await?
            .get::<String, _>(0, 0)?;
        let quirks = ClickHouseQuirks::new(version.clone());
        return Ok(ClickHouse {
            options,
            quirks,
            pool,
        });
    }

    pub fn version(&self) -> String {
        return self.quirks.get_version();
    }

    pub async fn get_slow_query_log(
        &self,
        filter: &String,
        start: DateTime<Local>,
        end: DateTime<Local>,
        limit: u64,
    ) -> Result<Columns> {
        let start = start
            .timestamp_nanos_opt()
            .ok_or(Error::msg("Invalid start"))?;
        let end = end.timestamp_nanos_opt().ok_or(Error::msg("Invalid end"))?;
        let dbtable = self.get_table_name("system.query_log");
        return self
            .execute(
                format!(
                    r#"
                    WITH
                        fromUnixTimestamp64Nano({start}) AS start_,
                        fromUnixTimestamp64Nano({end})   AS end_,
                        slow_queries_ids AS (
                            SELECT DISTINCT initial_query_id
                            FROM {db_table}
                            WHERE
                                event_date BETWEEN toDate(start_) AND toDate(end_) AND
                                event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                                is_initial_query AND
                                /* To make query faster */
                                query_duration_ms > 1e3
                                {filter}
                            ORDER BY query_duration_ms DESC
                            LIMIT {limit}
                        )
                    SELECT
                        ProfileEvents.Names,
                        ProfileEvents.Values,
                        Settings.Names,
                        Settings.Values,
                        thread_ids,
                        // Compatibility with system.processlist
                        memory_usage::Int64 AS peak_memory_usage,
                        query_duration_ms/1e3 AS elapsed,
                        user,
                        is_initial_query,
                        initial_query_id,
                        query_id,
                        hostName() as host_name,
                        current_database,
                        query_start_time_microseconds,
                        event_time_microseconds AS query_end_time_microseconds,
                        toValidUTF8(query) AS original_query,
                        normalizeQuery(query) AS normalized_query
                    FROM {db_table}
                    PREWHERE
                        event_date BETWEEN toDate(start_) AND toDate(end_) AND
                        event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                        type != 'QueryStart' AND
                        initial_query_id GLOBAL IN slow_queries_ids
                "#,
                    db_table = dbtable,
                    filter = if !filter.is_empty() {
                        format!("AND (client_hostname LIKE '{0}' OR os_user LIKE '{0}' OR user LIKE '{0}' OR initial_user LIKE '{0}' OR client_name LIKE '{0}' OR query_id LIKE '{0}' OR query LIKE '{0}')", &filter)
                    } else {
                        "".to_string()
                    }
                )
                .as_str(),
            )
            .await;
    }

    pub async fn get_last_query_log(
        &self,
        filter: &String,
        start: DateTime<Local>,
        end: DateTime<Local>,
        limit: u64,
    ) -> Result<Columns> {
        let start = start
            .timestamp_nanos_opt()
            .ok_or(Error::msg("Invalid start"))?;
        let end = end.timestamp_nanos_opt().ok_or(Error::msg("Invalid end"))?;
        // TODO:
        // - propagate sort order from the table
        // - distributed_group_by_no_merge=2 is broken for this query with WINDOW function
        let dbtable = self.get_table_name("system.query_log");
        return self
            .execute(
                format!(
                    r#"
                    WITH
                        fromUnixTimestamp64Nano({start}) AS start_,
                        fromUnixTimestamp64Nano({end})   AS end_,
                        last_queries_ids AS (
                            SELECT DISTINCT initial_query_id
                            FROM {db_table}
                            WHERE
                                event_date BETWEEN toDate(start_) AND toDate(end_) AND
                                event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                                type != 'QueryStart'
                                {filter}
                            ORDER BY event_date DESC, event_time DESC
                            LIMIT {limit}
                        )
                    SELECT
                        ProfileEvents.Names,
                        ProfileEvents.Values,
                        Settings.Names,
                        Settings.Values,
                        thread_ids,
                        // Compatibility with system.processlist
                        memory_usage::Int64 AS peak_memory_usage,
                        query_duration_ms/1e3 AS elapsed,
                        user,
                        is_initial_query,
                        initial_query_id,
                        query_id,
                        hostName() as host_name,
                        current_database,
                        query_start_time_microseconds,
                        event_time_microseconds AS query_end_time_microseconds,
                        toValidUTF8(query) AS original_query,
                        normalizeQuery(query) AS normalized_query
                    FROM {db_table}
                    PREWHERE
                        event_date BETWEEN toDate(start_) AND toDate(end_) AND
                        event_time BETWEEN toDateTime(start_) AND toDateTime(end_) AND
                        type != 'QueryStart' AND
                        initial_query_id GLOBAL IN last_queries_ids
                "#,
                    db_table = dbtable,
                    filter = if !filter.is_empty() {
                        format!("AND (client_hostname LIKE '{0}' OR os_user LIKE '{0}' OR user LIKE '{0}' OR initial_user LIKE '{0}' OR client_name LIKE '{0}' OR query_id LIKE '{0}' OR query LIKE '{0}')", &filter)
                    } else {
                        "".to_string()
                    }
                )
                .as_str(),
            )
            .await;
    }

    pub async fn get_processlist(&self, filter: String, limit: u64) -> Result<Columns> {
        let dbtable = self.get_table_name("system.processes");
        return self
            .execute(
                format!(
                    r#"
                    SELECT
                        ProfileEvents.Names,
                        ProfileEvents.Values,
                        Settings.Names,
                        Settings.Values,
                        thread_ids,
                        peak_memory_usage,
                        elapsed / {q} AS elapsed,
                        user,
                        is_initial_query,
                        initial_query_id,
                        query_id,
                        hostName() AS host_name,
                        {current_database} AS current_database,
                        /* NOTE: now64()/elapsed does not have enough precision to handle starting
                         * time properly, while this column is used for querying system.text_log,
                         * and it should be the smallest time that we are looking for */
                        (now64(6) - elapsed - 1) AS query_start_time_microseconds,
                        now64(6) AS query_end_time_microseconds,
                        toValidUTF8(query) AS original_query,
                        normalizeQuery(query) AS normalized_query
                    FROM {}
                    {filter}
                    LIMIT {limit}
                "#,
                    dbtable,
                    q = if self.quirks.has(ClickHouseAvailableQuirks::ProcessesElapsed) {
                        10
                    } else {
                        1
                    },
                    current_database = if self.quirks.has(ClickHouseAvailableQuirks::ProcessesCurrentDatabase) {
                        // This is required for EXPLAIN (available since 20.6),
                        // so EXPLAIN with non-default current_database will be broken from processes view.
                        "'default'"
                    } else {
                        "current_database"
                    },
                    filter = if !filter.is_empty() {
                        format!("WHERE (client_hostname LIKE '{0}' OR os_user LIKE '{0}' OR user LIKE '{0}' OR initial_user LIKE '{0}' OR client_name LIKE '{0}' OR query_id LIKE '{0}' OR query LIKE '{0}')", &filter)
                    } else {
                        "".to_string()
                    }
                )
                .as_str(),
            )
            .await;
    }

    pub async fn get_summary(&self) -> Result<ClickHouseServerSummary> {
        // NOTE: metrics (but not all of them) are deltas, so chdig do not need to reimplement this logic by itself.
        let block = self
            .execute(
                &format!(
                    r#"
                    WITH
                        -- memory detalization
                        (SELECT sum(CAST(value AS UInt64)) FROM {metrics} WHERE metric = 'MemoryTracking')       AS memory_tracked_,
                        (SELECT sum(total_bytes) FROM {tables} WHERE engine IN ('Join','Memory','Buffer','Set')) AS memory_tables_,
                        (SELECT sum(CAST(value AS UInt64)) FROM {asynchronous_metrics} WHERE metric LIKE '%CacheBytes' AND metric NOT LIKE '%Filesystem%') AS memory_caches_,
                        (SELECT sum(CAST(memory_usage AS UInt64)) FROM {processes})                              AS memory_processes_,
                        (SELECT count() FROM {processes})                                                        AS processes_,
                        (SELECT sum(CAST(memory_usage AS UInt64)) FROM {merges})                                 AS memory_merges_,
                        (SELECT sum(bytes_allocated) FROM {dictionaries})                                        AS memory_dictionaries_,
                        (SELECT count() FROM {one})                                                              AS servers_,
                        (SELECT count() FROM {merges})                                                           AS merges_,
                        (SELECT count() FROM {mutations} WHERE NOT is_done)                                      AS mutations_,
                        (SELECT count() FROM {replication_queue})                                                AS replication_queue_,
                        (SELECT sum(num_tries) FROM {replication_queue})                                         AS replication_queue_tries_,
                        (SELECT count() FROM {fetches})                                                          AS fetches_
                    SELECT
                        assumeNotNull(servers_)                                  AS servers,
                        assumeNotNull(memory_tracked_)                           AS memory_tracked,
                        assumeNotNull(memory_tables_)                            AS memory_tables,
                        assumeNotNull(memory_caches_)                            AS memory_caches,
                        assumeNotNull(memory_processes_)                         AS memory_processes,
                        assumeNotNull(processes_)                                AS processes,
                        assumeNotNull(memory_merges_)                            AS memory_merges,
                        assumeNotNull(merges_)                                   AS merges,
                        assumeNotNull(mutations_)                                AS mutations,
                        assumeNotNull(replication_queue_)                        AS replication_queue,
                        assumeNotNull(replication_queue_tries_)                  AS replication_queue_tries,
                        assumeNotNull(fetches_)                                  AS fetches,
                        assumeNotNull(memory_dictionaries_)                      AS memory_dictionaries,

                        asynchronous_metrics.*,
                        events.*,
                        metrics.*
                    FROM
                    (
                        WITH
                            -- exclude MD/LVM
                            metric LIKE '%_sd%' OR metric LIKE '%_nvme%' OR metric LIKE '%_vd%' AS is_disk,
                            metric LIKE '%vlan%' AS is_vlan
                        -- NOTE: cast should be after aggregation function since the type is Float64
                        SELECT
                            CAST(minIf(value, metric == 'OSUptime') AS UInt64)       AS os_uptime,
                            CAST(min(uptime()) AS UInt64)                            AS uptime,
                            -- memory
                            CAST(sumIf(value, metric == 'OSMemoryTotal') AS UInt64)  AS os_memory_total,
                            CAST(sumIf(value, metric == 'MemoryResident') AS UInt64) AS memory_resident,
                            -- May differs from primary_key_bytes_in_memory_allocated from
                            -- system.parts, since it takes into account only active parts
                            CAST(sumIf(value, metric == 'TotalPrimaryKeyBytesInMemoryAllocated') AS UInt64) AS memory_primary_keys,
                            -- cpu
                            CAST(countIf(metric LIKE 'OSUserTimeCPU%') AS UInt64)            AS cpu_count,
                            CAST(sumIf(value, metric LIKE 'OSUserTimeCPU%') AS UInt64)       AS cpu_user,
                            CAST(sumIf(value, metric LIKE 'OSSystemTimeCPU%') AS UInt64)     AS cpu_system,
                            -- threads detalization
                            CAST(sumIf(value, metric = 'HTTPThreads') AS UInt64)             AS threads_http,
                            CAST(sumIf(value, metric = 'TCPThreads') AS UInt64)              AS threads_tcp,
                            CAST(sumIf(value, metric = 'OSThreadsTotal') AS UInt64)          AS threads_os_total,
                            CAST(sumIf(value, metric = 'OSThreadsRunnable') AS UInt64)       AS threads_os_runnable,
                            CAST(sumIf(value, metric = 'InterserverThreads') AS UInt64)      AS threads_interserver,
                            -- network
                            CAST(sumIf(value, metric LIKE 'NetworkSendBytes%' AND NOT is_vlan) AS UInt64)    AS net_send_bytes,
                            CAST(sumIf(value, metric LIKE 'NetworkReceiveBytes%' AND NOT is_vlan) AS UInt64) AS net_receive_bytes,
                            -- block devices
                            CAST(sumIf(value, metric LIKE 'BlockReadBytes%' AND is_disk) AS UInt64)      AS block_read_bytes,
                            CAST(sumIf(value, metric LIKE 'BlockWriteBytes%' AND is_disk) AS UInt64)     AS block_write_bytes,
                            -- update intervals
                            CAST(anyLastIf(value, metric == 'AsynchronousMetricsUpdateInterval') AS UInt64) AS metrics_update_interval
                        FROM {asynchronous_metrics}
                    ) as asynchronous_metrics,
                    (
                        SELECT
                            sumIf(CAST(value AS UInt64), event == 'SelectedRows') AS selected_rows,
                            sumIf(CAST(value AS UInt64), event == 'InsertedRows') AS inserted_rows
                        FROM {events}
                    ) as events,
                    (
                        SELECT
                            sumIf(CAST(value AS UInt64), metric == 'StorageBufferBytes') AS storage_buffer_bytes,
                            sumIf(CAST(value AS UInt64), metric == 'DistributedFilesToInsert') AS storage_distributed_insert_files,

                            sumIf(CAST(value AS UInt64), metric == 'BackgroundMergesAndMutationsPoolTask')    AS threads_merges_mutations,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundFetchesPoolTask')               AS threads_fetches,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundCommonPoolTask')                AS threads_common,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundMovePoolTask')                  AS threads_moves,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundSchedulePoolTask')              AS threads_schedule,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundBufferFlushSchedulePoolTask')   AS threads_buffer_flush,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundDistributedSchedulePoolTask')   AS threads_distributed,
                            sumIf(CAST(value AS UInt64), metric == 'BackgroundMessageBrokerSchedulePoolTask') AS threads_message_broker,
                            sumIf(CAST(value AS UInt64), metric IN (
                                'BackupThreadsActive',
                                'RestoreThreadsActive',
                                'BackupsIOThreadsActive'
                            )) AS threads_backups,
                            sumIf(CAST(value AS UInt64), metric IN (
                                'DiskObjectStorageAsyncThreadsActive',
                                'ThreadPoolRemoteFSReaderThreadsActive',
                                'StorageS3ThreadsActive'
                            )) AS threads_remote_io,
                            sumIf(CAST(value AS UInt64), metric IN (
                                'IOThreadsActive',
                                'IOWriterThreadsActive',
                                'IOPrefetchThreadsActive',
                                'MarksLoaderThreadsActive'
                            )) AS threads_io,
                            sumIf(CAST(value AS UInt64), metric IN (
                                'QueryPipelineExecutorThreadsActive',
                                'QueryThread',
                                'AggregatorThreadsActive',
                                'StorageDistributedThreadsActive',
                                'DestroyAggregatesThreadsActive'
                            )) AS threads_queries
                        FROM {metrics}
                    ) as metrics
                    SETTINGS enable_global_with_statement=0
                "#,
                    metrics=self.get_table_name("system.metrics"),
                    events=self.get_table_name("system.events"),
                    tables=self.get_table_name("system.tables"),
                    processes=self.get_table_name("system.processes"),
                    merges=self.get_table_name("system.merges"),
                    mutations=self.get_table_name("system.mutations"),
                    replication_queue=self.get_table_name("system.replication_queue"),
                    fetches=self.get_table_name("system.replicated_fetches"),
                    dictionaries=self.get_table_name("system.dictionaries"),
                    asynchronous_metrics=self.get_table_name("system.asynchronous_metrics"),
                    one=self.get_table_name("system.one"),
                )
            )
            .await?;

        let get = |key: &str| {
            // By subquery.column
            if let Ok(value) = block.get::<u64, _>(0, key) {
                return value;
            }

            let parts = key.split(".").collect::<Vec<&str>>();
            assert!(parts.len() <= 2);
            // By column
            return block.get::<u64, _>(0, parts[parts.len() - 1]).expect(key);
        };

        return Ok(ClickHouseServerSummary {
            processes: get("processes"),
            merges: get("merges"),
            mutations: get("mutations"),
            replication_queue: get("replication_queue"),
            replication_queue_tries: get("replication_queue_tries"),
            fetches: get("fetches"),
            servers: get("servers"),

            uptime: ClickHouseServerUptime {
                _os: get("asynchronous_metrics.os_uptime"),
                server: get("asynchronous_metrics.uptime"),
            },

            rows: ClickHouseServerRows {
                selected: get("events.selected_rows"),
                inserted: get("events.inserted_rows"),
            },

            storages: ClickHouseServerStorages {
                buffer_bytes: get("metrics.storage_buffer_bytes"),
                distributed_insert_files: get("metrics.storage_distributed_insert_files"),
            },

            memory: ClickHouseServerMemory {
                os_total: get("asynchronous_metrics.os_memory_total"),
                resident: get("asynchronous_metrics.memory_resident"),

                tracked: get("memory_tracked"),
                tables: get("memory_tables"),
                caches: get("memory_caches"),
                processes: get("memory_processes"),
                merges: get("memory_merges"),
                dictionaries: get("memory_dictionaries"),
                primary_keys: get("asynchronous_metrics.memory_primary_keys"),
            },

            cpu: ClickHouseServerCPU {
                count: get("asynchronous_metrics.cpu_count"),
                user: get("asynchronous_metrics.cpu_user"),
                system: get("asynchronous_metrics.cpu_system"),
            },

            threads: ClickHouseServerThreads {
                os_total: get("asynchronous_metrics.threads_os_total"),
                os_runnable: get("asynchronous_metrics.threads_os_runnable"),
                http: get("asynchronous_metrics.threads_http"),
                tcp: get("asynchronous_metrics.threads_tcp"),
                interserver: get("asynchronous_metrics.threads_interserver"),
                pools: ClickHouseServerThreadPools {
                    merges_mutations: get("metrics.threads_merges_mutations"),
                    fetches: get("metrics.threads_fetches"),
                    common: get("metrics.threads_common"),
                    moves: get("metrics.threads_moves"),
                    schedule: get("metrics.threads_schedule"),
                    buffer_flush: get("metrics.threads_buffer_flush"),
                    distributed: get("metrics.threads_distributed"),
                    message_broker: get("metrics.threads_message_broker"),
                    backups: get("metrics.threads_backups"),
                    io: get("metrics.threads_io"),
                    remote_io: get("metrics.threads_remote_io"),
                    queries: get("metrics.threads_queries"),
                },
            },

            network: ClickHouseServerNetwork {
                send_bytes: get("asynchronous_metrics.net_send_bytes"),
                receive_bytes: get("asynchronous_metrics.net_receive_bytes"),
            },

            blkdev: ClickHouseServerBlockDevices {
                read_bytes: get("asynchronous_metrics.block_read_bytes"),
                write_bytes: get("asynchronous_metrics.block_write_bytes"),
            },

            update_interval: get("asynchronous_metrics.metrics_update_interval"),
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

    pub async fn execute_query(&self, database: &str, query: &str) -> Result<()> {
        self.execute_simple(&format!("USE {}", database)).await?;
        return self.execute_simple(query).await;
    }

    pub async fn explain_syntax(
        &self,
        database: &str,
        query: &str,
        settings: &HashMap<String, String>,
    ) -> Result<Vec<String>> {
        return self
            .explain("SYNTAX", database, query, Some(settings))
            .await;
    }

    pub async fn explain_plan(&self, database: &str, query: &str) -> Result<Vec<String>> {
        return self.explain("PLAN actions=1", database, query, None).await;
    }

    pub async fn explain_pipeline(&self, database: &str, query: &str) -> Result<Vec<String>> {
        return self.explain("PIPELINE", database, query, None).await;
    }

    pub async fn explain_pipeline_graph(&self, database: &str, query: &str) -> Result<Vec<String>> {
        return self
            .explain("PIPELINE graph=1", database, query, None)
            .await;
    }

    // NOTE: can we benefit from json=1?
    pub async fn explain_plan_indexes(&self, database: &str, query: &str) -> Result<Vec<String>> {
        return self.explain("PLAN indexes=1", database, query, None).await;
    }

    // TODO: copy all settings from the query
    async fn explain(
        &self,
        what: &str,
        database: &str,
        query: &str,
        settings: Option<&HashMap<String, String>>,
    ) -> Result<Vec<String>> {
        self.execute_simple(&format!("USE {}", database))
            .await
            .unwrap();

        if let Some(settings) = settings {
            // NOTE: it handles queries with SETTINGS incorrectly, i.e.:
            //
            //     SELECT 1 SETTINGS max_threads=1
            //
            //     EXPLAIN SYNTAX SELECT 1 SETTINGS max_threads=1 SETTINGS max_threads=1, max_insert_threads=1 ->
            //     SELECT 1 SETTINGS max_threads=1
            //
            // This can be fixed two ways:
            // - in ClickHouse
            // - by passing settings in the protocol
            if !settings.is_empty() {
                return Ok(collect_values(
                    &self
                        .execute(&format!(
                            "EXPLAIN {} {} SETTINGS {}",
                            what,
                            query,
                            settings
                                .iter()
                                .map(|kv| format!("{}='{}'", kv.0, kv.1.replace('\'', "\\\'")))
                                .collect::<Vec<String>>()
                                .join(",")
                        ))
                        .await?,
                    "explain",
                ));
            }
        }

        return Ok(collect_values(
            &self.execute(&format!("EXPLAIN {} {}", what, query)).await?,
            "explain",
        ));
    }

    pub async fn get_query_logs(
        &self,
        query_ids: &Option<Vec<String>>,
        start_microseconds: DateTime<Local>,
        end_microseconds: Option<DateTime<Local>>,
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
                    WITH
                        fromUnixTimestamp64Nano({}) AS start_time_,
                        fromUnixTimestamp64Nano({}) AS end_time_
                    SELECT
                        hostName() AS host_name,
                        event_time,
                        event_time_microseconds,
                        thread_id,
                        level::String AS level,
                        // logger_name AS logger_name,
                        message
                    FROM {}
                    WHERE
                            event_date >= toDate(start_time_) AND event_time >= toDateTime(start_time_) AND event_time_microseconds > start_time_
                        AND event_date <= toDate(end_time_)   AND event_time <= toDateTime(end_time_)   AND event_time_microseconds <= end_time_
                        {}
                        // TODO: if query finished, add filter for event_time end range
                    ORDER BY event_date, event_time, event_time_microseconds
                    "#,
                    start_microseconds
                        .timestamp_nanos_opt()
                        .ok_or(Error::msg("Invalid start time"))?,
                    end_microseconds
                        .unwrap_or(Local::now())
                        .timestamp_nanos_opt()
                        .ok_or(Error::msg("Invalid end time"))?,
                    dbtable,
                    if let Some(query_ids) = query_ids {
                        format!("AND query_id IN ('{}')", query_ids.join("','"))
                    } else {
                        "".into()
                    }
                )
                .as_str(),
            )
            .await;
    }

    /// Return query flamegraph in pyspy format for flameshow.
    /// It is the same format as TSV, but with ' ' delimiter between symbols and weight.
    pub async fn get_flamegraph(
        &self,
        trace_type: TraceType,
        query_ids: Option<&Vec<String>>,
        start_microseconds: Option<DateTime<Local>>,
        end_microseconds: Option<DateTime<Local>>,
    ) -> Result<Columns> {
        let dbtable = self.get_table_name("system.trace_log");
        return self
            .execute(&format!(
                r#"
            WITH
                {} AS start_time_,
                {} AS end_time_
            SELECT
              arrayStringConcat(arrayMap(
                addr -> demangle(addressToSymbol(addr)),
                arrayReverse(trace)
              ), ';') AS human_trace,
              {} weight
            FROM {}
            WHERE
                    event_date >= toDate(start_time_) AND event_time >  toDateTime(start_time_) AND event_time_microseconds > start_time_
                AND event_date <= toDate(end_time_)   AND event_time <= toDateTime(end_time_)   AND event_time_microseconds <= end_time_
                AND trace_type = '{:?}'
                {}
            GROUP BY human_trace
            SETTINGS allow_introspection_functions=1
            "#,
                match start_microseconds {
                    Some(time) => format!(
                        "fromUnixTimestamp64Nano({})",
                        time.timestamp_nanos_opt()
                            .ok_or(Error::msg("Invalid start time"))?
                    ),
                    None => "toDateTime64(now() - INTERVAL 1 HOUR, 6)".to_string(),
                },
                match end_microseconds {
                    Some(time) => format!(
                        "fromUnixTimestamp64Nano({})",
                        time.timestamp_nanos_opt()
                            .ok_or(Error::msg("Invalid end time"))?
                    ),
                    None => "toDateTime64(now(), 6)".to_string(),
                },
                match trace_type {
                    TraceType::Memory => "abs(sum(size))",
                    _ => "count()",
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

    pub async fn get_live_query_flamegraph(&self, query_ids: &[String]) -> Result<Columns> {
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
            SETTINGS allow_introspection_functions=1
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
        return format!("clusterAllReplicas('{}', {})", cluster, dbtable);
    }
}
