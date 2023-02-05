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
pub struct ClickHouseServerSummary {
    pub os_uptime: u64,

    pub os_memory_total: u64,
    pub memory_resident: u64,

    pub cpu_count: u64,
    pub cpu_user: u64,
    pub cpu_system: u64,

    pub net_send_bytes: u64,
    pub net_receive_bytes: u64,

    pub block_read_bytes: u64,
    pub block_write_bytes: u64,
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
                        -- TODO: show this column in the table (only for --cluster)
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
        let dbtable = self.get_table_name("system.asynchronous_metrics");
        let block = self
            .execute(
                format!(
                    r#"
                    SELECT
                        -- NOTE: metrics are deltas, so chdig do not need to reimplement this logic by itself.
                        anyIf(value, metric == 'OSUptime')::UInt64 os_uptime,
                        -- memory
                        anyIf(value, metric == 'OSMemoryTotal')::UInt64 os_memory_total,
                        anyIf(value, metric == 'MemoryResident')::UInt64 memory_resident,
                        -- cpu
                        countIf(metric LIKE 'OSUserTimeCPU%')::UInt64 cpu_count,
                        sumIf(value, metric LIKE 'OSUserTimeCPU%')::UInt64 cpu_user,
                        sumIf(value, metric LIKE 'OSSystemTimeCPU%')::UInt64 cpu_system,
                        -- network (note: maybe have duplicated accounting due to bridges and stuff)
                        sumIf(value, metric LIKE 'NetworkSendBytes%')::UInt64 net_send_bytes,
                        sumIf(value, metric LIKE 'NetworkReceiveBytes%')::UInt64 net_receive_bytes,
                        -- block devices
                        sumIf(value, metric LIKE 'BlockReadBytes%')::UInt64 block_read_bytes,
                        sumIf(value, metric LIKE 'BlockWriteBytes%')::UInt64 block_write_bytes
                    FROM {}
                "#,
                    dbtable
                )
                .as_str(),
            )
            .await;

        return Ok(ClickHouseServerSummary {
            os_uptime: block.get::<u64, _>(0, "os_uptime")?,

            os_memory_total: block.get::<u64, _>(0, "os_memory_total")?,
            memory_resident: block.get::<u64, _>(0, "memory_resident")?,

            cpu_count: block.get::<u64, _>(0, "cpu_count")?,
            cpu_user: block.get::<u64, _>(0, "cpu_user")?,
            cpu_system: block.get::<u64, _>(0, "cpu_system")?,

            net_send_bytes: block.get::<u64, _>(0, "net_send_bytes")?,
            net_receive_bytes: block.get::<u64, _>(0, "net_receive_bytes")?,

            block_read_bytes: block.get::<u64, _>(0, "block_read_bytes")?,
            block_write_bytes: block.get::<u64, _>(0, "block_write_bytes")?,
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

    fn get_table_name(&mut self, dbtable: &str) -> String {
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
