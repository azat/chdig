use crate::interpreter::options::ClickHouseOptions;
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
                        ProfileEvents['OSCPUVirtualTimeMicroseconds']/1e6/elapsed*100 AS cpu,
                        ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] AS disk_io,
                        (
                            ProfileEvents['NetworkReceiveBytes'] +
                            ProfileEvents['NetworkSendBytes']
                        ) AS net_io,
                        -- TODO:
                        -- peak_memory_usage,

                        thread_ids,
                        memory_usage,
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
        let block = client.query(query).fetch_all().await.unwrap();
        return block;
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
