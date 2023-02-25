use crate::interpreter::{
    clickhouse::{ClickHouseServerSummary, Columns},
    options::ChDigOptions,
    ClickHouse, Worker,
};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub type ContextArc = Arc<Mutex<Context>>;

pub struct Context {
    pub options: ChDigOptions,

    pub clickhouse: Arc<ClickHouse>,
    pub server_version: String,
    pub worker: Worker,

    pub cb_sink: cursive::CbSink,

    //
    // Events specific
    //
    pub processes: Option<Columns>,
    // For get_query_logs()
    pub query_logs: Option<Columns>,
    // For summary
    pub server_summary: ClickHouseServerSummary,
}

impl Context {
    pub async fn new(options: ChDigOptions, cb_sink: cursive::CbSink) -> Result<ContextArc> {
        let clickhouse = Arc::new(ClickHouse::new(options.clickhouse.clone()).await?);
        let server_version = clickhouse
            .version()
            .await
            .expect("Cannot obtain ClickHouse version");
        let worker = Worker::new();

        let context = Arc::new(Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            cb_sink,

            processes: None,
            query_logs: None,
            server_summary: ClickHouseServerSummary::default(),
        }));

        context.lock().unwrap().worker.start(context.clone());

        return Ok(context);
    }
}
