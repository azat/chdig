use crate::interpreter::{clickhouse::Columns, options::ChDigOptions, ClickHouse, Worker};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub type ContextArc = Arc<Mutex<Context>>;

pub struct Context {
    pub options: ChDigOptions,

    pub clickhouse: Arc<ClickHouse>,
    pub server_version: String,
    pub worker: Worker,

    pub cb_sink: cursive::CbSink,

    // For get_query_logs()
    // TODO: remove this by calling update from the worker instead (like for other views)
    pub query_logs: Option<Columns>,
}

impl Context {
    pub async fn new(options: ChDigOptions, cb_sink: cursive::CbSink) -> Result<ContextArc> {
        let clickhouse = Arc::new(ClickHouse::new(options.clickhouse.clone()).await?);
        let server_version = clickhouse.version();
        let worker = Worker::new();

        let context = Arc::new(Mutex::new(Context {
            options,
            clickhouse,
            server_version,
            worker,
            cb_sink,

            query_logs: None,
        }));

        context.lock().unwrap().worker.start(context.clone());

        return Ok(context);
    }
}
